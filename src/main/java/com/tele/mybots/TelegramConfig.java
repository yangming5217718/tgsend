package com.tele.mybots;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.telegram.telegrambots.client.okhttp.OkHttpTelegramClient;
import org.telegram.telegrambots.meta.TelegramUrl;
import org.telegram.telegrambots.meta.api.methods.GetMe;
import org.telegram.telegrambots.meta.api.methods.botapimethods.BotApiMethod;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.meta.generics.TelegramClient;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;

@Configuration
@EnableScheduling
public class TelegramConfig {

    // ===================== 国外：只走官方直连 =====================
    private static final TelegramUrl OFFICIAL_URL = TelegramUrl.builder()
            .schema("https")
            .host("api.telegram.org")
            .port(443)
            .build();

    /**
     * 探测失败冷却（仅用于 probe，不影响业务发送）
     */
    private static final long PROBE_SHORT_COOLDOWN_MS = Duration.ofSeconds(30).toMillis();
    private static final long PROBE_LONG_COOLDOWN_MS  = Duration.ofMinutes(10).toMillis();

    private final AtomicLong nextProbeAtOfficial = new AtomicLong(0);

    @SuppressWarnings("unused")
    private volatile List<Route> preferredRoutes = List.of(Route.OFFICIAL);

    private volatile String probeToken;

    enum Route { OFFICIAL }

    @Bean("telegramClient")
    public TelegramClient telegramClient(BotTokenSource tokenSource) {
        String token = tokenSource.token();
        this.probeToken = token;
        return buildSmartClient(token);
    }

    // ===================== 核心：官方单线路 client =====================
    private TelegramClient buildSmartClient(String token) {

        /**
         * 说明：
         * 1. connectTimeout 保持短，快速识别连不通
         * 2. read/write/callTimeout 收紧，避免单个 photo 卡太久
         * 3. 保留 retryOnConnectionFailure=true
         * 4. 加连接池，减少重复建连开销
         */
        OkHttpClient base = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(5))
                .readTimeout(Duration.ofSeconds(15))
                .writeTimeout(Duration.ofSeconds(20))
                .callTimeout(Duration.ofSeconds(20))
                .retryOnConnectionFailure(true)
                .connectionPool(new ConnectionPool(20, 5, java.util.concurrent.TimeUnit.MINUTES))
                .build();

        TelegramClient official = new OkHttpTelegramClient(base, token, OFFICIAL_URL);

        return (TelegramClient) Proxy.newProxyInstance(
                TelegramClient.class.getClassLoader(),
                new Class[]{TelegramClient.class},
                (proxy, method, args) -> {

                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(official, args);
                    }

                    String name = method.getName();
                    boolean needLog =
                            name.startsWith("execute")
                                    || name.startsWith("downloadFile")
                                    || name.startsWith("downloadFileAsStream");

                    if (!needLog) {
                        return invoke(official, method, args);
                    }

                    String trace = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
                    long t0 = System.currentTimeMillis();

                    try {
                        System.out.println("[TG] [" + trace + "] 进入客户端"
                                + " 线路=OFFICIAL"
                                + " 方法=" + method.getName());
                        logTry(trace, "OFFICIAL", method, args);
                        Object out = invoke(official, method, args);
                        long cost = System.currentTimeMillis() - t0;
                        logOk(trace, "OFFICIAL", method, cost, args);
                        return out;

                    } catch (Throwable e) {
                        long cost = System.currentTimeMillis() - t0;
                        logFail(trace, "OFFICIAL", method, e, cost, args);

                        // 保持原始异常语义
                        if (e instanceof TelegramApiException te) throw te;
                        if (e instanceof RuntimeException re) throw re;
                        throw new TelegramApiException(e);
                    }
                }
        );
    }

    // ===================== 启动探测一次 =====================
    @EventListener(ApplicationReadyEvent.class)
    public void probeOnBoot() {
        safeProbe("BOOT");
    }

    // ===================== 定时探测：每 5 分钟一次（初次延迟 2 分钟） =====================
    @Scheduled(fixedDelay = 300_000, initialDelay = 120_000)
    public void probeScheduled() {
        safeProbe("TIMER");
    }

    /**
     * 探测失败只记录，不影响业务发送
     */
    private void safeProbe(String source) {
        try {
            if (probeToken == null || probeToken.isEmpty()) return;

            long now = System.currentTimeMillis();
            if (now < nextProbeAtOfficial.get()) {
                System.out.println("[TG-PROBE] [" + source + "] OFFICIAL SKIP (probe cooldown)");
                return;
            }

            OkHttpClient probeHttp = new OkHttpClient.Builder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .readTimeout(Duration.ofSeconds(4))
                    .writeTimeout(Duration.ofSeconds(4))
                    .callTimeout(Duration.ofSeconds(5))
                    .retryOnConnectionFailure(true)
                    .build();

            TelegramClient officialProbe = new OkHttpTelegramClient(probeHttp, probeToken, OFFICIAL_URL);

            long t0 = System.currentTimeMillis();
            officialProbe.execute(new GetMe());
            long cost = System.currentTimeMillis() - t0;

            System.out.println("[TG-PROBE] [" + source + "] OFFICIAL OK " + cost + "ms");

        } catch (Exception e) {
            Throwable r = rootCause(e);
            System.out.println("[TG-PROBE] [" + source + "] OFFICIAL FAIL ex=" + r.getClass().getSimpleName()
                    + " msg=" + safeMsg(r.getMessage()));

            cooldownProbeByError(e);
        }
    }

    private void cooldownProbeByError(Throwable e) {
        long now = System.currentTimeMillis();
        boolean longCd = isNetworkVeryBadLike(e);

        long cd = longCd ? PROBE_LONG_COOLDOWN_MS : PROBE_SHORT_COOLDOWN_MS;
        nextProbeAtOfficial.set(now + cd);
    }

    /**
     * 这里只影响探测频率，不影响业务发送
     */
    private boolean isNetworkVeryBadLike(Throwable e) {
        Throwable r = rootCause(e);

        String msg = (r.getMessage() == null ? "" : r.getMessage().toLowerCase());
        String cls = r.getClass().getSimpleName().toLowerCase();

        if (msg.contains("connection refused")) return true;
        if (msg.contains("no route to host")) return true;
        if (msg.contains("unknownhost")) return true;
        if (msg.contains("failed to connect")) return true;

        if (r instanceof SocketTimeoutException) return false;
        if (cls.contains("timeout")) return false;

        return false;
    }

    private static Throwable rootCause(Throwable e) {
        Throwable r = e;
        while (r.getCause() != null && r.getCause() != r) r = r.getCause();
        return r;
    }

    private static Object invoke(TelegramClient client, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(client, args);
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause != null) throw cause;
            throw ite;
        }
    }

    // ===================== 详细日志 =====================
    private static void logTry(String trace, String route, Method m, Object[] args) {
        System.out.println("[TG] [" + trace + "] 开始请求"
                + " 线路=" + route
                + " 方法=" + m.getName()
                + " 参数=" + buildArgDetail(args));
    }

    private static void logOk(String trace, String route, Method m, long costMs, Object[] args) {
        System.out.println("[TG] [" + trace + "] 请求成功"
                + " 线路=" + route
                + " 方法=" + m.getName()
                + " 耗时=" + costMs + "ms"
                + " 参数=" + buildArgDetail(args));
    }

    private static void logFail(String trace, String route, Method m, Throwable e, long costMs, Object[] args) {
        Throwable r = rootCause(e);
        System.out.println("[TG] [" + trace + "] 请求失败"
                + " 线路=" + route
                + " 方法=" + m.getName()
                + " 耗时=" + costMs + "ms"
                + " 参数=" + buildArgDetail(args)
                + " 异常=" + r.getClass().getSimpleName()
                + " 错误=" + safeMsg(r.getMessage())
                + " 异常链=" + buildExceptionChain(e));
    }

    private static String buildArgDetail(Object[] args) {
        if (args == null || args.length == 0 || args[0] == null) return "arg0=null";

        Object a0 = args[0];

        try {
            if (a0 instanceof SendPhoto req) {
                String chatId = safeObj(req.getChatId());
                String caption = req.getCaption();
                Object replyMarkup = req.getReplyMarkup();
                Integer replyTo = req.getReplyToMessageId();

                String photoDesc = describeInputFile(req.getPhoto());

                return "SendPhoto"
                        + "{chatId=" + chatId
                        + ", captionLen=" + (caption == null ? 0 : caption.length())
                        + ", hasReplyMarkup=" + (replyMarkup != null)
                        + ", replyTo=" + (replyTo == null ? "null" : replyTo)
                        + ", photo=" + photoDesc
                        + "}";
            }

            if (a0 instanceof SendMessage req) {
                String chatId = safeObj(req.getChatId());
                String text = req.getText();
                Object replyMarkup = req.getReplyMarkup();
                Integer replyTo = req.getReplyToMessageId();

                return "SendMessage"
                        + "{chatId=" + chatId
                        + ", textLen=" + (text == null ? 0 : text.length())
                        + ", hasReplyMarkup=" + (replyMarkup != null)
                        + ", replyTo=" + (replyTo == null ? "null" : replyTo)
                        + "}";
            }

            if (a0 instanceof GetMe) {
                return "GetMe{}";
            }

            if (a0 instanceof BotApiMethod<?> method) {
                return method.getClass().getSimpleName() + "{}";
            }

            return a0.getClass().getSimpleName();
        } catch (Exception e) {
            return a0.getClass().getSimpleName() + "{detailErr=" + e.getClass().getSimpleName() + "}";
        }
    }

    private static String describeInputFile(InputFile photo) {
        if (photo == null) return "null";

        try {
            String attachName = safeObj(photo.getAttachName());
            String mediaName = safeObj(photo.getMediaName());
            boolean isNew = photo.isNew();

            Object media = photo.getNewMediaFile();
            if (media instanceof java.io.File f) {
                return "newFile"
                        + "{attachName=" + attachName
                        + ", mediaName=" + mediaName
                        + ", file=" + cut(f.getAbsolutePath())
                        + ", bytes=" + f.length()
                        + ", isNew=" + isNew
                        + "}";
            }

            if (photo.getNewMediaStream() != null) {
                return "newStream"
                        + "{attachName=" + attachName
                        + ", mediaName=" + mediaName
                        + ", isNew=" + isNew
                        + "}";
            }

            return "existing"
                    + "{attachName=" + attachName
                    + ", mediaName=" + mediaName
                    + ", isNew=" + isNew
                    + "}";

        } catch (Exception e) {
            return "InputFile{detailErr=" + e.getClass().getSimpleName() + "}";
        }
    }

    private static String buildExceptionChain(Throwable e) {
        StringBuilder sb = new StringBuilder();
        Throwable cur = e;
        int depth = 0;

        while (cur != null && depth < 6) {
            if (depth > 0) sb.append(" <- ");
            sb.append(cur.getClass().getSimpleName());
            if (cur.getMessage() != null) {
                sb.append(":").append(safeMsg(cur.getMessage()));
            }
            cur = cur.getCause();
            depth++;
        }
        return sb.toString();
    }

    private static String safeObj(Object o) {
        return o == null ? "null" : safeMsg(String.valueOf(o));
    }

    private static String safeMsg(String s) {
        if (s == null) return "null";
        return s.replace("\n", " ").replace("\r", " ");
    }

    private static String cut(String s) {
        if (s == null) return "null";
        return s.length() <= 180 ? s : s.substring(0, 180) + "...";
    }
}