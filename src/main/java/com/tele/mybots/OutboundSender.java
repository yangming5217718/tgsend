package com.tele.mybots;
import java.io.File;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.send.SendAnimation;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.api.objects.ResponseParameters;
import org.telegram.telegrambots.meta.api.objects.message.Message;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.meta.generics.TelegramClient;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.tele.common.KeyboardUtil;
import com.tele.common.TgTextUtil;
import com.tele.common.RedisSlidingWindowRateLimiter;
import com.tele.common.Utils;
import com.tele.entity.CpBotmessageSendUser;
import com.tele.mapper.CpBotmessageSendUserMapper;

import jakarta.annotation.PreDestroy;

@Component
public class OutboundSender {

    // ===================== 固定参数（不走配置文件） =====================
    private static final int MPS_MIN = 5;
    private static final int MPS_MAX = 60;
    private static final int BURST_GLOBAL = 30;
    private static final double CHAT_RATE = 2.0;
    private static final int CHAT_BURST = 3;
    private static final int MPS_INIT = 10;

    // 并发
    private static final int INFLIGHT_MAX = 80;
    private static final int INFLIGHT_ACQUIRE_TIMEOUT_SEC = 2;
    private static final int SEND_WORKERS = 16;

    // 过期宽限
    private static final long EXPIRY_GRACE_MS = 2000;

    // 图片下载/缓存
    private static final long IMG_MAX_BYTES = 8L * 1024 * 1024;
    private static final int IMG_CACHE_HOURS = 6;
    private static final String IMG_TEMP_DIR = "/tmp/tg-img-cache";

    // Telegram file_id 缓存
    private static final String TG_FILEID_CACHE_PREFIX = "tg:fileid:";
    private static final Duration TG_FILEID_CACHE_TTL = Duration.ofDays(30);

    // Telegram 文本限制
    private static final int TG_CAPTION_MAX = 1024;
    private static final int TG_TEXT_MAX = 4096;

    /** 行上 parsemode 为空时的兜底，与 TgTextUtil.DEFAULT_PARSE_MODE 同值 */

    // ===================== 格式/常量 =====================
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final DateTimeFormatter LOG_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final String IDEM_S_MAIN  = "idem:tg:sent:main:";

    // ===================== 依赖 =====================
    private final StringRedisTemplate redis;
    private final RedisSlidingWindowRateLimiter limiter;
    private final CpBotmessageSendUserMapper mainMapper;

    @Autowired @Qualifier("telegramClient")
    private TelegramClient telegramClient;

    private volatile int mpsAdaptive;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService msgExec;   // 扫库线程
    private ExecutorService sendExec;  // 共享发送线程池

    private final Semaphore inflight = new Semaphore(INFLIGHT_MAX, true);

    @SuppressWarnings("unused")
    private final Deque<Long> recent429 = new ArrayDeque<>();

    private volatile long cooldownUntilMs = 0;

    /**
     * 每个 chat 一个串行队列。
     * 重点：
     * 1. 同一个 chat 严格按顺序执行；
     * 2. 同 chat 后续任务只存在队列里，不占 sendExec worker 等锁；
     * 3. 不同 chat 可以并行发送。
     */
    private final ConcurrentHashMap<String, ChatSendQueue> chatQueues = new ConcurrentHashMap<>();

    public OutboundSender(
            StringRedisTemplate redis,
            RedisSlidingWindowRateLimiter limiter,
            CpBotmessageSendUserMapper mainMapper
    ) {
        this.redis = redis;
        this.limiter = limiter;
        this.mainMapper = mainMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (running.get()) return;

        this.mpsAdaptive = Math.max(MPS_MIN, Math.min(MPS_INIT, MPS_MAX));
        running.set(true);

        msgExec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "msg-db-sender");
            t.setDaemon(true);
            return t;
        });

        sendExec = Executors.newFixedThreadPool(SEND_WORKERS, r -> {
            Thread t = new Thread(r, "tg-send-worker");
            t.setDaemon(true);
            return t;
        });

        msgExec.submit(this::msgloop);

        logInfo("发送器启动成功"
                + " 可用并发=" + inflight.availablePermits()
                + " 当前每秒发送上限=" + mpsAdaptive
                + " 全局突发上限=" + BURST_GLOBAL
                + " 单群速率=" + CHAT_RATE
                + " 单群突发上限=" + CHAT_BURST
                + " 发送线程数=" + SEND_WORKERS
                + " 图片最大字节=" + IMG_MAX_BYTES
                + " 图片缓存小时=" + IMG_CACHE_HOURS
                + " 图片临时目录=" + IMG_TEMP_DIR
                + " 默认解析模式=" + TgTextUtil.DEFAULT_PARSE_MODE);
    }

    @PreDestroy
    public void stop() {
        running.set(false);

        if (msgExec != null) {
            msgExec.shutdownNow();
            try {
                msgExec.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        if (sendExec != null) {
            sendExec.shutdownNow();
            try {
                sendExec.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        logInfo("发送器已停止");
    }

    // ========== 主循环 ==========
    public void msgloop() {
        int backoffMs = 200;

        while (running.get()) {
            try {
                if (isGlobalCooldown()) {
                    long left = cooldownUntilMs - System.currentTimeMillis();
                    int sleepMs = (int) Math.min(1000, Math.max(50, left));
                    sleepQuietly(sleepMs);
                    continue;
                }

                List<CpBotmessageSendUser> mainList = mainMapper.selectMsgListForUser();
                for (CpBotmessageSendUser row : mainList) {
                    if (!running.get()) return;
                    sendMain(row);
                }

                if (!isGlobalCooldown()) {
                    mpsAdaptive = Math.min(MPS_MAX, mpsAdaptive + 1);
                }
                backoffMs = 200;

                sleepQuietly(80);

            } catch (org.springframework.jdbc.CannotGetJdbcConnectionException e) {
                if (!running.get()) return;
                logWarn("DB连接失败，" + backoffMs + "ms后重试: " + safeMsg(e.getMessage()));
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);

            } catch (org.springframework.data.redis.RedisSystemException e) {
                if (!running.get()) return;
                logWarn("Redis异常，" + backoffMs + "ms后重试: " + safeMsg(e.getMessage()));
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);

            } catch (IllegalStateException e) {
                String msg = String.valueOf(e.getMessage());
                if (!running.get() || msg.contains("STOPPING")) return;
                logWarn("程序状态异常，错误=" + safeMsg(msg));
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);

            } catch (Exception e) {
                if (!running.get()) return;
                logErr("发送主循环异常，错误=" + safeMsg(e.getMessage()));
                e.printStackTrace();
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);
            }
        }
    }

    // ========== 主表发送 ==========
    private void sendMain(CpBotmessageSendUser row) {
        if (row == null) return;
        if (!markMainSending(row.getId())) return;

        final String trace = newTrace("m");
        logInfo(trace, "消息已锁定"
                        + " 表=main"
                        + " ID=" + row.getId()
                        + " 群ID=" + row.getChatid()
                        + " 过期时间=" + row.getExptime());
        try {
            submitByChat(row.getChatid(), () -> sendCommonSync(
                    trace,
                    "main",
                    row.getId(),
                    row.getChatid(),
                    row.getImgsrc(),
                    row.getContent(),
                    row.getButtontext(),
                    row.getParsemode(),
                    row.getMsgid(),
                    row.getExptime(),
                    IDEM_S_MAIN,
                    telegramClient,
                    sentMsgId -> updateMainAck(row.getId(), sentMsgId),
                    err -> updateMainFail(row.getId(), err)
            ));
        } catch (RejectedExecutionException e) {
            logWarn(trace, "提交发送任务被拒绝 表=main ID=" + row.getId() + " 群ID=" + row.getChatid());
            updateMainFail(row.getId(), "submit_rejected");
        } catch (Exception e) {
            logWarn(trace,
                    "提交发送任务异常"
                            + " 表=main"
                            + " ID=" + row.getId()
                            + " 群ID=" + row.getChatid()
                            + " 异常类型=" + e.getClass().getSimpleName()
                            + " 错误=" + safeMsg(e.getMessage()));
            updateMainFail(row.getId(), "submit_ex " + e.getClass().getSimpleName());
        }
    }

    // ========== 同 chat 严格串行，不同 chat 并行 ==========
    private void submitByChat(String chatid, Runnable task) {
        if (task == null) {
            return;
        }
        /*
         * 没有 chatid 的任务无法做同群串行，
         * 直接交给公共发送池。
         */
        if (chatid == null || chatid.isEmpty()) {
            sendExec.submit(task);
            return;
        }
        /*
         * 每个 chat 只有一个 ChatSendQueue。
         * 后续同 chat 消息只是进入 queue，
         * 不会再占用 tg-send-worker 等待 ReentrantLock。
         */
        ChatSendQueue queue = chatQueues.computeIfAbsent(chatid, ChatSendQueue::new);
        queue.offer(task);
    }
    /**
     * 单个 Telegram chat 的串行发送队列。
     * 一个 chat 无论积压多少条消息，
     * 同时最多只占用一个 sendExec worker。
     */
    private class ChatSendQueue {
        private final String chatid;
        private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
        /**
         * true:
         * 当前这个 chat 已经有 worker 在处理。
         * false:
         * 当前没有 worker，可以启动一个。
         */
        private final AtomicBoolean active = new AtomicBoolean(false);

        private ChatSendQueue(String chatid) {
            this.chatid = chatid;
        }
        /**
         * 消息进入当前 chat 队列。
         */
        private void offer(Runnable task) {
            queue.offer(task);
            /*
             * 尝试启动当前 chat 的消费 worker。
             * 如果已经有人在消费，
             * CAS 会失败，当前消息只排队即可。
             */
            trySchedule();
        }
        /**
         * 保证同一个 chat 最多只有一个 worker。
         */
        private void trySchedule() {
            if (!running.get()) {
                return;
            }
            if (!active.compareAndSet(false, true)) {
                return;
            }
            try {
                sendExec.submit(this::drain);
            } catch (RejectedExecutionException e) {
                /*
                 * submit 失败必须把 active 恢复，
                 * 否则这个 chat 会永久认为有人在消费。
                 */
                active.set(false);
                logWarn("群发送队列提交失败"
                        + " 群ID=" + chatid
                        + " 排队数量=" + queue.size());
                throw e;
            }
        }

        /**
         * 当前 worker 顺序消费这个 chat 的消息。
         * 这里不会使用 lock.lock()。
         */
        private void drain() {
            logInfo("群发送队列开始处理"
                    + " 群ID=" + chatid
                    + " 排队数量=" + queue.size());
            try {
                while (running.get()) {
                    Runnable task = queue.poll();
                    if (task == null) {
                        break;
                    }
                    try {
                        task.run();
                    } catch (Throwable e) {
                        /*
                         * 单条消息异常不能把整个 chat 队列打死。
                         * sendCommonSync 本身已经会处理大部分异常，
                         * 这里属于最后一道保护。
                         */
                        logWarn("群发送队列任务异常"
                                + " 群ID=" + chatid
                                + " 异常类型=" + e.getClass().getSimpleName()
                                + " 错误=" + safeMsg(e.getMessage()));
                    }
                }
            } finally {
                /*
                 * 先释放 active。
                 */
                active.set(false);
                logInfo("群发送队列处理结束"
                        + " 群ID=" + chatid
                        + " 剩余数量=" + queue.size());
                /*
                 * 这里必须再次检查。
                 * 防止刚刚认为队列为空的瞬间，
                 * 又有新消息进来。
                 */
                if (running.get() && !queue.isEmpty()) {
                    trySchedule();
                }
            }
        }
    }

    // ========== 通用发送 ==========
    private void sendCommonSync(
            String trace,
            String table,
            Long id,
            String chatid,
            String img,
            String content,
            String buttontext,
            String parsemodeRaw,
            String msgid,
            String exptime,
            String idemPrefix,
            TelegramClient client,
            java.util.function.Consumer<Integer> ackOk,
            java.util.function.Consumer<String> ackFail
    ) {
        if (!running.get()) return;

        if (id == null) return;
        if (chatid == null || chatid.isEmpty()) {
            ackFail.accept("chatid empty");
            return;
        }

        String idemKey = idemPrefix + id;

        if (Boolean.TRUE.equals(redis.hasKey(idemKey))) {
            logInfo(trace,
                    "命中已发送记录，直接确认成功"
                            + " 表=" + table
                            + " ID=" + id
                            + " 群ID=" + chatid);
            /*
             * 幂等命中时没有本次 Telegram 响应，
             * 传 null 表示「不要覆盖已有 sendid」。
             */
            ackOk.accept(null);
            return;
        }

        if (isExpired(exptime)) {
            LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
            logWarn(trace,
                    "消息已过期"
                            + " 表=" + table
                            + " ID=" + id
                            + " 群ID=" + chatid
                            + " 当前时间=" + now.format(TS)
                            + " 过期时间=" + exptime
                            + " 宽限毫秒=" + EXPIRY_GRACE_MS);
            ackFail.accept("expired");
            return;
        }

        if (isGlobalCooldown()) {
            ackFail.accept("cooldown");
            return;
        }

        if (!limiter.allowOnceBurst("tg:rl:g", mpsAdaptive, BURST_GLOBAL)) {
            ackFail.accept("rate_limited(global)");
            return;
        }
        if (!limiter.allowOnceBurst("tg:rl:c:" + chatid, CHAT_RATE, CHAT_BURST)) {
            ackFail.accept("rate_limited(chat)");
            return;
        }

        try {
            if (!inflight.tryAcquire(INFLIGHT_ACQUIRE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                ackFail.accept("inflight_full");
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ackFail.accept("interrupted");
            return;
        }

        /*
         * parse mode 由行上的 parsemode 决定，转义随之分派。
         * 两者必须一起取——列填了 HTML 却按 MarkdownV2 转义的话，
         * & < > 没转、. - 全被加反斜杠，而且不报错。
         */
        final String parseMode = TgTextUtil.normalizeMode(parsemodeRaw);

        final boolean hasImg = img != null && !img.isEmpty();

        final boolean hasButton =
                StringUtils.isNotBlank(buttontext);
        final int contentLen = content == null ? 0 : content.length();

        logInfo(trace,
                "准备发送"
                        + " 表=" + table
                        + " ID=" + id
                        + " 群ID=" + chatid
                        + " 类型=" + (hasImg ? (isGif(img) ? "动图" : "图片") : "文本")
                        + " 内容长度=" + contentLen
                        + " 有按钮=" + (hasButton ? "是" : "否")
                        + " 当前每秒上限=" + mpsAdaptive
                        + " 当前并发数="
                        + (INFLIGHT_MAX - inflight.availablePermits()));

        long t0 = System.currentTimeMillis();

        InlineKeyboardMarkup replyMarkup = null;

        if (hasButton) {
            try {
                replyMarkup = KeyboardUtil.createUserKeyboard(buttontext);
            } catch (Exception e) {
                logWarn(trace, "解析按钮配置失败" + " table=" + table + " id=" + id + " error=" + safeMsg(e.getMessage()));
            }
        }

        try {
            Message sent;

            if (hasImg) {

                String cap = normalizeAndEscapeFor(content, TG_CAPTION_MAX, parseMode);

                if (isGif(img)) {
                    // GIF 动图用 SendAnimation
                    SendAnimation req = new SendAnimation(chatid, new InputFile(img));

                    if (!cap.isEmpty()) req.setCaption(cap);
                    req.setParseMode(parseMode);

                    if (replyMarkup != null) {
                        req.setReplyMarkup(replyMarkup);
                    }

                    if (msgid != null) {
                        try {
                            req.setReplyToMessageId(Integer.parseInt(msgid));
                        } catch (Exception ignore) {
                        }
                    }

                    CompletableFuture<Message> future = client.executeAsync(req);

                    try {
                        logInfo(trace,
                                "Telegram开始发送"
                                        + " 表=" + table
                                        + " ID=" + id
                                        + " 群ID=" + chatid);
                        sent = future.get(25, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        future.cancel(true);
                        logWarn(trace,
                                "Telegram发送超时"
                                        + " 表=" + table
                                        + " ID=" + id
                                        + " 群ID=" + chatid
                                        + " 超时时间=25秒");

                        throw new SocketTimeoutException(
                                "Telegram发送超过25秒未完成"
                        );
                    }

                } else {
                    // 普通图片继续用 SendPhoto
                    SendPhoto req = new SendPhoto(chatid, new InputFile(img));

                    if (!cap.isEmpty()) req.setCaption(cap);
                    req.setParseMode(parseMode);

                    if (replyMarkup != null) {
                        req.setReplyMarkup(replyMarkup);
                    }

                    if (msgid != null) {
                        try {
                            req.setReplyToMessageId(Integer.parseInt(msgid));
                        } catch (Exception ignore) {
                        }
                    }

                    CompletableFuture<Message> future = client.executeAsync(req);

                    try {
                        logInfo(trace,
                                "Telegram开始发送"
                                        + " 表=" + table
                                        + " ID=" + id
                                        + " 群ID=" + chatid);
                        sent = future.get(25, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        future.cancel(true);
                        logWarn(trace,
                                "Telegram发送超时"
                                        + " 表=" + table
                                        + " ID=" + id
                                        + " 群ID=" + chatid
                                        + " 超时时间=25秒");

                        throw new SocketTimeoutException(
                                "Telegram发送超过25秒未完成"
                        );
                    }
                }

            } else {
                String txt = normalizeAndEscapeFor(content, TG_TEXT_MAX, parseMode);

                SendMessage req = new SendMessage(chatid, txt);
                req.setParseMode(parseMode);
                req.setDisableWebPagePreview(true);

                if (replyMarkup != null) {
                    req.setReplyMarkup(replyMarkup);
                }
                if (msgid != null) {
                    try { req.setReplyToMessageId(Integer.parseInt(msgid)); } catch (Exception ignore) {}
                }

                CompletableFuture<Message> future = client.executeAsync(req);

                try {
                    logInfo(trace,
                            "Telegram开始发送"
                                    + " 表=" + table
                                    + " ID=" + id
                                    + " 群ID=" + chatid);
                    sent = future.get(25, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    future.cancel(true);
                    logWarn(trace,
                            "Telegram发送超时"
                                    + " 表=" + table
                                    + " ID=" + id
                                    + " 群ID=" + chatid
                                    + " 超时时间=25秒");

                    throw new SocketTimeoutException(
                            "Telegram发送超过25秒未完成"
                    );
                }
            }

            long cost = System.currentTimeMillis() - t0;

            try {
                redis.opsForValue().set(idemKey, "1", Duration.ofHours(6));
            } catch (Exception ignore) {}

            if (hasImg && !isGif(img)) {
                cacheTelegramPhotoFileIdFromMessage(trace, img, sent);
            }

            ackOk.accept(sent == null ? null : sent.getMessageId());

            logInfo(trace,
                    "发送成功并确认"
                            + " 表=" + table
                            + " ID=" + id
                            + " 群ID=" + chatid
                            + " Telegram消息ID="
                            + (sent == null ? "null" : sent.getMessageId())
                            + " 耗时=" + cost + "ms");

        } catch (TelegramApiRequestException tex) {
            long cost = System.currentTimeMillis() - t0;

            int code = tex.getErrorCode();
            String desc = safeMsg(tex.getMessage());

            Integer retryAfter = Optional.ofNullable(tex.getParameters())
                    .map(ResponseParameters::getRetryAfter)
                    .orElse(1);

            if (code == 429) {
                recent429.addLast(System.currentTimeMillis());
                while (recent429.size() > 200) recent429.removeFirst();

                setCooldownSeconds(retryAfter);
                mpsAdaptive = Math.max(MPS_MIN, (int) (mpsAdaptive * 0.7));

                logWarn(trace,
                        "Telegram触发429限流"
                                + " 表=" + table
                                + " ID=" + id
                                + " 群ID=" + chatid
                                + " 建议等待=" + retryAfter + "秒"
                                + " 耗时=" + cost + "ms"
                                + " 错误=" + desc);

                ackFail.accept("429 retry_after=" + retryAfter + " costMs=" + cost + " desc=" + desc);
                return;
            }

            logWarn(trace,
                    "Telegram接口返回失败"
                            + " 表=" + table
                            + " ID=" + id
                            + " 群ID=" + chatid
                            + " 错误码=" + code
                            + " 耗时=" + cost + "ms"
                            + " 错误=" + desc);

            if (hasImg && isLikelyFileIdInvalidError(desc)) {
                evictTelegramFileIdCache(img);
                logWarn(trace,
                        "Telegram图片FileId缓存已清除"
                                + " 图片=" + cut(img)
                                + " 原因=" + desc);
            }

            ackFail.accept("tg error(" + code + "): " + desc + " costMs=" + cost);

        } catch (Exception e) {
            long cost = System.currentTimeMillis() - t0;
            String root = rootCauseString(e);
            logWarn(trace,
                    "发送过程异常"
                            + " 表=" + table
                            + " ID=" + id
                            + " 群ID=" + chatid
                            + " 耗时=" + cost + "ms"
                            + " 异常类型=" + e.getClass().getSimpleName()
                            + " 错误=" + safeMsg(e.getMessage())
                            + " 根因=" + root);

            ackFail.accept("send_ex " + root + " costMs=" + cost);

        } finally {
            inflight.release();
        }
    }

    // ========== Telegram file_id 缓存 ==========
    private String buildTelegramFileCacheKey(String img) {
        if (img == null || img.isEmpty()) return null;

        if (img.startsWith("http://") || img.startsWith("https://")) {
            return TG_FILEID_CACHE_PREFIX + sha1Hex(img);
        }

        File f = new File(img);
        if (f.exists() && f.isFile()) {
            String raw = f.getAbsolutePath() + "|" + f.length() + "|" + f.lastModified();
            return TG_FILEID_CACHE_PREFIX + sha1Hex(raw);
        }

        return null;
    }


    private void cacheTelegramFileId(String img, String fileId) {
        if (img == null || img.isEmpty() || fileId == null || fileId.isEmpty()) return;

        try {
            String key = buildTelegramFileCacheKey(img);
            if (key == null) return;
            redis.opsForValue().set(key, fileId, TG_FILEID_CACHE_TTL);
        } catch (Exception ignore) {
        }
    }

    private void evictTelegramFileIdCache(String img) {
        try {
            String key = buildTelegramFileCacheKey(img);
            if (key != null) {
                redis.delete(key);
            }
        } catch (Exception ignore) {
        }
    }

    private void cacheTelegramPhotoFileIdFromMessage(String trace, String originalImg, Message sent) {
        if (originalImg == null || originalImg.isEmpty() || sent == null) return;

        try {
            if (sent.getPhoto() == null || sent.getPhoto().isEmpty()) return;

            String fileId = sent.getPhoto().get(sent.getPhoto().size() - 1).getFileId();
            if (fileId == null || fileId.isEmpty()) return;

            cacheTelegramFileId(originalImg, fileId);
            logInfo(trace,
                    "Telegram图片FileId缓存成功"
                            + " FileId=" + cut(fileId));
        } catch (Exception e) {
            logWarn(trace,
                    "Telegram图片FileId缓存失败"
                            + " 异常类型=" + e.getClass().getSimpleName()
                            + " 错误=" + safeMsg(e.getMessage()));
        }
    }

    private boolean isLikelyFileIdInvalidError(String err) {
        if (err == null) return false;
        String low = err.toLowerCase();
        return low.contains("wrong file identifier")
                || low.contains("file_id")
                || low.contains("wrong remote file id")
                || low.contains("failed to get http url content")
                || low.contains("there is no photo in the request");
    }


    // ========== MarkdownV2 ==========
    /** 实现在 {@link TgTextUtil}，与 MsgUpdateStreamWorker 编辑路径共用同一份 */
    private static String normalizeAndEscapeFor(String s, int maxLen, String parseMode) {
        return TgTextUtil.normalizeAndEscape(s, maxLen, parseMode);
    }

    // ========== 抢占发送 ==========
    private boolean markMainSending(Long id) {
        if (id == null) return false;
        UpdateWrapper<CpBotmessageSendUser> uw = new UpdateWrapper<>();
        uw.eq("id", id).eq("status", 0).set("status", 9);
        return mainMapper.update(null, uw) == 1;
    }

    // ========== ACK ==========
    private void updateMainAck(Long id, Integer sentMessageId) {
        UpdateWrapper<CpBotmessageSendUser> uw = new UpdateWrapper<>();
        uw.eq("id", id)
          .set("status", 1)
          .set("returnmsg", "ok")
          .set("sendtime", Utils.getCurrentDateTimeForyyyyMMddHHmmss());
        /*
         * sentMessageId 为 null 表示本次没有真的调用 Telegram（幂等命中），
         * 此时保留原有 sendid，不要覆盖成空。
         */
        if (sentMessageId != null) {
            uw.set("sendid", String.valueOf(sentMessageId));
        }
        mainMapper.update(null, uw);
    }

    private void updateMainFail(Long id, String err) {
        UpdateWrapper<CpBotmessageSendUser> uw = new UpdateWrapper<>();
        uw.eq("id", id).set("returnmsg", err);

        if (shouldRetry(err)) {
            uw.set("status", 0);
        } else {
            uw.set("status", -1);
        }
        mainMapper.update(null, uw);
    }

    private boolean shouldRetry(String err) {
        if (err == null) return false;

        if (err.startsWith("429 ")) return true;
        if (err.startsWith("rate_limited")) return true;
        if (err.startsWith("cooldown")) return true;
        if (err.startsWith("inflight_full")) return true;
        if (err.startsWith("interrupted")) return true;
        if (err.startsWith("submit_rejected")) return true;
        if (err.startsWith("submit_ex")) return true;

        if (err.startsWith("download_fail")) {
            String low = err.toLowerCase();
            if (low.contains("http_code=403")
                    || low.contains("http_code=404")
                    || low.contains("not_image")
                    || low.contains("invalid_image_body")
                    || low.contains("too_large")) {
                return false;
            }
            return true;
        }

        if (err.startsWith("img_invalid_format")) return false;
        if (err.startsWith("photo_too_large")) return false;
        if (err.startsWith("expired")) return false;

        if (err.startsWith("tg error(400)")) return false;

        String low = err.toLowerCase();
        if (low.contains("wrong file identifier")) return true;
        if (low.contains("file_id")) return true;

        return low.contains("timeout")
                || low.contains("timed out")
                || low.contains("connection reset")
                || low.contains("broken pipe")
                || low.contains("unexpected end of stream")
                || low.contains("connection refused")
                || low.contains("all routes failed")
                || low.contains("ioexception")
                || low.contains("socketexception")
                || low.contains("connectexception");
    }

    // ========== 冷却 ==========
    private boolean isGlobalCooldown() {
        return System.currentTimeMillis() < cooldownUntilMs;
    }

    private void setCooldownSeconds(int seconds) {
        long ms = Math.max(1000L, (long) seconds * 1000L);
        cooldownUntilMs = System.currentTimeMillis() + ms;
    }

    // ========== 工具 ==========
    private boolean isExpired(String exptime) {
        if (exptime == null || exptime.isEmpty()) return false;
        try {
            LocalDateTime exp = LocalDateTime.parse(exptime, TS);
            return LocalDateTime.now(ZoneId.systemDefault())
                    .isAfter(exp.plus(Duration.ofMillis(EXPIRY_GRACE_MS)));
        } catch (Exception e) {
            return false;
        }
    }

    private void sleepQuietly(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private String newTrace(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private void logInfo(String msg) {
        System.out.println(LocalDateTime.now().format(LOG_TS) + " [INFO] " + msg);
    }

    private void logInfo(String trace, String msg) {
        System.out.println(LocalDateTime.now().format(LOG_TS) + " [INFO] [" + trace + "] " + msg);
    }

    private void logWarn(String msg) {
        System.out.println(LocalDateTime.now().format(LOG_TS) + " [WARN] " + msg);
    }

    private void logWarn(String trace, String msg) {
        System.out.println(LocalDateTime.now().format(LOG_TS) + " [WARN] [" + trace + "] " + msg);
    }

    private void logErr(String msg) {
        System.out.println(LocalDateTime.now().format(LOG_TS) + " [ERROR] " + msg);
    }

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }

    private static String rootCauseString(Throwable e) {
        Throwable r = e;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        String msg = r.getMessage();
        return r.getClass().getSimpleName() + (msg == null ? "" : (": " + safeMsg(msg)));
    }

    private static String sha1Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] dig = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(s.hashCode());
        }
    }

    private static String cut(String s) {
        if (s == null) return "null";
        return s.length() <= 140 ? s : s.substring(0, 140) + "...";
    }


    private boolean isGif(String img) {
        if (img == null) return false;

        String low = img.toLowerCase();

        int q = low.indexOf("?");
        if (q >= 0) {
            low = low.substring(0, q);
        }

        return low.endsWith(".gif");
    }
}