package com.tele.mybots;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tele.entity.CpConfig;
import com.tele.mapper.CpConfigMapper;

import jakarta.annotation.PreDestroy;

/**
 * 用 long polling（getUpdates）拉取 update，塞进和 webhook 同一条 {@code tg:updates} 流。
 * <p>
 * <b>默认关闭</b>，只有显式配 {@code app.polling.enabled=true} 才会注册这个 Bean。
 * <p>
 * 为什么需要它：webhook 要求公网 HTTPS（端口只能 443/80/88/8443），而本服务
 * 是裸 HTTP 9999、机器上没有 TLS 终结、入站端口也被防火墙挡着。getUpdates 是
 * <b>出站拉取</b>，一个公网入口都不需要，联调时零基础设施改动。
 * <p>
 * 一个 bot 只能二选一：设了 webhook 就不能 getUpdates（Telegram 返回 409）。
 * 所以这里用的是专门的测试 bot（没有 webhook），和线上那个走 webhook 的 bot 互不干扰。
 * <p>
 * 入流的字段与 {@code TelegramWebhookController} 完全一致（{@code update} / {@code ts}），
 * 所以下游 {@code UpdateWorker} 及其后的整条链路一行都不用改。
 */
@Component
@ConditionalOnProperty(name = "app.polling.enabled", havingValue = "true")
public class TelegramPollingWorker {

    private static final Logger log = LoggerFactory.getLogger(TelegramPollingWorker.class);

    private static final String API_BASE = "https://api.telegram.org/bot";

    /**
     * 必须显式列全。allowed_updates 是<b>全量替换</b>语义，漏掉哪一类就永远收不到那一类。
     * inline 两项是 inline 分享链路的命脉：
     * inline_query 触发预创建实例，chosen_inline_result 回填 inline_message_id。
     */
    private static final String ALLOWED_UPDATES =
            "[\"message\",\"callback_query\",\"inline_query\",\"chosen_inline_result\"]";

    /** long poll 挂起秒数。Telegram 侧最长 50，留点余量 */
    private static final int POLL_TIMEOUT_SEC = 25;

    /** offset 存 Redis，重启后不重复消费已确认的 update */
    private static final String OFFSET_KEY_PREFIX = "tg:polling:offset:";

    private final ObjectMapper om = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService exec;
    private HttpClient http;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private CpConfigMapper cpConfigMapper;

    /** 从 cp_config 里按这个 code 取 token，默认取测试 bot 那一行 */
    @Value("${app.polling.token-code:bot_token_test}")
    private String tokenCode;

    @Value("${app.redis.streams.updates:tg:updates}")
    private String updatesStream;

    /** webhook 那边的幂等 TTL 是 1 小时，这里保持一致 */
    @Value("${app.polling.idem-hours:1}")
    private long idemHours;

    private volatile String token;
    private volatile String botTag;

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (running.get()) {
            return;
        }

        CpConfig cfg = cpConfigMapper.selectConfig(tokenCode);
        if (cfg == null || StringUtils.isBlank(cfg.getValue())) {
            log.error("[POLLING] cp_config 里没有可用的 code={}，轮询不启动", tokenCode);
            return;
        }
        this.token = cfg.getValue().trim();
        this.botTag = this.token.split(":")[0];   // 只记 bot_id，不记 token

        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        running.set(true);
        exec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "tg-polling-worker");
            t.setDaemon(true);
            return t;
        });
        exec.submit(this::loop);

        log.info("[POLLING] 启动 tokenCode={} botId={} stream={} allowed_updates={}",
                tokenCode, botTag, updatesStream, ALLOWED_UPDATES);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (exec != null) {
            exec.shutdownNow();
        }
        log.info("[POLLING] 停止");
    }

    // ==========================================================
    // 主循环
    // ==========================================================
    private void loop() {
        int backoffMs = 1000;
        long offset = readOffset();
        log.info("[POLLING] 从 offset={} 开始", offset);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                String body = getUpdates(offset);
                if (body == null) {
                    sleepQuietly(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 30000);
                    continue;
                }

                JsonNode root = om.readTree(body);
                if (!root.path("ok").asBoolean(false)) {
                    /*
                     * 409 Conflict 说明这个 bot 设了 webhook——两种模式不能并存。
                     * 这种错误退避再久也不会自己好，要人去把 webhook 删掉，所以单独提示。
                     */
                    String desc = root.path("description").asText("");
                    if (desc.contains("terminated by other getUpdates")
                            || desc.contains("can't use getUpdates")) {
                        log.error("[POLLING] {} —— 该 bot 设了 webhook 或有别的进程在拉，本实例退避后重试", desc);
                    } else {
                        log.warn("[POLLING] getUpdates 返回失败: {}", StringUtils.abbreviate(desc, 200));
                    }
                    sleepQuietly(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 30000);
                    continue;
                }

                backoffMs = 1000;

                JsonNode results = root.path("result");
                if (!results.isArray() || results.isEmpty()) {
                    continue;
                }

                for (JsonNode update : results) {
                    long updateId = update.path("update_id").asLong(-1);
                    if (updateId >= 0) {
                        offset = updateId + 1;   // 下一轮带上，等于向 Telegram 确认已收
                    }
                    publish(update, updateId);
                }
                writeOffset(offset);

            } catch (Exception e) {
                if (!running.get() || Thread.currentThread().isInterrupted()) {
                    return;
                }
                log.error("[POLLING] 循环异常，{}ms 后重试", backoffMs, e);
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 30000);
            }
        }
        log.info("[POLLING] 循环退出");
    }

    private String getUpdates(long offset) {
        try {
            String url = API_BASE + token + "/getUpdates"
                    + "?timeout=" + POLL_TIMEOUT_SEC
                    + "&offset=" + offset
                    + "&allowed_updates=" + java.net.URLEncoder.encode(ALLOWED_UPDATES, StandardCharsets.UTF_8);

            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(POLL_TIMEOUT_SEC + 15))
                    .GET()
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() / 100 != 2) {
                log.warn("[POLLING] HTTP {} body={}", resp.statusCode(),
                        StringUtils.abbreviate(resp.body(), 200));
                // 4xx/5xx 的 body 里也有 description，交给上面统一解析
                return resp.body();
            }
            return resp.body();

        } catch (Exception e) {
            log.warn("[POLLING] getUpdates 请求失败: {}", safeMsg(e.getMessage()));
            return null;
        }
    }

    /**
     * 塞进 tg:updates，字段与 webhook 入口逐字一致，下游无感。
     */
    private void publish(JsonNode update, long updateId) {
        try {
            /*
             * 幂等键沿用 webhook 那套。getUpdates 本身在确认 offset 前会重投，
             * 进程崩在「已发流、未写 offset」之间时会重复拉到同一条。
             */
            String idemKey = "tg:idm:" + updateId;
            Boolean first = redis.opsForValue().setIfAbsent(idemKey, "1", Duration.ofHours(idemHours));
            if (Boolean.FALSE.equals(first)) {
                log.debug("[POLLING] update_id={} 已处理过，跳过", updateId);
                return;
            }

            Map<String, String> fields = new HashMap<>();
            fields.put("update", om.writeValueAsString(update));
            fields.put("ts", String.valueOf(System.currentTimeMillis()));

            redis.opsForStream().add(
                    StreamRecords.newRecord().ofMap(fields).withStreamKey(updatesStream));

            log.info("[POLLING] 已入流 update_id={} 类型={}", updateId, updateType(update));

        } catch (Exception e) {
            log.error("[POLLING] 入流失败 update_id={}", updateId, e);
        }
    }

    private String updateType(JsonNode u) {
        for (String k : new String[]{"message", "callback_query", "inline_query",
                "chosen_inline_result", "chat_member", "my_chat_member"}) {
            if (!u.path(k).isMissingNode()) {
                return k;
            }
        }
        return "unknown";
    }

    // ==========================================================
    // offset 持久化
    // ==========================================================
    private long readOffset() {
        try {
            String s = redis.opsForValue().get(OFFSET_KEY_PREFIX + botTag);
            if (StringUtils.isNotBlank(s) && StringUtils.isNumeric(s)) {
                return Long.parseLong(s);
            }
        } catch (Exception e) {
            log.warn("[POLLING] 读 offset 失败，从 0 开始: {}", safeMsg(e.getMessage()));
        }
        return 0L;
    }

    private void writeOffset(long offset) {
        try {
            redis.opsForValue().set(OFFSET_KEY_PREFIX + botTag, String.valueOf(offset));
        } catch (Exception e) {
            log.warn("[POLLING] 写 offset 失败: {}", safeMsg(e.getMessage()));
        }
    }

    private void sleepQuietly(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }
}
