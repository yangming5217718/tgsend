package com.tele.mybots;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.ParseMode;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageCaption;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageReplyMarkup;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;

import com.tele.common.KeyboardUtil;
import com.tele.common.RedisSlidingWindowRateLimiter;
import com.tele.common.Utils;
import com.tele.mybots.router.TelegramFacade;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * 消费 {@code msg:update} 流，编辑已经发出去的普通消息。
 * <p>
 * 事件字段：{@code botcode / chatid / msgid / content / buttontext / opTime}。
 * {@code msgid} 是 Telegram 返回的 message_id，发送成功时由 OutboundSender
 * 写进 {@code cp_botmessage_send_user.sendid}。
 * <p>
 * 编辑顺序刻意不去猜消息类型：消息是文本还是带图，在发送那一刻就定死了，
 * 拿当前的 imgsrc 判断会判错。改成先试 caption，Telegram 报
 * 「there is no caption in the message to edit」再回退 text。
 */
@Component
public class MsgUpdateStreamWorker {

    private static final String DEFAULT_STREAM = "msg:update";
    private static final String DEFAULT_GROUP = "msg_update_worker";

    /*
     * 和 OutboundSender 共用同一个全局令牌桶 key。
     * 编辑和发送花的是同一个 bot 的 API 配额，各限各的等于把上限翻倍，照样撞 429。
     */
    private static final String RL_GLOBAL_KEY = "tg:rl:g";
    private static final double EDIT_RATE = 20.0;
    private static final int EDIT_BURST = 30;
    /** 等令牌的上限，超了就放行——编辑量本来不大，卡死比稍微超速更糟 */
    private static final int RL_MAX_WAIT_MS = 3000;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService exec;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private TelegramFacade tg;

    @Autowired
    private RedisSlidingWindowRateLimiter limiter;

    @Value("${app.msg.update.stream:" + DEFAULT_STREAM + "}")
    private String updateStream;

    @Value("${app.msg.update.group:" + DEFAULT_GROUP + "}")
    private String updateGroup;

    @Value("${app.msg.update.threads:2}")
    private int threads;

    @PostConstruct
    public void init() {
        try {
            ensureStreamAndGroup();
        } catch (Exception e) {
            log("init fail: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (running.get()) {
            return;
        }

        int n = Math.max(1, threads);
        exec = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r, "msg-update-worker");
            t.setDaemon(true);
            return t;
        });
        running.set(true);

        for (int i = 0; i < n; i++) {
            exec.submit(this::loop);
        }

        log("START stream=" + updateStream + " group=" + updateGroup + " threads=" + n);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (exec != null) {
            exec.shutdownNow();
        }
        log("STOP");
    }

    // ==========================================================
    // 流与消费组
    // ==========================================================
    private void ensureStreamAndGroup() {
        try {
            DataType dataType = redis.type(updateStream);

            if (dataType != null && dataType != DataType.NONE && dataType != DataType.STREAM) {
                log("stream key exists but wrong type, deleting key=" + updateStream
                        + ", type=" + dataType.code());
                redis.delete(updateStream);
            }

            if (!Boolean.TRUE.equals(redis.hasKey(updateStream))) {
                redis.opsForStream().add(updateStream, Map.of("boot", "1"));
                log("stream created: " + updateStream);
            }

            try {
                redis.opsForStream().createGroup(updateStream, ReadOffset.latest(), updateGroup);
                log("group created: " + updateGroup);
            } catch (Exception e) {
                log("group maybe exists: " + e.getMessage());
            }

            try {
                Long xlen = redis.opsForStream().size(updateStream);
                log("ensureStreamAndGroup done stream=" + updateStream
                        + " group=" + updateGroup + " xlen=" + xlen);
            } catch (Exception e) {
                log("ensureStreamAndGroup stat fail: " + e.getMessage());
            }

            logPendingStatus();

        } catch (Exception e) {
            log("ensureStreamAndGroup fail: " + e.getMessage());
            throw e;
        }
    }

    private void logPendingStatus() {
        try {
            PendingMessagesSummary summary = redis.opsForStream().pending(updateStream, updateGroup);
            if (summary == null) {
                log("pending summary is null stream=" + updateStream + " group=" + updateGroup);
                return;
            }
            log("pending summary stream=" + updateStream
                    + " group=" + updateGroup
                    + " totalPending=" + summary.getTotalPendingMessages());
        } catch (Exception e) {
            log("pending summary fail: " + e.getMessage());
        }
    }

    // ==========================================================
    // 主循环
    // ==========================================================
    private void loop() {
        final String consumerName = "mu-" + UUID.randomUUID();
        final StreamReadOptions opts = StreamReadOptions.empty()
                .count(20)
                .block(Duration.ofSeconds(2));

        int backoffMs = 200;
        int idleCount = 0;

        log("loop start consumer=" + consumerName);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                        Consumer.from(updateGroup, consumerName),
                        opts,
                        StreamOffset.create(updateStream, ReadOffset.lastConsumed())
                );

                backoffMs = 200;

                if (records == null || records.isEmpty()) {
                    idleCount++;
                    if (idleCount % 60 == 0) {
                        try {
                            Long xlen = redis.opsForStream().size(updateStream);
                            log("loop idle consumer=" + consumerName
                                    + " idleCount=" + idleCount + " xlen=" + xlen);
                        } catch (Exception ignore) {
                        }
                    }
                    continue;
                }

                idleCount = 0;
                log("loop read consumer=" + consumerName + " size=" + records.size());

                for (MapRecord<String, Object, Object> r : records) {
                    handleOne(r);
                }

            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted() || !running.get()) {
                    log("loop interrupted consumer exit");
                    return;
                }

                log("loop error consumer=" + consumerName + " err=" + e.getMessage());
                e.printStackTrace();

                try {
                    ensureStreamAndGroup();
                } catch (Exception rebuildEx) {
                    log("ensureStreamAndGroup in loop fail: " + rebuildEx.getMessage());
                }

                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);
            }
        }

        log("loop end consumer=" + consumerName);
    }

    // ==========================================================
    // 单条处理
    // ==========================================================
    private void handleOne(MapRecord<String, Object, Object> r) {
        String rid = r.getId().getValue();
        String trace = newTrace();

        try {
            String chatId = clean(val(r, "chatid"));
            String msgIdStr = clean(val(r, "msgid"));
            String content = clean(val(r, "content"));
            String buttontext = clean(val(r, "buttontext"));
            String opTime = clean(val(r, "opTime"));

            log(trace, "HANDLE rid=" + rid
                    + " chatid=" + debugValue(chatId)
                    + " msgid=" + debugValue(msgIdStr)
                    + " contentLen=" + (content == null ? 0 : content.length())
                    + " hasButton=" + StringUtils.isNotBlank(buttontext)
                    + " opTime=" + debugValue(opTime));

            // chatid 和 msgid 缺一不可，没有它们定位不到要编辑哪条消息
            if (StringUtils.isBlank(chatId) || StringUtils.isBlank(msgIdStr)) {
                log(trace, "SKIP missing chatid/msgid rid=" + rid);
                ackAndDelete(r);
                return;
            }

            if (!StringUtils.isNumeric(msgIdStr)) {
                log(trace, "SKIP invalid msgid rid=" + rid + " msgid=" + msgIdStr);
                ackAndDelete(r);
                return;
            }

            Integer msgId = Integer.valueOf(msgIdStr);

            InlineKeyboardMarkup markup = null;
            try {
                if (StringUtils.isNotBlank(buttontext)) {
                    markup = KeyboardUtil.createUserKeyboard(buttontext);
                }
            } catch (Exception e) {
                log(trace, "keyboard parse fail rid=" + rid + " err=" + e.getMessage());
            }

            boolean editedText = false;
            boolean editedCaption = false;
            boolean editedMarkup = false;

            if (StringUtils.isNotBlank(content)) {
                boolean needTryText = false;

                acquireRateToken();
                try {
                    EditMessageCaption req = EditMessageCaption.builder()
                            .chatId(chatId)
                            .messageId(msgId)
                            .caption(content)
                            .parseMode(ParseMode.MARKDOWNV2)
                            .replyMarkup(markup)
                            .build();

                    tg.execute(trace, req);
                    editedCaption = true;
                    editedMarkup = true;

                    log(trace, "edit caption ok rid=" + rid + " chatid=" + chatId + " msgid=" + msgId);
                } catch (TelegramApiException e) {
                    if (isNotModified(e)) {
                        // 目标状态已经达成，算成功，重试永远不会好
                        log(trace, "edit caption not modified rid=" + rid);
                        editedMarkup = true;
                    } else if (isNoCaptionToEdit(e) || isMessageNotModifiedAsCaptionType(e)) {
                        needTryText = true;
                        log(trace, "edit caption skipped rid=" + rid
                                + " reason=no caption in original message, fallback to text");
                    } else {
                        log(trace, "edit caption fail rid=" + rid + " err=" + e.getMessage());
                        throw e;
                    }
                }

                if (needTryText) {
                    acquireRateToken();
                    try {
                        EditMessageText req = EditMessageText.builder()
                                .chatId(chatId)
                                .messageId(msgId)
                                .text(content)
                                .parseMode(ParseMode.MARKDOWNV2)
                                .replyMarkup(markup)
                                .build();

                        tg.execute(trace, req);
                        editedText = true;
                        editedMarkup = true;

                        log(trace, "edit text ok rid=" + rid + " chatid=" + chatId + " msgid=" + msgId);
                    } catch (TelegramApiException e) {
                        if (isNotModified(e)) {
                            log(trace, "edit text not modified rid=" + rid);
                            editedMarkup = true;
                        } else if (isNoTextToEdit(e)) {
                            log(trace, "edit text skipped rid=" + rid
                                    + " reason=no text in original message");
                        } else {
                            log(trace, "edit text fail rid=" + rid + " err=" + e.getMessage());
                            throw e;
                        }
                    }
                }
            }

            if (!editedMarkup) {
                acquireRateToken();
                try {
                    EditMessageReplyMarkup req = EditMessageReplyMarkup.builder()
                            .chatId(chatId)
                            .messageId(msgId)
                            .replyMarkup(markup)
                            .build();

                    tg.execute(trace, req);
                    editedMarkup = true;
                    log(trace, "edit markup ok rid=" + rid + " chatid=" + chatId + " msgid=" + msgId);
                } catch (TelegramApiException e) {
                    if (isNotModified(e)) {
                        log(trace, "edit markup not modified rid=" + rid);
                    } else {
                        log(trace, "edit markup fail rid=" + rid + " err=" + e.getMessage());
                        throw e;
                    }
                }
            }

            log(trace, "DONE rid=" + rid
                    + " editedText=" + editedText
                    + " editedCaption=" + editedCaption
                    + " editedMarkup=" + editedMarkup);

            ackAndDelete(r);

        } catch (Exception e) {
            /*
             * 失败也 ack + 删除：这条流没有重试语义，重发由上游负责。
             * 留在 pending 里只会让后面每次扫描都重新捞到它。
             */
            log(trace, "HANDLE fail rid=" + rid + " err=" + e.getMessage());
            ackAndDelete(r);
        }
    }

    // ==========================================================
    // 生产者
    // ==========================================================
    public void publishMessageUpdate(String chatId, Integer msgId,
                                     String content, String buttontext) {
        try {
            if (StringUtils.isBlank(chatId) || msgId == null) {
                log("publishMessageUpdate skip invalid args");
                return;
            }

            Map<String, String> msg = Map.of(
                    "chatid", chatId,
                    "msgid", String.valueOf(msgId),
                    "content", StringUtils.defaultString(content),
                    "buttontext", StringUtils.defaultString(buttontext),
                    "opTime", Utils.getCurrentDateTimeForyyyyMMddHHmmss()
            );

            redis.opsForStream().add(updateStream, msg);
            log("publishMessageUpdate ok chatid=" + chatId + " msgid=" + msgId);

        } catch (Exception e) {
            log("publishMessageUpdate fail err=" + e.getMessage());
        }
    }

    // ==========================================================
    // 工具
    // ==========================================================
    private void acquireRateToken() {
        long deadline = System.currentTimeMillis() + RL_MAX_WAIT_MS;
        while (!limiter.allowOnceBurst(RL_GLOBAL_KEY, EDIT_RATE, EDIT_BURST)) {
            if (System.currentTimeMillis() >= deadline) {
                log("rate token wait timeout, proceeding anyway");
                return;
            }
            if (!sleepQuietly(50)) {
                return;
            }
        }
    }

    private void ackAndDelete(MapRecord<String, Object, Object> r) {
        String rid = r.getId().getValue();
        try {
            redis.opsForStream().acknowledge(updateStream, updateGroup, r.getId());
            try {
                redis.opsForStream().delete(updateStream, r.getId());
            } catch (Exception de) {
                log("delete fail rid=" + rid + " err=" + de.getMessage());
            }
        } catch (Exception e) {
            log("ack fail rid=" + rid + " err=" + e.getMessage());
        }
    }

    private boolean isNotModified(TelegramApiException e) {
        return contains(e, "not modified");
    }

    private boolean isNoTextToEdit(TelegramApiException e) {
        return contains(e, "there is no text in the message to edit");
    }

    private boolean isNoCaptionToEdit(TelegramApiException e) {
        return contains(e, "there is no caption in the message to edit")
                || contains(e, "message caption is empty")
                || contains(e, "there is no caption");
    }

    private boolean isMessageNotModifiedAsCaptionType(TelegramApiException e) {
        return contains(e, "message can't be edited")
                || contains(e, "message content is not modified");
    }

    private boolean contains(TelegramApiException e, String needle) {
        if (e == null) return false;
        return String.valueOf(e.getMessage()).toLowerCase().contains(needle);
    }

    private String val(MapRecord<String, Object, Object> r, String key) {
        Object v = r.getValue().get(key);
        return v == null ? null : String.valueOf(v);
    }

    /**
     * 上游有时会把值连着 JSON 引号一起写进流字段，这里剥掉。
     */
    private String clean(String s) {
        if (s == null) return null;
        String v = s.trim();
        if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            v = v.substring(1, v.length() - 1);
        }
        return v.replace("\\\"", "\"").trim();
    }

    private boolean sleepQuietly(int ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private String newTrace() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private String debugValue(String v) {
        return v == null ? "null" : "[" + v + "]";
    }

    private void log(String msg) {
        System.out.println("[MSG-UPDATE] " + msg);
    }

    private void log(String trace, String msg) {
        System.out.println("[MSG-UPDATE][" + trace + "] " + msg);
    }
}
