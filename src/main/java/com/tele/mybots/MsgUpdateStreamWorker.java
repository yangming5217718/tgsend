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
import com.tele.common.TgTextUtil;
import com.tele.common.RedisSlidingWindowRateLimiter;
import com.tele.common.Utils;
import com.tele.entity.CpBotmessageSendUser;
import com.tele.mapper.CpBotmessageSendUserMapper;
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

    /** 按 (chatid, sendid) 回查原始行，取 parsemode 和当初发出去的 buttontext */
    @Autowired
    private CpBotmessageSendUserMapper sendUserMapper;

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
    // buttontext 三态
    // ==========================================================
    /**
     * 事件里 buttontext 表达的三种意图，与 {@code InlineUpdateStreamWorker} 保持一致。
     * <p>
     * 以前空串和「清空」共用同一种表达，而两者的正确行为正好相反：
     * Telegram 的 editMessage* 只要不带 {@code reply_markup} 就等于<b>把键盘删掉</b>，
     * 不是「保持原样」。于是「上游没提按钮」被执行成了「上游要求删掉按钮」，
     * 而删键盘是一次合法编辑，返回 200，日志记的是成功。
     * <p>
     * 这条链路比 inline 更容易踩：文案没变化时会单独发一次
     * {@code EditMessageReplyMarkup}，markup 为 null 就是一次纯粹的删键盘请求。
     */
    private enum MarkupMode {
        /** 空 / 缺失 / 解析失败：本次不改按钮 */
        KEEP,
        /** 显式传 [] 或 [[]]：真的要清空 */
        CLEAR,
        /** 正常 JSON：换成这套 */
        SET
    }

    private record MarkupDecision(MarkupMode mode, InlineKeyboardMarkup markup) {
    }

    /**
     * 判定必须做在 {@link KeyboardUtil#createUserKeyboard} 之外。
     * 那个方法对空串和 {@code []} 都返回 null，到 markup 这一层两者已经分不出来了。
     */
    private MarkupDecision decideMarkup(String buttontext, String trace, String rid) {
        if (StringUtils.isBlank(buttontext)) {
            return new MarkupDecision(MarkupMode.KEEP, null);
        }

        String trimmed = buttontext.trim();
        if ("[]".equals(trimmed) || "[[]]".equals(trimmed)) {
            log(trace, "按钮显式清空 rid=" + rid + " buttontext=" + trimmed);
            return new MarkupDecision(MarkupMode.CLEAR, null);
        }

        InlineKeyboardMarkup markup;
        try {
            markup = KeyboardUtil.createUserKeyboard(buttontext);
        } catch (Exception e) {
            /*
             * 解析不了就当成「不改按钮」。原来是 markup 保持 null，等于静默降级成
             * 清空键盘——一个格式错误换来这条消息的按钮全没，代价太大。
             */
            log(trace, "keyboard parse fail，本次不改按钮 rid=" + rid + " err=" + e.getMessage());
            return new MarkupDecision(MarkupMode.KEEP, null);
        }

        if (markup == null) {
            log(trace, "按钮串没有有效按钮，本次不改按钮 rid=" + rid);
            return new MarkupDecision(MarkupMode.KEEP, null);
        }

        return new MarkupDecision(MarkupMode.SET, markup);
    }

    /** 只做解析，失败返回 null；给 KEEP 分支重建原键盘用 */
    private InlineKeyboardMarkup parseKeyboard(String buttontext, String trace, String rid) {
        if (StringUtils.isBlank(buttontext)) {
            return null;
        }
        try {
            return KeyboardUtil.createUserKeyboard(buttontext);
        } catch (Exception e) {
            log(trace, "重建原键盘失败 rid=" + rid + " err=" + e.getMessage());
            return null;
        }
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

            /*
             * 回查原始行，拿 parsemode 和当初发出去的 buttontext。
             *
             * 事件里的 msgid 是「要编辑哪条消息」，对应表上的 sendid 列，
             * 不是同名的 msgid 列（那个是回复目标）。查错列不会报错，
             * 只会永远查不到、一路走兜底，看起来一切正常。
             *
             * 查不到是正常情况：发失败的行没有 sendid，消息也可能由别的系统发出。
             */
            CpBotmessageSendUser origin = null;
            try {
                origin = sendUserMapper.selectByChatIdAndSendId(chatId, msgIdStr);
            } catch (Exception e) {
                log(trace, "origin lookup fail rid=" + rid + " err=" + e.getMessage());
            }
            log(trace, "origin " + (origin == null ? "not found" : "id=" + origin.getId()
                    + " parsemode=" + origin.getParsemode()
                    + " hasButton=" + StringUtils.isNotBlank(origin.getButtontext())));

            MarkupDecision decision = decideMarkup(buttontext, trace, rid);

            InlineKeyboardMarkup markup;
            if (decision.mode() == MarkupMode.KEEP) {
                /*
                 * KEEP = 本次不改按钮。但 Telegram 的 editMessage* 只要不带
                 * reply_markup 就等于把键盘删掉，没有「保持原样」这个选项。
                 * 所以必须拿原始 buttontext 重建一份原样发回去。
                 */
                if (origin == null) {
                    /*
                     * 重建不出来，只有两条路：照常编辑（文案更新，按钮悄悄没了，
                     * 日志还记成功），或者跳过。选跳过——看得见的故障比看不见的好。
                     */
                    log(trace, "SKIP 无法重建按钮：buttontext 为空且回查不到原始行"
                            + " rid=" + rid + " chatid=" + chatId + " msgid=" + msgIdStr);
                    ackAndDelete(r);
                    return;
                }
                markup = parseKeyboard(origin.getButtontext(), trace, rid);
            } else {
                markup = decision.markup();
            }

            boolean editedText = false;
            boolean editedCaption = false;
            boolean editedMarkup = false;

            if (StringUtils.isNotBlank(content)) {
                boolean needTryText = false;

                /*
                 * 必须转义，而且要用跟 OutboundSender 完全相同的实现。
                 *
                 * 这条链路的 content 和发送时是同一段文案（开奖播报、期号、金额），
                 * 发送侧转义了、编辑侧没转义的话，一段发得出去的文案一编辑就
                 * 400 can't parse entities——MarkdownV2 把 . 和 - 都列为保留字符，
                 * 而金额和时间里必然带。
                 *
                 * caption 和 text 的长度上限不同，各自按各自的截。
                 *
                 * parse mode 取回查到的行上的值，与发送时用的是同一个字段——
                 * 两边取值不同的话，同一段文案发得出去却编辑不了。
                 * 回查不到就走默认值，不因为拿不到 parsemode 而放弃编辑。
                 */
                String editParseMode = origin == null || StringUtils.isBlank(origin.getParsemode())
                        ? TgTextUtil.DEFAULT_PARSE_MODE
                        : origin.getParsemode().trim();
                String safeCaption = TgTextUtil.normalizeAndEscape(
                        content, TgTextUtil.TG_CAPTION_MAX, editParseMode);
                String safeText = TgTextUtil.normalizeAndEscape(
                        content, TgTextUtil.TG_TEXT_MAX, editParseMode);

                acquireRateToken();
                try {
                    EditMessageCaption req = EditMessageCaption.builder()
                            .chatId(chatId)
                            .messageId(msgId)
                            .caption(safeCaption)
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
                                .text(safeText)
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
