package com.tele.mybots;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageCaption;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageReplyMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.tele.common.KeyboardUtil;
import com.tele.common.RedisSlidingWindowRateLimiter;
import com.tele.common.Utils;
import com.tele.entity.CpBotmessageSendInline;
import com.tele.entity.CpBotmessageSendInlineItem;
import com.tele.mapper.CpBotmessageSendInlineItemMapper;
import com.tele.mapper.CpBotmessageSendInlineMapper;
import com.tele.mybots.router.TelegramFacade;
import com.tele.service.InlineItemIdInjector;
import com.tele.service.InlineQueryService;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * 消费 {@code inline:update} 流，更新已经分享出去的 inline 消息。
 * <p>
 * 事件两种形态：
 * <ul>
 *   <li>带 {@code itemId} —— 只更新那一个实例。<b>buttontext 是为这个实例单独拼的
 *       （里面带着它自己的 itemId），绝不能写回母版</b>，否则母版会被最后一个实例的
 *       按钮覆盖，从此不再是母版。</li>
 *   <li>只带 {@code inlineId} —— buttontext 是母版级的，更新母版并刷新它的全部实例。</li>
 * </ul>
 * 处理完一律 ack + XDEL：这条流没有重试语义，重发由上游负责，
 * 留在 pending 里只会让后面每次扫描都重新捞到它。
 */
@Component
public class InlineUpdateStreamWorker {

    private static final Logger log = LoggerFactory.getLogger(InlineUpdateStreamWorker.class);

    private static final String DEFAULT_STREAM = "inline:update";
    private static final String DEFAULT_GROUP = "inline_update_worker";

    private static final String INLINE_SENT_SET_PREFIX = "inline:sent:";
    private static final String INLINE_META_PREFIX = "inline:meta:";
    private static final String INLINE_ITEM_META_PREFIX = "inline:item:";

    /* 与 OutboundSender / MsgUpdateStreamWorker 共用同一个全局令牌桶 */
    private static final String RL_GLOBAL_KEY = "tg:rl:g";
    private static final double EDIT_RATE = 20.0;
    private static final int EDIT_BURST = 30;
    private static final int RL_MAX_WAIT_MS = 3000;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService exec;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private TelegramFacade tg;

    @Autowired
    private RedisSlidingWindowRateLimiter limiter;

    @Autowired
    private CpBotmessageSendInlineMapper inlineMapper;

    @Autowired
    private CpBotmessageSendInlineItemMapper itemMapper;

    @Autowired
    private InlineQueryService inlineQueryService;

    /** KEEP 分支重建实例键盘时用，跟分享时是同一个注入器 */
    @Autowired
    private InlineItemIdInjector itemIdInjector;

    @Value("${app.inline.update.stream:" + DEFAULT_STREAM + "}")
    private String updateStream;

    @Value("${app.inline.update.group:" + DEFAULT_GROUP + "}")
    private String updateGroup;

    @Value("${app.inline.update.threads:1}")
    private int threads;

    @PostConstruct
    public void init() {
        try {
            ensureStreamAndGroup();
        } catch (Exception e) {
            log.error("[INLINE-UPDATE] init fail", e);
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (running.get()) {
            return;
        }

        int n = Math.max(1, threads);
        exec = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r, "inline-update-worker");
            t.setDaemon(true);
            return t;
        });
        running.set(true);

        for (int i = 0; i < n; i++) {
            exec.submit(this::loop);
        }

        log.info("[INLINE-UPDATE] START stream={} group={} threads={}", updateStream, updateGroup, n);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (exec != null) {
            exec.shutdownNow();
        }
        log.info("[INLINE-UPDATE] STOP");
    }

    // ==========================================================
    // 流与消费组
    // ==========================================================
    private void ensureStreamAndGroup() {
        DataType dataType = redis.type(updateStream);
        if (dataType != null && dataType != DataType.NONE && dataType != DataType.STREAM) {
            log.warn("[INLINE-UPDATE] key 类型不对，删除重建 key={} type={}", updateStream, dataType.code());
            redis.delete(updateStream);
        }

        if (!Boolean.TRUE.equals(redis.hasKey(updateStream))) {
            redis.opsForStream().add(updateStream, Map.of("boot", "1"));
            log.info("[INLINE-UPDATE] stream created: {}", updateStream);
        }

        try {
            redis.opsForStream().createGroup(updateStream, ReadOffset.latest(), updateGroup);
            log.info("[INLINE-UPDATE] group created: {}", updateGroup);
        } catch (Exception e) {
            log.info("[INLINE-UPDATE] group 可能已存在: {}", e.getMessage());
        }

        try {
            log.info("[INLINE-UPDATE] ensureStreamAndGroup done stream={} group={} xlen={}",
                    updateStream, updateGroup, redis.opsForStream().size(updateStream));
        } catch (Exception ignore) {
        }
    }

    // ==========================================================
    // 主循环
    // ==========================================================
    private void loop() {
        final String consumerName = "iu-" + UUID.randomUUID();
        final StreamReadOptions opts = StreamReadOptions.empty()
                .count(20)
                .block(Duration.ofSeconds(2));

        int backoffMs = 200;
        int idleCount = 0;

        log.info("[INLINE-UPDATE] loop start consumer={}", consumerName);

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
                        log.info("[INLINE-UPDATE] loop idle consumer={} idleCount={}", consumerName, idleCount);
                    }
                    continue;
                }

                idleCount = 0;

                for (MapRecord<String, Object, Object> r : records) {
                    String rid = r.getId().getValue();
                    try {
                        String inlineId = clean(val(r, "inlineId"));
                        String itemIdStr = clean(val(r, "itemId"));
                        String content = clean(val(r, "content"));
                        String buttontext = clean(val(r, "buttontext"));

                        Long itemId = null;
                        if (StringUtils.isNotBlank(itemIdStr) && StringUtils.isNumeric(itemIdStr)) {
                            itemId = Long.valueOf(itemIdStr);
                        }

                        /*
                         * itemId 优先：一条消息里两个字段可能都有，
                         * 带 itemId 就说明这是给某一个实例的定向更新。
                         */
                        if (itemId != null) {
                            handleInlineUpdateEventSingle(itemId, content, buttontext);
                        } else if (StringUtils.isNotBlank(inlineId)) {
                            handleInlineUpdateEvent(inlineId, content, buttontext);
                        } else {
                            log.warn("[INLINE-UPDATE] inlineId 和 itemId 都为空，丢弃 rid={}", rid);
                        }
                    } catch (Exception ex) {
                        log.error("[INLINE-UPDATE] 处理失败 rid={}", rid, ex);
                    } finally {
                        ackAndDelete(r);
                    }
                }

            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted() || !running.get()) {
                    log.info("[INLINE-UPDATE] loop interrupted, exit");
                    return;
                }

                log.error("[INLINE-UPDATE] loop error", e);
                try {
                    ensureStreamAndGroup();
                } catch (Exception rebuildEx) {
                    log.error("[INLINE-UPDATE] ensureStreamAndGroup in loop fail", rebuildEx);
                }

                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);
            }
        }

        log.info("[INLINE-UPDATE] loop end consumer={}", consumerName);
    }

    // ==========================================================
    // 母版级更新：刷新该母版的全部实例
    // ==========================================================
    public void handleInlineUpdateEvent(String inlineId, String content, String buttontext) {
        String trace = newTrace();
        String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();

        try {
            CpBotmessageSendInline main = inlineMapper.selectById(inlineId);
            if (main == null) {
                log.warn("[INLINE-UPDATE][{}] 母版不存在 inlineId={}", trace, inlineId);
                return;
            }

            MarkupDecision decision = decideMarkup(buttontext, trace);

            CpBotmessageSendInline upd = new CpBotmessageSendInline();
            upd.setId(inlineId);
            if (StringUtils.isNotBlank(content)) {
                upd.setContent(content);
            }
            /*
             * 这条分支的 buttontext 是母版级的，写回母版是对的——
             * 但仅限于本次真的表达了按钮意图。KEEP 表示「不改按钮」，
             * 照写会把母版的按钮配置抹成空串，且无从恢复。
             */
            if (decision.mode() != MarkupMode.KEEP) {
                upd.setButtontext(buttontext);
            }
            upd.setSendtime(now);
            inlineMapper.updateById(upd);

            // KEEP：用母版现有按钮重建，让这次编辑把原键盘原样发回去
            InlineKeyboardMarkup markup = decision.mode() == MarkupMode.KEEP
                    ? parseKeyboard(main.getButtontext(), trace)
                    : decision.markup();

            int ok = batchEdit(inlineId, content, markup, resolveParseMode(main), trace);

            log.info("[INLINE-UPDATE][{}] 母版更新完成 inlineId={} editSuccess={}",
                    trace, inlineId, ok);

        } catch (Exception e) {
            log.error("[INLINE-UPDATE][{}] 母版更新失败 inlineId={}", trace, inlineId, e);
        }
    }

    // ==========================================================
    // 实例级更新：只动一个实例
    // ==========================================================
    public void handleInlineUpdateEventSingle(Long itemId, String content, String buttontext) {
        String trace = newTrace();
        String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();

        try {
            CpBotmessageSendInlineItem item = itemMapper.selectById(itemId);
            if (item == null) {
                log.warn("[INLINE-UPDATE][{}] 实例不存在 itemId={}", trace, itemId);
                return;
            }

            String inlineId = item.getInlineId();
            String inlineMessageId = item.getInlineMessageId();

            if (StringUtils.isBlank(inlineMessageId)) {
                log.warn("[INLINE-UPDATE][{}] 实例没有 inline_message_id，无法编辑 itemId={}", trace, itemId);
                return;
            }

            /*
             * parsemode 存在母版上，实例表没有这一列，所以要回查母版。
             * 查不到就走默认值，不因为拿不到 parsemode 而放弃编辑。
             */
            CpBotmessageSendInline main = StringUtils.isBlank(inlineId)
                    ? null
                    : inlineMapper.selectById(inlineId);

            if (StringUtils.isNotBlank(inlineId)) {
                CpBotmessageSendInline upd = new CpBotmessageSendInline();
                upd.setId(inlineId);
                if (StringUtils.isNotBlank(content)) {
                    upd.setContent(content);
                }
                /*
                 * 这里刻意不写 buttontext。
                 *
                 * 带 itemId 的事件，buttontext 是为这一个实例单独拼的
                 * （startapp 尾号是它自己的 itemId）。写回母版的话，
                 * 上游逐实例循环推送时最后一个实例会赢，母版从此带着某个实例的 id，
                 * 不再是母版。
                 *
                 * content 不一样：它是整条分享的文案，所有实例共用，照写。
                 * 母版级的 buttontext 由不带 itemId 的事件更新。
                 */
                upd.setSendtime(now);
                inlineMapper.updateById(upd);
            }

            MarkupDecision decision = decideMarkup(buttontext, trace);

            /*
             * KEEP 时要还原这一条实例自己的键盘。实例表没有 buttontext 列，
             * 能重建它的唯一材料就是「母版串 + 这条实例的 itemId」，
             * 用的还是分享时那个注入器、那个 itemId。
             *
             * 注意这不是「消费端改写上游给的按钮」——上游明确说了别动按钮，
             * 直接拿母版串会把这条实例的 itemId 抹掉，那才是改写。
             *
             * 局限：母版 buttontext 若在分享之后被改过，重建的是「按新母版重新注入」，
             * 不是客户端当前显示的那份。要严格保真得给实例表加列存实际发出的按钮。
             */
            InlineKeyboardMarkup markup;
            if (decision.mode() == MarkupMode.KEEP) {
                String rebuilt = main == null
                        ? null
                        : itemIdInjector.inject(main.getButtontext(), itemId);
                markup = parseKeyboard(rebuilt, trace);
            } else {
                markup = decision.markup();
            }

            boolean ok = editOne(inlineMessageId, content, markup, resolveParseMode(main), trace);

            if (ok) {
                item.setUpdatetime(now);
                itemMapper.updateById(item);
                writeItemMeta(item);
            }

            log.info("[INLINE-UPDATE][{}] 实例更新 itemId={} inlineMessageId={} ok={}",
                    trace, itemId, inlineMessageId, ok);

        } catch (Exception e) {
            log.error("[INLINE-UPDATE][{}] 实例更新失败 itemId={}", trace, itemId, e);
        }
    }

    // ==========================================================
    // 编辑
    // ==========================================================
    private int batchEdit(String inlineId, String content, InlineKeyboardMarkup markup,
                          String parseMode, String trace) {
        Set<String> ids = inlineQueryService.getInlineMessageIds(inlineId);
        if (ids.isEmpty()) {
            log.info("[INLINE-UPDATE][{}] 母版没有有效实例 inlineId={}", trace, inlineId);
            return 0;
        }

        int ok = 0;
        int fail = 0;
        for (String imid : ids) {
            if (editOne(imid, content, markup, parseMode, trace)) {
                ok++;
            } else {
                fail++;
            }
        }

        log.info("[INLINE-UPDATE][{}] 批量编辑完成 inlineId={} success={} fail={}",
                trace, inlineId, ok, fail);
        return ok;
    }

    /**
     * 取母版声明的 parsemode，跟 {@code InlineQueryService} 发送时用的是同一个字段和同一个默认值。
     * <p>
     * 两边必须一致：发送用 legacy markdown、编辑用 MarkdownV2 的话，同一段文案发得出去却编辑不了——
     * MarkdownV2 把 {@code = . - ! ( ) # + _ * [ ] ~ > |} 全列为保留字符，
     * 而金额、时间、小数这些真实文案里几乎必然带 {@code .} 或 {@code -}。
     * 那种失败还是静默的：{@code shouldInvalidate} 不认 {@code can't parse entities}，
     * 实例状态不变，下一次推送照样全挂。
     */
    private String resolveParseMode(CpBotmessageSendInline main) {
        if (main == null || StringUtils.isBlank(main.getParsemode())) {
            return "markdown";
        }
        return main.getParsemode().trim();
    }

    /**
     * inline 消息没有 chat_id，只能靠 inline_message_id 定位。
     * content 为空时只换按钮。
     */
    private boolean editOne(String inlineMessageId, String content,
                            InlineKeyboardMarkup markup, String parseMode, String trace) {
        acquireRateToken();

        try {
            if (StringUtils.isNotBlank(content)) {
                tg.execute(trace, EditMessageCaption.builder()
                        .inlineMessageId(inlineMessageId)
                        .caption(content)
                        .parseMode(parseMode)
                        .replyMarkup(markup)
                        .build());
            } else {
                tg.execute(trace, EditMessageReplyMarkup.builder()
                        .inlineMessageId(inlineMessageId)
                        .replyMarkup(markup)
                        .build());
            }
            return true;

        } catch (Exception e) {
            String low = String.valueOf(e.getMessage()).toLowerCase();

            /* 内容一模一样，目标状态已经达成，重试永远不会好 */
            if (low.contains("not modified")) {
                return true;
            }

            if (shouldInvalidate(low)) {
                log.warn("[INLINE-UPDATE][{}] 实例已失效，标记 status=-1 inlineMessageId={} err={}",
                        trace, inlineMessageId, safeMsg(e.getMessage()));
                markInlineMessageInvalid(inlineMessageId);
                return false;
            }

            log.warn("[INLINE-UPDATE][{}] 编辑失败 inlineMessageId={} err={}",
                    trace, inlineMessageId, safeMsg(e.getMessage()));
            return false;
        }
    }

    /** 这些错误说明这条 inline 消息永远改不了了，继续重试只是白烧配额 */
    private boolean shouldInvalidate(String low) {
        return low.contains("message not found")
                || low.contains("message to edit not found")
                || low.contains("message can't be edited")
                || low.contains("message_id_invalid")
                || low.contains("inline message id invalid")
                || low.contains("invalid inline message id")
                || low.contains("message identifier is not specified");
    }

    public void markInlineMessageInvalid(String inlineMessageId) {
        if (StringUtils.isBlank(inlineMessageId)) {
            return;
        }
        String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();

        try {
            CpBotmessageSendInlineItem item = itemMapper.selectOne(
                    new QueryWrapper<CpBotmessageSendInlineItem>()
                            .eq("inline_message_id", inlineMessageId)
                            .last("limit 1"));

            if (item != null) {
                item.setStatus(-1);
                item.setUpdatetime(now);
                itemMapper.updateById(item);

                // 库和 Redis 索引要一起摘，否则下次批量编辑还会捞到它
                if (StringUtils.isNotBlank(item.getInlineId())) {
                    try {
                        redis.opsForSet().remove(INLINE_SENT_SET_PREFIX + item.getInlineId(), inlineMessageId);
                    } catch (Exception ignore) {
                    }
                }
                if (item.getId() != null) {
                    try {
                        redis.delete(INLINE_ITEM_META_PREFIX + item.getId());
                    } catch (Exception ignore) {
                    }
                }
            }

            try {
                redis.delete(INLINE_META_PREFIX + inlineMessageId);
            } catch (Exception ignore) {
            }

        } catch (Exception e) {
            log.error("[INLINE-UPDATE] 标记失效失败 inlineMessageId={}", inlineMessageId, e);
        }
    }

    // ==========================================================
    // 生产者
    // ==========================================================
    /** 更新整个母版的全部实例 */
    public void publishInlineUpdate(String inlineId, String content, String buttontext) {
        if (StringUtils.isBlank(inlineId)) {
            log.warn("[INLINE-UPDATE] publishInlineUpdate 跳过，inlineId 为空");
            return;
        }
        publish(Map.of(
                "inlineId", inlineId,
                "content", StringUtils.defaultString(content),
                "buttontext", StringUtils.defaultString(buttontext),
                "opTime", Utils.getCurrentDateTimeForyyyyMMddHHmmss()));
    }

    /** 只更新一个实例 */
    public void publishInlineUpdateByItemId(Long itemId, String content, String buttontext) {
        if (itemId == null) {
            log.warn("[INLINE-UPDATE] publishInlineUpdateByItemId 跳过，itemId 为空");
            return;
        }
        publish(Map.of(
                "itemId", String.valueOf(itemId),
                "content", StringUtils.defaultString(content),
                "buttontext", StringUtils.defaultString(buttontext),
                "opTime", Utils.getCurrentDateTimeForyyyyMMddHHmmss()));
    }

    private void publish(Map<String, String> msg) {
        try {
            redis.opsForStream().add(updateStream, msg);
        } catch (Exception e) {
            log.error("[INLINE-UPDATE] publish 失败 msg={}", msg, e);
        }
    }

    // ==========================================================
    // 工具
    // ==========================================================
    /**
     * 事件里的 buttontext 表达的三种意图。
     * <p>
     * 以前空串和「清空」共用同一种表达，而两者的正确行为正好相反：
     * Telegram 的 editMessage* 只要不带 {@code reply_markup} 就等于<b>把键盘删掉</b>，
     * 不是「保持原样」。所以「上游没提按钮」被执行成了「上游要求删掉按钮」——
     * 而且删键盘是一次合法编辑，返回 200，日志里记的是 success。
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
     * 那个方法对空串和 {@code []} 都返回 null，到了 markup 这一层两者已经分不出来了。
     */
    private MarkupDecision decideMarkup(String buttontext, String trace) {
        if (StringUtils.isBlank(buttontext)) {
            return new MarkupDecision(MarkupMode.KEEP, null);
        }

        String trimmed = buttontext.trim();
        if ("[]".equals(trimmed) || "[[]]".equals(trimmed)) {
            log.info("[INLINE-UPDATE][{}] 按钮显式清空 buttontext={}", trace, trimmed);
            return new MarkupDecision(MarkupMode.CLEAR, null);
        }

        InlineKeyboardMarkup markup;
        try {
            markup = KeyboardUtil.createUserKeyboard(buttontext);
        } catch (Exception e) {
            /*
             * 解析不了就当成「不改按钮」。原来是返回 null，等于静默降级成清空键盘——
             * 一个格式错误换来所有已分享消息的按钮全没，代价太大。
             */
            log.warn("[INLINE-UPDATE][{}] 按钮解析失败，本次不改按钮 err={}",
                    trace, safeMsg(e.getMessage()));
            return new MarkupDecision(MarkupMode.KEEP, null);
        }

        if (markup == null) {
            // 解析成功但没有任何有效按钮（比如全是空行），同样按不改处理
            log.warn("[INLINE-UPDATE][{}] 按钮串没有有效按钮，本次不改按钮", trace);
            return new MarkupDecision(MarkupMode.KEEP, null);
        }

        return new MarkupDecision(MarkupMode.SET, markup);
    }

    /** 只做解析，失败返回 null；给 KEEP 分支重建原键盘用 */
    private InlineKeyboardMarkup parseKeyboard(String buttontext, String trace) {
        if (StringUtils.isBlank(buttontext)) {
            return null;
        }
        try {
            return KeyboardUtil.createUserKeyboard(buttontext);
        } catch (Exception e) {
            log.warn("[INLINE-UPDATE][{}] 重建原键盘失败 err={}", trace, safeMsg(e.getMessage()));
            return null;
        }
    }

    private void writeItemMeta(CpBotmessageSendInlineItem item) {
        if (item == null || item.getId() == null) {
            return;
        }
        try {
            String key = INLINE_ITEM_META_PREFIX + item.getId();
            redis.opsForHash().put(key, "inlineId", StringUtils.defaultString(item.getInlineId()));
            redis.opsForHash().put(key, "inlineMessageId", StringUtils.defaultString(item.getInlineMessageId()));
            redis.opsForHash().put(key, "botcode", StringUtils.defaultString(item.getBotcode()));
            redis.expire(key, Duration.ofDays(90));
        } catch (Exception ignore) {
        }
    }

    private void acquireRateToken() {
        long deadline = System.currentTimeMillis() + RL_MAX_WAIT_MS;
        while (!limiter.allowOnceBurst(RL_GLOBAL_KEY, EDIT_RATE, EDIT_BURST)) {
            if (System.currentTimeMillis() >= deadline) {
                return;
            }
            if (!sleepQuietly(50)) {
                return;
            }
        }
    }

    private void ackAndDelete(MapRecord<String, Object, Object> r) {
        try {
            redis.opsForStream().acknowledge(updateStream, updateGroup, r.getId());
            try {
                redis.opsForStream().delete(updateStream, r.getId());
            } catch (Exception ignore) {
            }
        } catch (Exception e) {
            log.error("[INLINE-UPDATE] ack 失败 rid={}", r.getId(), e);
        }
    }

    private String val(MapRecord<String, Object, Object> r, String key) {
        Object v = r.getValue().get(key);
        return v == null ? null : String.valueOf(v);
    }

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

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }
}
