package com.tele.mybots;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageCaption;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageReplyMarkup;
import org.telegram.telegrambots.meta.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.meta.generics.TelegramClient;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.tele.common.KeyboardUtil;
import com.tele.common.RedisSlidingWindowRateLimiter;
import com.tele.common.Utils;
import com.tele.entity.CpBotmessageSendInline;
import com.tele.entity.CpBotmessageSendInlineItem;
import com.tele.mapper.CpBotmessageSendInlineItemMapper;
import com.tele.mapper.CpBotmessageSendInlineMapper;

/**
 * inline 分享消息的更新侧。
 * <p>
 * 一条母版可能被分享成 N 条实例，更新就是拿 inline_message_id 逐条 editMessage。
 * inline 消息没有 chat_id，只能靠 inline_message_id 定位，
 * 所以实例表里那一列丢了就再也改不回来。
 */
@Component
public class InlineMessageUpdater {

    private static final Logger log = LoggerFactory.getLogger(InlineMessageUpdater.class);

    private static final int TG_CAPTION_MAX = 1024;
    private static final int TG_TEXT_MAX = 4096;

    /*
     * 和 OutboundSender 共用同一个全局令牌桶 key：
     * 编辑和发送花的是同一个 bot 的 API 配额，
     * 分开限速等于把上限翻倍，照样会撞 429。
     */
    private static final String RL_GLOBAL_KEY = "tg:rl:g";
    private static final double EDIT_RATE = 20.0;
    private static final int EDIT_BURST = 30;

    private final CpBotmessageSendInlineMapper inlineMapper;
    private final CpBotmessageSendInlineItemMapper itemMapper;
    private final RedisSlidingWindowRateLimiter limiter;
    private final TelegramClient client;

    public InlineMessageUpdater(CpBotmessageSendInlineMapper inlineMapper,
                                CpBotmessageSendInlineItemMapper itemMapper,
                                RedisSlidingWindowRateLimiter limiter,
                                @Qualifier("telegramClient") TelegramClient client) {
        this.inlineMapper = inlineMapper;
        this.itemMapper = itemMapper;
        this.limiter = limiter;
        this.client = client;
    }

    /** 一次批量更新的结果统计 */
    public record UpdateResult(int total, int ok, int dead, int failed) {
        @Override
        public String toString() {
            return "总数=" + total + " 成功=" + ok + " 已失效=" + dead + " 失败=" + failed;
        }
    }

    /**
     * 按母版当前内容，刷新它所有还有效的实例。
     * <p>
     * 调用方先把新内容写进 cp_botmessage_send_inline，再调这里。
     */
    public UpdateResult refreshByInlineId(String inlineId, String trace) {
        CpBotmessageSendInline tpl = inlineMapper.selectById(inlineId);
        if (tpl == null) {
            log.warn("[INLINE_EDIT] 母版不存在 trace={} id={}", trace, inlineId);
            return new UpdateResult(0, 0, 0, 0);
        }

        List<CpBotmessageSendInlineItem> items = itemMapper.selectAliveByInlineId(inlineId);
        if (items.isEmpty()) {
            log.info("[INLINE_EDIT] 母版没有有效实例 trace={} id={}", trace, inlineId);
            return new UpdateResult(0, 0, 0, 0);
        }

        InlineKeyboardMarkup markup = null;
        if (StringUtils.isNotBlank(tpl.getButtontext())) {
            try {
                markup = KeyboardUtil.createUserKeyboard(tpl.getButtontext());
            } catch (Exception e) {
                log.warn("[INLINE_EDIT] 母版按钮解析失败 trace={} id={} 错误={}",
                        trace, inlineId, safeMsg(e.getMessage()));
            }
        }

        /*
         * 消息类型在分享那一刻就定死了：
         * 母版带图 -> 实例是图片消息 -> 只能改 caption；
         * 母版无图 -> 实例是文本消息 -> 只能改 text。
         * 拿母版当前的 imgsrc 判断，前提是母版不会中途从有图改成无图。
         */
        final boolean isPhoto = StringUtils.isNotBlank(tpl.getImgsrc());
        final String parseMode = StringUtils.isBlank(tpl.getParsemode()) ? "markdown" : tpl.getParsemode().trim();
        final String body = clip(tpl.getContent(), isPhoto ? TG_CAPTION_MAX : TG_TEXT_MAX);

        int ok = 0, dead = 0, failed = 0;

        log.info("[INLINE_EDIT] 开始更新 trace={} 母版={} 实例数={} 类型={}",
                trace, inlineId, items.size(), isPhoto ? "图片" : "文本");

        for (CpBotmessageSendInlineItem item : items) {
            String imid = item.getInlineMessageId();
            if (StringUtils.isBlank(imid)) {
                markDead(item, "inline_message_id 为空");
                dead++;
                continue;
            }

            // 撞不到令牌就等，编辑量通常不大，不值得为此丢更新
            while (!limiter.allowOnceBurst(RL_GLOBAL_KEY, EDIT_RATE, EDIT_BURST)) {
                if (!sleepQuietly(50)) {
                    break;
                }
            }

            switch (editOne(imid, body, parseMode, markup, isPhoto, trace)) {
                case OK -> ok++;
                case DEAD -> {
                    markDead(item, "Telegram 报消息不存在或无法编辑");
                    dead++;
                }
                case FAIL -> failed++;
            }
        }

        UpdateResult result = new UpdateResult(items.size(), ok, dead, failed);
        log.info("[INLINE_EDIT] 更新结束 trace={} 母版={} {}", trace, inlineId, result);
        return result;
    }

    /**
     * 只换按钮，不动正文。红包「已抢完」这类场景用这个，
     * 比重发整段文案便宜，也不会因为文案没变触发 message is not modified。
     */
    public UpdateResult refreshMarkupOnly(String inlineId, String buttontext, String trace) {
        List<CpBotmessageSendInlineItem> items = itemMapper.selectAliveByInlineId(inlineId);
        if (items.isEmpty()) {
            log.info("[INLINE_EDIT] 母版没有有效实例 trace={} id={}", trace, inlineId);
            return new UpdateResult(0, 0, 0, 0);
        }

        InlineKeyboardMarkup markup = null;
        if (StringUtils.isNotBlank(buttontext)) {
            try {
                markup = KeyboardUtil.createUserKeyboard(buttontext);
            } catch (Exception e) {
                log.warn("[INLINE_EDIT] 按钮解析失败 trace={} id={} 错误={}",
                        trace, inlineId, safeMsg(e.getMessage()));
                return new UpdateResult(items.size(), 0, 0, items.size());
            }
        }

        int ok = 0, dead = 0, failed = 0;
        for (CpBotmessageSendInlineItem item : items) {
            String imid = item.getInlineMessageId();
            if (StringUtils.isBlank(imid)) {
                markDead(item, "inline_message_id 为空");
                dead++;
                continue;
            }

            while (!limiter.allowOnceBurst(RL_GLOBAL_KEY, EDIT_RATE, EDIT_BURST)) {
                if (!sleepQuietly(50)) {
                    break;
                }
            }

            EditMessageReplyMarkup req = EditMessageReplyMarkup.builder()
                    .inlineMessageId(imid)
                    .replyMarkup(markup)
                    .build();

            switch (execute(req, imid, trace)) {
                case OK -> ok++;
                case DEAD -> {
                    markDead(item, "Telegram 报消息不存在或无法编辑");
                    dead++;
                }
                case FAIL -> failed++;
            }
        }

        UpdateResult result = new UpdateResult(items.size(), ok, dead, failed);
        log.info("[INLINE_EDIT] 按钮更新结束 trace={} 母版={} {}", trace, inlineId, result);
        return result;
    }

    // ==========================================================
    // 单条编辑
    // ==========================================================
    private enum Outcome { OK, DEAD, FAIL }

    private Outcome editOne(String inlineMessageId, String body, String parseMode,
                            InlineKeyboardMarkup markup, boolean isPhoto, String trace) {
        if (isPhoto) {
            EditMessageCaption req = EditMessageCaption.builder()
                    .inlineMessageId(inlineMessageId)
                    .caption(body)
                    .parseMode(parseMode)
                    .replyMarkup(markup)
                    .build();
            return execute(req, inlineMessageId, trace);
        }

        EditMessageText req = EditMessageText.builder()
                .inlineMessageId(inlineMessageId)
                .text(body)
                .parseMode(parseMode)
                .disableWebPagePreview(true)
                .replyMarkup(markup)
                .build();
        return execute(req, inlineMessageId, trace);
    }

    private Outcome execute(org.telegram.telegrambots.meta.api.methods.botapimethods.BotApiMethod<?> req,
                            String inlineMessageId, String trace) {
        long t0 = System.currentTimeMillis();
        try {
            client.execute(req);
            return Outcome.OK;
        } catch (TelegramApiRequestException e) {
            long cost = System.currentTimeMillis() - t0;
            String desc = safeMsg(e.getMessage());
            String low = desc.toLowerCase();

            /*
             * 内容一模一样时 Telegram 报 400 message is not modified。
             * 目标状态已经达成，算成功，不然重试永远不会好。
             */
            if (low.contains("message is not modified")) {
                return Outcome.OK;
            }

            if (isDeadMessage(low)) {
                log.warn("[INLINE_EDIT] 实例已失效 trace={} inline_message_id={} 错误={}",
                        trace, inlineMessageId, desc);
                return Outcome.DEAD;
            }

            log.warn("[INLINE_EDIT] 编辑失败 trace={} inline_message_id={} 错误码={} 耗时={}ms 错误={}",
                    trace, inlineMessageId, e.getErrorCode(), cost, desc);
            return Outcome.FAIL;

        } catch (Exception e) {
            long cost = System.currentTimeMillis() - t0;
            log.warn("[INLINE_EDIT] 编辑异常 trace={} inline_message_id={} 耗时={}ms 异常={} 错误={}",
                    trace, inlineMessageId, cost, e.getClass().getSimpleName(), safeMsg(e.getMessage()));
            return Outcome.FAIL;
        }
    }

    /**
     * 这些错误说明这条 inline 消息永远改不了了，
     * 继续重试只是白烧配额，直接把实例标失效。
     */
    private boolean isDeadMessage(String low) {
        return low.contains("message_id_invalid")
                || low.contains("message to edit not found")
                || low.contains("message can't be edited")
                || low.contains("inline message identifier is invalid")
                || low.contains("query is too old");
    }

    private void markDead(CpBotmessageSendInlineItem item, String reason) {
        try {
            UpdateWrapper<CpBotmessageSendInlineItem> uw = new UpdateWrapper<>();
            uw.eq("id", item.getId())
              .set("status", -1)
              .set("updatetime", Utils.getCurrentDateTimeForyyyyMMddHHmmss());
            itemMapper.update(null, uw);
            log.info("[INLINE_EDIT] 实例标记失效 id={} 原因={}", item.getId(), reason);
        } catch (Exception e) {
            log.warn("[INLINE_EDIT] 标记失效失败 id={} 错误={}", item.getId(), safeMsg(e.getMessage()));
        }
    }

    // ==========================================================
    // 工具
    // ==========================================================
    private boolean sleepQuietly(int ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static String clip(String s, int max) {
        if (s == null) return "";
        if (s.length() <= max) return s;
        return s.substring(0, Math.max(0, max - 3)) + "...";
    }

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }
}
