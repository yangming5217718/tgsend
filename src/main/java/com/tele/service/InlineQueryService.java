package com.tele.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.api.methods.AnswerInlineQuery;
import org.telegram.telegrambots.meta.api.objects.inlinequery.inputmessagecontent.InputTextMessageContent;
import org.telegram.telegrambots.meta.api.objects.inlinequery.result.InlineQueryResult;
import org.telegram.telegrambots.meta.api.objects.inlinequery.result.InlineQueryResultArticle;
import org.telegram.telegrambots.meta.api.objects.inlinequery.result.InlineQueryResultPhoto;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.generics.TelegramClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.tele.common.KeyboardUtil;
import com.tele.common.Utils;
import com.tele.entity.CpBotmessageSendInline;
import com.tele.entity.CpBotmessageSendInlineItem;
import com.tele.mapper.CpBotmessageSendInlineItemMapper;
import com.tele.mapper.CpBotmessageSendInlineMapper;

/**
 * inline 分享的接收侧。
 * <p>
 * 两个入口：
 * <ul>
 *   <li>inline_query：用户在输入框打了 @bot xxx，要回一批候选结果</li>
 *   <li>chosen_inline_result：用户选中并发出去了，这时才拿得到 inline_message_id</li>
 * </ul>
 * inline_message_id 是后续编辑这条消息的唯一凭据，必须落库。
 */
@Service
public class InlineQueryService {

    private static final Logger log = LoggerFactory.getLogger(InlineQueryService.class);

    /** Telegram 对 caption 和正文的长度限制 */
    private static final int TG_CAPTION_MAX = 1024;
    private static final int TG_TEXT_MAX = 4096;

    /** answerInlineQuery 一次最多回多少条 */
    private static final int MAX_RESULTS = 50;

    private final CpBotmessageSendInlineMapper inlineMapper;
    private final CpBotmessageSendInlineItemMapper itemMapper;
    private final TelegramClient sender;

    @Value("${bot.config.bot_user_name:}")
    private String botUserName;

    @Autowired
    public InlineQueryService(CpBotmessageSendInlineMapper inlineMapper,
                              CpBotmessageSendInlineItemMapper itemMapper,
                              @Qualifier("telegramClient") TelegramClient sender) {
        this.inlineMapper = inlineMapper;
        this.itemMapper = itemMapper;
        this.sender = sender;
    }

    // ==========================================================
    // inline_query：用户正在输入
    // ==========================================================
    public void handleInlineQuery(JsonNode inlineQuery, String trace) {
        String queryId = inlineQuery.path("id").asText(null);
        if (queryId == null) {
            log.warn("[INLINE_QUERY] 缺少 query id trace={}", trace);
            return;
        }

        String fromId = inlineQuery.path("from").path("id").asText("");
        String query = inlineQuery.path("query").asText("").trim();

        log.info("[INLINE_QUERY] 收到 trace={} 用户={} 文本={}", trace, fromId, query);

        List<InlineQueryResult> results = new ArrayList<>();
        try {
            results = buildResults(query, trace);
        } catch (Exception e) {
            log.error("[INLINE_QUERY] 构造结果失败 trace={} 文本={}", trace, query, e);
        }

        /*
         * cacheTime=0 + isPersonal=true：
         * 母版内容随时可能被后台改，Telegram 不该缓存旧结果。
         */
        AnswerInlineQuery answer = AnswerInlineQuery.builder()
                .inlineQueryId(queryId)
                .results(results)
                .cacheTime(0)
                .isPersonal(true)
                .build();

        try {
            sender.execute(answer);
            log.info("[INLINE_QUERY] 已回应 trace={} 结果数={}", trace, results.size());
        } catch (Exception e) {
            log.warn("[INLINE_QUERY] 回应失败 trace={} queryId={} 错误={}",
                    trace, queryId, safeMsg(e.getMessage()));
        }
    }

    /**
     * 母版定位规则：inline 输入框里的文本就是 cp_botmessage_send_inline.id。
     * <p>
     * 分享按钮用 switch_inline_query 预填这个 id，用户不需要自己打字。
     * 换定位规则时只改这个方法。
     */
    private List<InlineQueryResult> buildResults(String query, String trace) {
        List<InlineQueryResult> results = new ArrayList<>();

        if (StringUtils.isBlank(query)) {
            log.info("[INLINE_QUERY] 文本为空，不返回候选 trace={}", trace);
            return results;
        }

        CpBotmessageSendInline tpl = inlineMapper.selectById(query);
        if (tpl == null) {
            log.info("[INLINE_QUERY] 母版不存在 trace={} id={}", trace, query);
            return results;
        }
        if (tpl.getStatus() == -1) {
            log.info("[INLINE_QUERY] 母版已失效 trace={} id={} status={}", trace, query, tpl.getStatus());
            return results;
        }

        InlineKeyboardMarkup markup = null;
        if (StringUtils.isNotBlank(tpl.getButtontext())) {
            try {
                markup = KeyboardUtil.createUserKeyboard(tpl.getButtontext());
            } catch (Exception e) {
                log.warn("[INLINE_QUERY] 母版按钮解析失败 trace={} id={} 错误={}",
                        trace, tpl.getId(), safeMsg(e.getMessage()));
            }
        }

        /*
         * 关键：Telegram 只在结果带 inline keyboard 时才回 inline_message_id。
         * 没有按钮 = 发出去以后永远编辑不了这条消息。
         */
        if (markup == null) {
            log.warn("[INLINE_QUERY] 母版没有可用按钮，分享出去的消息将无法编辑 trace={} id={}",
                    trace, tpl.getId());
        }

        /*
         * 母版内容是后台配置的固定文案，parsemode 由母版自己声明，
         * 这里不做转义——转义会把作者写的格式全部打平。
         */
        String parseMode = StringUtils.isBlank(tpl.getParsemode()) ? "markdown" : tpl.getParsemode().trim();
        String title = buildTitle(tpl);

        if (StringUtils.isNotBlank(tpl.getImgsrc())) {
            InlineQueryResultPhoto photo = InlineQueryResultPhoto.builder()
                    .id(tpl.getId())
                    .photoUrl(tpl.getImgsrc())
                    .thumbnailUrl(tpl.getImgsrc())
                    .title(title)
                    .caption(clip(tpl.getContent(), TG_CAPTION_MAX))
                    .parseMode(parseMode)
                    .replyMarkup(markup)
                    .build();
            results.add(photo);
        } else {
            InputTextMessageContent content = InputTextMessageContent.builder()
                    .messageText(clip(tpl.getContent(), TG_TEXT_MAX))
                    .parseMode(parseMode)
                    .disableWebPagePreview(true)
                    .build();

            InlineQueryResultArticle article = InlineQueryResultArticle.builder()
                    .id(tpl.getId())
                    .title(title)
                    .inputMessageContent(content)
                    .replyMarkup(markup)
                    .build();
            results.add(article);
        }

        if (results.size() > MAX_RESULTS) {
            return results.subList(0, MAX_RESULTS);
        }
        return results;
    }

    // ==========================================================
    // chosen_inline_result：用户已经把消息发出去了
    // ==========================================================
    public void handleChosenInlineResult(JsonNode chosen, String trace) {
        String resultId = chosen.path("result_id").asText(null);
        String inlineMessageId = chosen.path("inline_message_id").asText(null);
        String fromId = chosen.path("from").path("id").asText("");
        String query = chosen.path("query").asText("");

        log.info("[INLINE_CHOSEN] 收到 trace={} 母版={} 用户={} inline_message_id={}",
                trace, resultId, fromId, inlineMessageId);

        if (StringUtils.isBlank(resultId)) {
            log.warn("[INLINE_CHOSEN] 缺少 result_id，丢弃 trace={}", trace);
            return;
        }

        /*
         * 没有 inline_message_id 说明结果没带 inline keyboard。
         * 这种实例永远编辑不了，落库只会堆无用行，直接告警丢弃。
         */
        if (StringUtils.isBlank(inlineMessageId)) {
            log.warn("[INLINE_CHOSEN] 没有 inline_message_id（母版缺按钮），该实例不可编辑，不落库 trace={} 母版={}",
                    trace, resultId);
            return;
        }

        String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();

        CpBotmessageSendInlineItem item = new CpBotmessageSendInlineItem();
        item.setInlineId(resultId);
        item.setInlineMessageId(inlineMessageId);
        item.setBotcode(botUserName);
        item.setFromId(fromId);
        /*
         * chat_instance 只在 callback_query 里才有，
         * chosen_inline_result 不带，等这条消息上有人点按钮时再回填。
         */
        item.setChatInstance(null);
        item.setQueryText(clip(query, 255));
        item.setStatus(1);
        item.setCreatetime(now);
        item.setUpdatetime(now);
        item.setSource(0);

        try {
            int rows = itemMapper.insertIgnore(item);
            if (rows == 0) {
                // 唯一键挡住了，说明 Telegram 重投了同一条 chosen_inline_result
                log.info("[INLINE_CHOSEN] 实例已存在，忽略重复投递 trace={} inline_message_id={}",
                        trace, inlineMessageId);
            } else {
                log.info("[INLINE_CHOSEN] 实例已落库 trace={} 母版={} inline_message_id={}",
                        trace, resultId, inlineMessageId);
            }
        } catch (Exception e) {
            log.error("[INLINE_CHOSEN] 实例落库失败 trace={} 母版={} inline_message_id={}",
                    trace, resultId, inlineMessageId, e);
        }
    }

    // ==========================================================
    // 工具
    // ==========================================================
    private String buildTitle(CpBotmessageSendInline tpl) {
        String c = tpl.getContent();
        if (StringUtils.isBlank(c)) {
            return "分享";
        }
        String firstLine = c.replace("\r\n", "\n").replace("\r", "\n").split("\n", 2)[0].trim();
        if (firstLine.isEmpty()) {
            return "分享";
        }
        return clip(firstLine, 60);
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
