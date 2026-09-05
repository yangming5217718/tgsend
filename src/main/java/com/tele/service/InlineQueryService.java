package com.tele.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.api.methods.AnswerInlineQuery;
import org.telegram.telegrambots.meta.api.objects.inlinequery.inputmessagecontent.InputTextMessageContent;
import org.telegram.telegrambots.meta.api.objects.inlinequery.result.InlineQueryResult;
import org.telegram.telegrambots.meta.api.objects.inlinequery.result.InlineQueryResultArticle;
import org.telegram.telegrambots.meta.api.objects.inlinequery.result.InlineQueryResultPhoto;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.tele.common.KeyboardUtil;
import com.tele.common.Utils;
import com.tele.entity.CpBotmessageSendInline;
import com.tele.entity.CpBotmessageSendInlineItem;
import com.tele.mapper.CpBotmessageSendInlineItemMapper;
import com.tele.mapper.CpBotmessageSendInlineMapper;
import com.tele.mybots.router.TelegramFacade;

/**
 * inline 分享的接收侧。
 * <p>
 * 两个入口：
 * <ul>
 *   <li>{@code inline_query} —— 用户在输入框打了 @bot xxx，要回一批候选结果。
 *       <b>这一步就先把实例行建出来</b>，因为按钮里要带上 itemId，
 *       等用户选中再建就来不及了。</li>
 *   <li>{@code chosen_inline_result} —— 用户选中并发出去了，这时才拿得到
 *       inline_message_id，回填到预创建的那一行并转为有效。</li>
 * </ul>
 * inline 消息没有 chat_id，inline_message_id 是唯一能定位它的东西，丢了就再也改不回来。
 */
@Service
public class InlineQueryService {

    private static final Logger log = LoggerFactory.getLogger(InlineQueryService.class);

    /** 母版 id -> 该母版所有实例的 inline_message_id */
    private static final String INLINE_SENT_SET_PREFIX = "inline:sent:";
    /** inline_message_id -> 元数据 */
    private static final String INLINE_META_PREFIX = "inline:meta:";
    /** itemId -> 元数据 */
    private static final String INLINE_ITEM_META_PREFIX = "inline:item:";
    /** (fromId, inlineId) -> 预创建的 itemId */
    private static final String INLINE_PRE_MAP_PREFIX = "inline:pre:";
    /** 同一用户同一查询短时间内重复触发的去重键 */
    private static final String INLINE_PRE_DEDUPE_PREFIX = "inline:pre:dedupe:";
    private static final String INLINE_PRE_LOCK_PREFIX = "inline:pre:lock:";

    private static final Duration INLINE_REDIS_TTL = Duration.ofDays(90);
    private static final Duration INLINE_PRE_DEDUPE_TTL = Duration.ofSeconds(5);
    private static final Duration INLINE_PRE_MAP_TTL = Duration.ofMinutes(10);

    private static final int TG_CAPTION_MAX = 1024;
    private static final int TG_TEXT_MAX = 4096;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private TelegramFacade tg;

    @Autowired
    private CpBotmessageSendInlineMapper inlineMapper;

    @Autowired
    private CpBotmessageSendInlineItemMapper itemMapper;

    @Autowired
    private InlineItemIdInjector itemIdInjector;

    @Value("${app.inline.default-title:分享}")
    private String defaultTitle;

    // ==========================================================
    // inline_query：用户正在输入
    // ==========================================================
    public void handleInlineQuery(JsonNode inlineQuery, String trace) {
        String inlineQueryId = inlineQuery.path("id").asText(null);
        if (StringUtils.isBlank(inlineQueryId)) {
            log.warn("[INLINE][{}] INLINE_QUERY 缺少 query id", trace);
            return;
        }

        String query = StringUtils.trimToEmpty(inlineQuery.path("query").asText(""));
        String fromId = inlineQuery.path("from").path("id").asText("");
        String chatType = inlineQuery.path("chat_type").asText("");

        log.info("[INLINE][{}] INLINE_QUERY in fromId={} chatType={} query={}",
                trace, fromId, chatType, query);

        try {
            CpBotmessageSendInline tpl = findTemplate(query, trace);
            if (tpl == null) {
                answerEmpty(inlineQueryId, trace);
                return;
            }

            /*
             * 预创建实例：必须在构造按钮之前，按钮里要带这个 itemId。
             */
            Long itemId = preCreateInlineItem(tpl.getId(), fromId, query, 0, trace);

            String buttontext = tpl.getButtontext();
            if (itemId != null && StringUtils.isNotBlank(buttontext)) {
                buttontext = itemIdInjector.inject(buttontext, itemId);
            }

            InlineKeyboardMarkup markup = null;
            if (StringUtils.isNotBlank(buttontext)) {
                try {
                    markup = KeyboardUtil.createUserKeyboard(buttontext);
                } catch (Exception e) {
                    log.warn("[INLINE][{}] 母版按钮解析失败 id={} err={}",
                            trace, tpl.getId(), safeMsg(e.getMessage()));
                }
            }

            /*
             * Telegram 只在结果带 inline keyboard 时才回 inline_message_id。
             * 没有按钮 = 这条分享出去的消息永远编辑不了。
             */
            if (markup == null) {
                log.warn("[INLINE][{}] 母版没有可用按钮，分享出去的消息将无法编辑 id={}", trace, tpl.getId());
            }

            InlineQueryResult result = buildResult(tpl, markup);

            AnswerInlineQuery answer = AnswerInlineQuery.builder()
                    .inlineQueryId(inlineQueryId)
                    .results(List.of(result))
                    /*
                     * cacheTime=0 + isPersonal=true：
                     * 母版内容随时会被后台改，而且每个用户拿到的按钮带各自的 itemId，
                     * 一旦被 Telegram 缓存就会串号。
                     */
                    .cacheTime(0)
                    .isPersonal(true)
                    .build();

            tg.execute(trace, answer);

            log.info("[INLINE][{}] INLINE_QUERY answered id={} itemId={}", trace, tpl.getId(), itemId);

        } catch (Exception e) {
            log.error("[INLINE][{}] INLINE_QUERY 处理失败 query={}", trace, query, e);
        }
    }

    /**
     * 母版定位：先按主键，再按 transferid。
     * <p>
     * 分享按钮用 switch_inline_query 预填其中之一，用户不需要自己打字。
     */
    private CpBotmessageSendInline findTemplate(String query, String trace) {
        if (StringUtils.isBlank(query)) {
            log.info("[INLINE][{}] query 为空，不返回候选", trace);
            return null;
        }

        CpBotmessageSendInline tpl = null;
        try {
            tpl = inlineMapper.selectById(query);
        } catch (Exception e) {
            log.warn("[INLINE][{}] 按 id 查母版失败 query={} err={}", trace, query, safeMsg(e.getMessage()));
        }

        if (tpl == null) {
            try {
                tpl = inlineMapper.selectOne(new QueryWrapper<CpBotmessageSendInline>()
                        .eq("transferid", query)
                        .last("limit 1"));
            } catch (Exception e) {
                log.warn("[INLINE][{}] 按 transferid 查母版失败 query={} err={}",
                        trace, query, safeMsg(e.getMessage()));
            }
        }

        if (tpl == null) {
            log.info("[INLINE][{}] 母版不存在 query={}", trace, query);
            return null;
        }
        if (tpl.getStatus() == -1) {
            log.info("[INLINE][{}] 母版已失效 id={} status={}", trace, tpl.getId(), tpl.getStatus());
            return null;
        }
        return tpl;
    }

    private InlineQueryResult buildResult(CpBotmessageSendInline tpl, InlineKeyboardMarkup markup) {
        /*
         * 母版内容是后台配置好的固定文案，parsemode 由母版自己声明，
         * 这里不做转义——转义会把作者写的格式全部打平。
         */
        String parseMode = StringUtils.isBlank(tpl.getParsemode()) ? "markdown" : tpl.getParsemode().trim();
        String title = buildTitle(tpl);

        if (StringUtils.isNotBlank(tpl.getImgsrc())) {
            return InlineQueryResultPhoto.builder()
                    .id(tpl.getId())
                    .photoUrl(tpl.getImgsrc())
                    .thumbnailUrl(tpl.getImgsrc())
                    .title(title)
                    .caption(clip(tpl.getContent(), TG_CAPTION_MAX))
                    .parseMode(parseMode)
                    .replyMarkup(markup)
                    .build();
        }

        InputTextMessageContent content = InputTextMessageContent.builder()
                .messageText(clip(tpl.getContent(), TG_TEXT_MAX))
                .parseMode(parseMode)
                .disableWebPagePreview(true)
                .build();

        return InlineQueryResultArticle.builder()
                .id(tpl.getId())
                .title(title)
                .inputMessageContent(content)
                .replyMarkup(markup)
                .build();
    }

    private void answerEmpty(String inlineQueryId, String trace) {
        try {
            tg.execute(trace, AnswerInlineQuery.builder()
                    .inlineQueryId(inlineQueryId)
                    .results(new ArrayList<>())
                    .cacheTime(1)
                    .isPersonal(true)
                    .build());
        } catch (Exception e) {
            log.warn("[INLINE][{}] 回空结果失败 err={}", trace, safeMsg(e.getMessage()));
        }
    }

    // ==========================================================
    // chosen_inline_result：用户已经把消息发出去了
    // ==========================================================
    public void handleChosenInlineResult(JsonNode chosen, String trace) {
        String resultId = chosen.path("result_id").asText("");
        String inlineMessageId = chosen.path("inline_message_id").asText("");
        String fromId = chosen.path("from").path("id").asText("");
        String queryText = chosen.path("query").asText("");
        String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();

        log.info("[INLINE][{}] CHOSEN in resultId={} fromId={} inlineMessageId={}",
                trace, resultId, fromId, inlineMessageId);

        if (StringUtils.isBlank(resultId)) {
            log.warn("[INLINE][{}] CHOSEN 缺少 result_id，丢弃", trace);
            return;
        }

        /*
         * 没有 inline_message_id 说明结果没带 inline keyboard，这种实例永远编辑不了。
         * 预创建的那一行留在 status=0，由后续清理处理，不转成有效。
         */
        if (StringUtils.isBlank(inlineMessageId)) {
            log.warn("[INLINE][{}] CHOSEN 没有 inline_message_id（母版缺按钮），该实例不可编辑 resultId={}",
                    trace, resultId);
            return;
        }

        try {
            CpBotmessageSendInline tpl = inlineMapper.selectById(resultId);
            if (tpl == null) {
                log.warn("[INLINE][{}] CHOSEN 母版不存在 resultId={}", trace, resultId);
                return;
            }

            CpBotmessageSendInlineItem item = findPreCreatedItem(resultId, fromId, trace);

            if (item == null) {
                // 预创建那条找不着了（Redis 掉了且库里也查不到），按 inline_message_id 再找一次
                item = itemMapper.selectOne(new QueryWrapper<CpBotmessageSendInlineItem>()
                        .eq("inline_message_id", inlineMessageId)
                        .last("limit 1"));
            }

            if (item == null) {
                item = new CpBotmessageSendInlineItem();
                item.setInlineId(resultId);
                item.setInlineMessageId(inlineMessageId);
                item.setFromId(StringUtils.defaultIfBlank(fromId, null));
                item.setQueryText(clip(queryText, 255));
                item.setStatus(1);
                item.setCreatetime(now);
                item.setUpdatetime(now);
                item.setSource(0);
                itemMapper.insert(item);
                log.info("[INLINE][{}] CHOSEN 新建实例 itemId={}", trace, item.getId());
            } else {
                item.setInlineId(resultId);
                item.setInlineMessageId(inlineMessageId);
                item.setFromId(StringUtils.defaultIfBlank(fromId, null));
                item.setQueryText(clip(queryText, 255));
                item.setStatus(1);
                item.setUpdatetime(now);
                itemMapper.updateById(item);
                log.info("[INLINE][{}] CHOSEN 回填预创建实例 itemId={}", trace, item.getId());
            }

            writeRedisIndex(resultId, inlineMessageId, item, now, trace);

            inlineMapper.update(null, new com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper<CpBotmessageSendInline>()
                    .eq("id", resultId)
                    .set("sendtime", now)
                    .set("status", 1));

        } catch (Exception e) {
            log.error("[INLINE][{}] CHOSEN 处理失败 resultId={}", trace, resultId, e);
        }
    }

    // ==========================================================
    // 预创建
    // ==========================================================
    private Long preCreateInlineItem(String inlineId, String fromId,
                                     String queryText, Integer source, String trace) {
        String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();
        int src = source == null ? 0 : source;
        String dedupeKey = INLINE_PRE_DEDUPE_PREFIX + StringUtils.defaultString(fromId)
                + ":" + StringUtils.defaultString(queryText) + ":" + src;
        String lockKey = INLINE_PRE_LOCK_PREFIX + dedupeKey;

        try {
            Long reused = reuseByDedupeKey(dedupeKey, trace);
            if (reused != null) {
                return reused;
            }

            Boolean locked = redis.opsForValue().setIfAbsent(lockKey, "1", INLINE_PRE_DEDUPE_TTL);
            if (Boolean.FALSE.equals(locked)) {
                /*
                 * 同一秒内客户端可能并发触发多次 inline_query（Android 尤其明显）。
                 * 不去重就会连续插好几条 status=0 的孤儿行。
                 * 这里短暂等前一个请求把 item 插完，复用它。
                 */
                for (int i = 0; i < 5; i++) {
                    try {
                        Thread.sleep(30L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    reused = reuseByDedupeKey(dedupeKey, trace);
                    if (reused != null) {
                        return reused;
                    }
                }

                CpBotmessageSendInlineItem old = findPreCreatedItem(inlineId, fromId, trace);
                if (old != null && old.getId() != null) {
                    return old.getId();
                }
            }

            CpBotmessageSendInlineItem item = new CpBotmessageSendInlineItem();
            item.setInlineId(inlineId);
            item.setFromId(StringUtils.defaultIfBlank(fromId, null));
            item.setQueryText(clip(queryText, 255));
            item.setSource(src);
            item.setStatus(0);
            item.setCreatetime(now);
            item.setUpdatetime(now);
            itemMapper.insert(item);

            Long itemId = item.getId();
            if (itemId != null) {
                try {
                    redis.opsForValue().set(INLINE_PRE_MAP_PREFIX + fromId + ":" + inlineId,
                            String.valueOf(itemId), INLINE_PRE_MAP_TTL);
                    redis.opsForValue().set(dedupeKey, String.valueOf(itemId), INLINE_PRE_DEDUPE_TTL);
                    writeItemMeta(item);
                } catch (Exception e) {
                    log.warn("[INLINE][{}] 预创建写 Redis 失败 itemId={} err={}",
                            trace, itemId, safeMsg(e.getMessage()));
                }
            }

            log.info("[INLINE][{}] 预创建实例 itemId={} inlineId={} fromId={}",
                    trace, itemId, inlineId, fromId);
            return itemId;

        } catch (Exception e) {
            log.error("[INLINE][{}] 预创建失败 inlineId={} fromId={}", trace, inlineId, fromId, e);
            return null;
        }
    }

    private Long reuseByDedupeKey(String dedupeKey, String trace) {
        try {
            String s = redis.opsForValue().get(dedupeKey);
            if (StringUtils.isBlank(s) || !StringUtils.isNumeric(s)) {
                return null;
            }
            CpBotmessageSendInlineItem item = itemMapper.selectById(Long.valueOf(s));
            if (item != null && item.getStatus() != null && item.getStatus() == 0) {
                log.info("[INLINE][{}] 复用预创建实例 itemId={}", trace, item.getId());
                return item.getId();
            }
        } catch (Exception e) {
            log.warn("[INLINE][{}] 去重键读取失败 err={}", trace, safeMsg(e.getMessage()));
        }
        return null;
    }

    private CpBotmessageSendInlineItem findPreCreatedItem(String inlineId, String fromId, String trace) {
        if (StringUtils.isBlank(inlineId) || StringUtils.isBlank(fromId)) {
            return null;
        }

        try {
            String s = redis.opsForValue().get(INLINE_PRE_MAP_PREFIX + fromId + ":" + inlineId);
            if (StringUtils.isNotBlank(s) && StringUtils.isNumeric(s)) {
                CpBotmessageSendInlineItem item = itemMapper.selectById(Long.valueOf(s));
                if (item != null && item.getStatus() != null && item.getStatus() == 0) {
                    return item;
                }
            }
        } catch (Exception e) {
            log.warn("[INLINE][{}] 预创建映射读取失败 err={}", trace, safeMsg(e.getMessage()));
        }

        try {
            return itemMapper.selectOne(new QueryWrapper<CpBotmessageSendInlineItem>()
                    .eq("inline_id", inlineId)
                    .eq("from_id", fromId)
                    .eq("status", 0)
                    .orderByDesc("id")
                    .last("limit 1"));
        } catch (Exception e) {
            log.warn("[INLINE][{}] 预创建回查失败 err={}", trace, safeMsg(e.getMessage()));
            return null;
        }
    }

    // ==========================================================
    // Redis 索引
    // ==========================================================
    private void writeRedisIndex(String inlineId, String inlineMessageId,
                                 CpBotmessageSendInlineItem item, String now, String trace) {
        try {
            String setKey = INLINE_SENT_SET_PREFIX + inlineId;
            redis.opsForSet().add(setKey, inlineMessageId);
            redis.expire(setKey, INLINE_REDIS_TTL);

            String metaKey = INLINE_META_PREFIX + inlineMessageId;
            redis.opsForHash().put(metaKey, "inlineId", inlineId);
            redis.opsForHash().put(metaKey, "botcode", StringUtils.defaultString(item.getBotcode()));
            redis.opsForHash().put(metaKey, "fromId", StringUtils.defaultString(item.getFromId()));
            redis.opsForHash().put(metaKey, "queryText", StringUtils.defaultString(item.getQueryText()));
            redis.opsForHash().put(metaKey, "createtime", now);
            redis.expire(metaKey, INLINE_REDIS_TTL);

            writeItemMeta(item);
        } catch (Exception e) {
            log.warn("[INLINE][{}] 写 Redis 索引失败 inlineMessageId={} err={}",
                    trace, inlineMessageId, safeMsg(e.getMessage()));
        }
    }

    private void writeItemMeta(CpBotmessageSendInlineItem item) {
        if (item == null || item.getId() == null) {
            return;
        }
        String key = INLINE_ITEM_META_PREFIX + item.getId();
        redis.opsForHash().put(key, "inlineId", StringUtils.defaultString(item.getInlineId()));
        redis.opsForHash().put(key, "inlineMessageId", StringUtils.defaultString(item.getInlineMessageId()));
        redis.opsForHash().put(key, "botcode", StringUtils.defaultString(item.getBotcode()));
        redis.opsForHash().put(key, "fromId", StringUtils.defaultString(item.getFromId()));
        redis.opsForHash().put(key, "queryText", StringUtils.defaultString(item.getQueryText()));
        redis.expire(key, INLINE_REDIS_TTL);
    }

    /** Redis 索引丢了可以从库里重建，正确性不依赖 Redis */
    public void rebuildInlineRedisIndex(String inlineId) {
        if (StringUtils.isBlank(inlineId)) {
            return;
        }
        try {
            List<CpBotmessageSendInlineItem> list = itemMapper.selectAliveByInlineId(inlineId);
            if (list == null || list.isEmpty()) {
                log.info("[INLINE] 重建索引跳过，无有效实例 inlineId={}", inlineId);
                return;
            }

            String setKey = INLINE_SENT_SET_PREFIX + inlineId;
            redis.delete(setKey);

            String now = Utils.getCurrentDateTimeForyyyyMMddHHmmss();
            for (CpBotmessageSendInlineItem item : list) {
                if (StringUtils.isBlank(item.getInlineMessageId())) {
                    continue;
                }
                writeRedisIndex(inlineId, item.getInlineMessageId(), item, now, "rebuild");
            }
            log.info("[INLINE] 重建索引完成 inlineId={} size={}", inlineId, list.size());
        } catch (Exception e) {
            log.error("[INLINE] 重建索引失败 inlineId={}", inlineId, e);
        }
    }

    /** 先读 Redis 再读库合并，Redis 掉了不影响正确性 */
    public Set<String> getInlineMessageIds(String inlineId) {
        Set<String> result = new java.util.LinkedHashSet<>();
        if (StringUtils.isBlank(inlineId)) {
            return result;
        }

        try {
            Set<String> members = redis.opsForSet().members(INLINE_SENT_SET_PREFIX + inlineId);
            if (members != null) {
                for (String m : members) {
                    if (StringUtils.isNotBlank(m)) {
                        result.add(m);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[INLINE] 读 Redis 索引失败 err={}", safeMsg(e.getMessage()));
        }

        try {
            List<CpBotmessageSendInlineItem> list = itemMapper.selectAliveByInlineId(inlineId);
            if (list != null) {
                for (CpBotmessageSendInlineItem item : list) {
                    if (StringUtils.isNotBlank(item.getInlineMessageId())) {
                        result.add(item.getInlineMessageId());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[INLINE] 读库索引失败 err={}", safeMsg(e.getMessage()));
        }

        return result;
    }

    // ==========================================================
    // 工具
    // ==========================================================
    private String buildTitle(CpBotmessageSendInline tpl) {
        String c = tpl.getContent();
        if (StringUtils.isBlank(c)) {
            return defaultTitle;
        }
        String firstLine = c.replace("\r\n", "\n").replace("\r", "\n").split("\n", 2)[0].trim();
        return firstLine.isEmpty() ? defaultTitle : clip(firstLine, 60);
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
