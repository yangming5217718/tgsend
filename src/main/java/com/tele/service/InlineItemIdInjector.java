package com.tele.service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 往分享按钮的 startapp 里注入 inline 实例 id。
 * <p>
 * 分享出去的每条 inline 消息都对应 {@code cp_botmessage_send_inline_item} 的一行，
 * 按钮里带上这行的 id，回调才能定位到具体是哪一次分享。
 * <p>
 * <b>只对前端真的会解析 itemId 的链接注入，其余一个字符都不改。</b>
 * 无差别注入会破坏别的参数——同一个位置可能是红包 packid、收款金额、
 * 券记录 uuid，追加或替换尾号都会把它们改坏。
 * <p>
 * 三条规则的变换语义各不相同，取决于前端怎么切这个串：
 * <ul>
 *   <li>红包：前端按<b>第一个</b> {@code _} 切成 packId / itemId，
 *       所以要替换第一个 {@code _} 之后的全部</li>
 *   <li>分享：前端取<b>最后一段</b>当 itemId，所以只替换最后一段</li>
 *   <li>邀请：前端匹配 {@code ^invite_(.+)$}，替换 {@code invite_} 之后的全部</li>
 * </ul>
 * 三条都是替换而非叠加，重复注入幂等。
 * <p>
 * 白名单可配置，默认值取自现有钱包业务。<b>接入新的 Mini App 时，
 * 必须先去看前端怎么解析 startapp，再往这里加</b>，不要凭直觉加。
 */
@Component
public class InlineItemIdInjector {

    private static final Logger log = LoggerFactory.getLogger(InlineItemIdInjector.class);

    private static final ObjectMapper OM = new ObjectMapper();

    /** 红包这类 startapp 是裸 id、没有前缀可认，只能靠 Mini App 短名判断 */
    @Value("${app.inline.itemid.redpacket-apps:hbpic,redpackage}")
    private String redpacketAppsRaw;

    /** 分享类 startapp 的前缀，取最后一段当 itemId */
    @Value("${app.inline.itemid.share-prefixes:routertosharecoupon,sharecoupon}")
    private String sharePrefixesRaw;

    /** 邀请绑定的 startapp 前缀 */
    @Value("${app.inline.itemid.invite-prefix:invite}")
    private String invitePrefix;

    private Set<String> redpacketApps() {
        return splitLower(redpacketAppsRaw);
    }

    private List<String> sharePrefixes() {
        return splitLower(sharePrefixesRaw).stream().toList();
    }

    private Set<String> splitLower(String raw) {
        if (StringUtils.isBlank(raw)) {
            return Set.of();
        }
        return java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(String::toLowerCase)
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
    }

    /**
     * 处理整串 buttontext。
     * <p>
     * 本项目的 buttontext 是 {@code [[{"text","type","value"}]]} 这种二维数组。
     * 没有任何按钮命中白名单时<b>原样返回</b>，不重新序列化——
     * 重新序列化会丢掉原串的格式，而绝大多数 buttontext 一个按钮都不会被改。
     */
    public String inject(String buttontext, Long itemId) {
        if (StringUtils.isBlank(buttontext) || itemId == null) {
            return buttontext;
        }

        try {
            JsonNode root = OM.readTree(buttontext);
            if (!root.isArray()) {
                return buttontext;
            }

            boolean changed = false;

            for (JsonNode row : root) {
                if (!row.isArray()) {
                    continue;
                }
                for (JsonNode btn : row) {
                    if (!(btn instanceof ObjectNode btnObj)) {
                        continue;
                    }

                    String type = StringUtils.trimToEmpty(btnObj.path("type").asText("")).toLowerCase();
                    if (!"url".equals(type) && !"webapp".equals(type) && !"web_app".equals(type)) {
                        continue;
                    }

                    String value = btnObj.path("value").asText("");
                    if (StringUtils.isBlank(value)) {
                        continue;
                    }

                    String newValue = injectIntoStartApp(value, itemId);
                    if (!newValue.equals(value)) {
                        btnObj.put("value", newValue);
                        changed = true;
                    }
                }
            }

            if (!changed) {
                return buttontext;
            }

            String result = root.toString();
            log.info("[INLINE-INJECT] itemId={} old={} new={}", itemId, buttontext, result);
            return result;

        } catch (Exception e) {
            log.warn("[INLINE-INJECT] 解析 buttontext 失败，原样返回 itemId={} err={}",
                    itemId, safeMsg(e.getMessage()));
            return buttontext;
        }
    }

    /**
     * 处理单个 URL。不属于白名单任何一类就原样返回。
     */
    public String injectIntoStartApp(String url, Long itemId) {
        if (StringUtils.isBlank(url) || itemId == null) {
            return url;
        }

        try {
            String anchor = "";
            String base = url;

            int hashIdx = url.indexOf('#');
            if (hashIdx >= 0) {
                base = url.substring(0, hashIdx);
                anchor = url.substring(hashIdx);
            }

            final String mark = "startapp=";
            int idx = base.indexOf(mark);
            if (idx < 0) {
                /*
                 * 没有 startapp 的链接一律不动。
                 * 尤其不要退化成追加 ?itemId=——没有任何前端读这个查询参数，
                 * 只会污染 https://t.me/xxx 这类静态入口。
                 */
                return url;
            }

            int valueStart = idx + mark.length();
            int nextAmp = base.indexOf('&', valueStart);

            String prefix = base.substring(0, valueStart);
            String suffix = "";
            String startappValue;

            if (nextAmp >= 0) {
                startappValue = base.substring(valueStart, nextAmp);
                suffix = base.substring(nextAmp);
            } else {
                startappValue = base.substring(valueStart);
            }

            String newValue = applyRule(base, startappValue, itemId);
            if (newValue == null || newValue.equals(startappValue)) {
                return url;
            }

            return prefix + newValue + suffix + anchor;

        } catch (Exception e) {
            log.warn("[INLINE-INJECT] 处理 URL 失败，原样返回 url={} err={}", url, safeMsg(e.getMessage()));
            return url;
        }
    }

    /** 返回 null 表示不属于任何一类 */
    private String applyRule(String urlBase, String startappValue, Long itemId) {
        String value = StringUtils.trimToEmpty(startappValue);
        if (value.isEmpty()) {
            return null;
        }
        String lower = value.toLowerCase();

        // 1) 分享类：前端取最后一段
        for (String p : sharePrefixes()) {
            if (lower.startsWith(p)) {
                int prefixSep = value.indexOf('_');
                int lastSep = value.lastIndexOf('_');
                if (lastSep <= prefixSep) {
                    return value + "_" + itemId;
                }
                return value.substring(0, lastSep + 1) + itemId;
            }
        }

        // 2) 邀请：前端匹配 ^invite_(.+)$
        String invite = StringUtils.trimToEmpty(invitePrefix).toLowerCase();
        if (StringUtils.isNotBlank(invite)) {
            if (lower.equals(invite)) {
                return value + "_" + itemId;
            }
            if (lower.startsWith(invite + "_")) {
                return value.substring(0, invite.length() + 1) + itemId;
            }
        }

        // 3) 红包：前端按第一个 _ 切
        if (isRedpacketMiniApp(urlBase)) {
            int sep = value.indexOf('_');
            if (sep < 0) {
                return value + "_" + itemId;
            }
            return value.substring(0, sep + 1) + itemId;
        }

        return null;
    }

    /** 形如 https://t.me/{bot}/{appName}?startapp=...，取 path 最后一段比对 */
    private boolean isRedpacketMiniApp(String urlBase) {
        if (StringUtils.isBlank(urlBase)) {
            return false;
        }

        String path = urlBase;

        int q = path.indexOf('?');
        if (q >= 0) {
            path = path.substring(0, q);
        }

        int scheme = path.indexOf("://");
        if (scheme >= 0) {
            path = path.substring(scheme + 3);
        }

        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        int lastSlash = path.lastIndexOf('/');
        if (lastSlash < 0) {
            return false;
        }

        return redpacketApps().contains(path.substring(lastSlash + 1).trim().toLowerCase());
    }

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }
}
