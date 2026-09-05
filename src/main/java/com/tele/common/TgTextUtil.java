package com.tele.common;

import org.apache.commons.lang3.StringUtils;

/**
 * 按 parse mode 准备发给 Telegram 的文案：截断 + 转义。发送与编辑共用同一份实现。
 * <p>
 * 转义逻辑原来只长在 {@code OutboundSender} 的私有静态方法里，
 * {@code MsgUpdateStreamWorker} 够不着，只能不转义——于是同一段文案发得出去、
 * 一编辑就 400。抽到这里就是为了让两条路径不可能再分叉。
 * <p>
 * <b>转义必须跟着 parse mode 走。</b>某行填了 {@code HTML} 却仍按 MarkdownV2 转义的话，
 * {@code & < >} 没转、而 {@code .} {@code -} 全被加了反斜杠，发出去是一段又乱又错的文本，
 * 而且不报错。parsemode 变成可配置的那一刻，这里就必须分派。
 * <p>
 * <b>只适用于系统生成、没有格式诉求的文案</b>（开奖播报、期号、金额）。
 * inline 母版那种人手写、可能带 {@code *粗体*} 的文案不能整段转义，
 * 那边沿用 qbbot 的约定：作者把模板写成合法 MarkdownV2，只对插值变量转义。
 */
public final class TgTextUtil {

    /** Telegram 图片 caption 上限 */
    public static final int TG_CAPTION_MAX = 1024;
    /** Telegram 文本消息上限 */
    public static final int TG_TEXT_MAX = 4096;

    /** parsemode 缺失时的兜底，与 {@code InlineQueryService.DEFAULT_PARSE_MODE} 同值 */
    public static final String DEFAULT_PARSE_MODE = "MarkdownV2";

    private TgTextUtil() {
    }

    /**
     * 归一 parse mode 的大小写。
     * <p>
     * Telegram 认的字面量是 {@code MarkdownV2} / {@code Markdown} / {@code HTML}，
     * 而库里的注释一度写成 {@code markdownV2}。照注释填会得到 400，
     * 所以这里按小写比对再返回官方写法，让人怎么填都能对。
     */
    public static String normalizeMode(String raw) {
        if (StringUtils.isBlank(raw)) {
            return DEFAULT_PARSE_MODE;
        }
        String v = raw.trim();
        if ("markdownv2".equalsIgnoreCase(v)) {
            return "MarkdownV2";
        }
        if ("markdown".equalsIgnoreCase(v)) {
            return "Markdown";
        }
        if ("html".equalsIgnoreCase(v)) {
            return "HTML";
        }
        // 认不出来的值原样透传：与其猜错，不如让 Telegram 报错，错误信息比静默降级有用
        return v;
    }

    /**
     * 统一换行、按上限截断、再按 parse mode 转义。
     * <p>
     * 截断必须在转义之前：先转义会让反斜杠参与长度计数，截断点还可能落在
     * {@code \} 和被转义字符中间，切出半截转义序列，Telegram 直接 400。
     */
    public static String normalizeAndEscape(String s, int maxLen, String parseMode) {
        if (s == null) {
            return "";
        }
        String t = s.replace("\r\n", "\n").replace("\r", "\n");

        if (t.length() > maxLen) {
            t = t.substring(0, Math.max(0, maxLen - 3)) + "...";
        }

        return escape(t, parseMode);
    }

    public static String escape(String text, String parseMode) {
        if (text == null || text.isEmpty()) {
            return "";
        }

        String mode = normalizeMode(parseMode);

        if ("HTML".equals(mode)) {
            return escapeHtml(text);
        }
        if ("MarkdownV2".equals(mode)) {
            return escapeMarkdownV2(text);
        }
        /*
         * legacy Markdown 没有可靠的转义方案——Telegram 的 legacy 解析器
         * 不认 \ 转义任意保留字符，硬加反斜杠反而会把字面反斜杠显示出来。
         * 所以这里原样返回，由文案作者保证 * _ ` [ 成对。
         * 认不出来的模式同理，原样返回让 Telegram 报错。
         */
        return text;
    }

    private static String escapeMarkdownV2(String text) {
        StringBuilder sb = new StringBuilder(text.length() * 2);
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '_':
                case '*':
                case '[':
                case ']':
                case '(':
                case ')':
                case '~':
                case '>':
                case '#':
                case '+':
                case '-':
                case '=':
                case '|':
                case '{':
                case '}':
                case '.':
                case '!':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    /** HTML 模式只需要转 {@code & < >}，且 {@code &} 必须第一个转，否则会把后面转出来的实体再转一遍 */
    private static String escapeHtml(String text) {
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }
}
