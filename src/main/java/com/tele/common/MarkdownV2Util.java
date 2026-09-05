package com.tele.common;

/**
 * MarkdownV2 文案处理，发送与编辑共用同一份实现。
 * <p>
 * 这段逻辑原来只长在 {@code OutboundSender} 里（私有静态方法），所以
 * {@code MsgUpdateStreamWorker} 编辑时用的是未转义的原文——**同一段文案发得出去、
 * 一编辑就 400**。抽到这里就是为了让两条路径不可能再分叉。
 * <p>
 * MarkdownV2 把 {@code _ * [ ] ( ) ~ > # + - = | { } . !} 全列为保留字符，
 * 而金额、时间、小数这些真实文案里几乎必然带 {@code .} 或 {@code -}。
 * <p>
 * <b>只适用于系统生成、没有格式诉求的文案</b>（开奖播报、期号、金额）。
 * inline 母版那种由人手写、可能带 {@code *粗体*} 的文案不能整段转义，
 * 那边沿用 qbbot 的约定：作者把模板写成合法 MarkdownV2，只对插值变量转义。
 */
public final class MarkdownV2Util {

    /** Telegram 图片 caption 上限 */
    public static final int TG_CAPTION_MAX = 1024;
    /** Telegram 文本消息上限 */
    public static final int TG_TEXT_MAX = 4096;

    private MarkdownV2Util() {
    }

    /**
     * 统一换行、按上限截断、再整段转义。
     * <p>
     * 截断必须在转义之前：先转义会让反斜杠参与计数，截断点还可能落在
     * {@code \} 和被转义字符中间，切出一个半截转义序列，Telegram 直接 400。
     */
    public static String normalizeAndEscape(String s, int maxLen) {
        if (s == null) {
            return "";
        }
        String t = s.replace("\r\n", "\n").replace("\r", "\n");

        if (t.length() > maxLen) {
            t = t.substring(0, Math.max(0, maxLen - 3)) + "...";
        }

        return escape(t);
    }

    public static String escape(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
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
}
