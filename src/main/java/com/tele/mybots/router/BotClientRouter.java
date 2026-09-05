package com.tele.mybots.router;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.generics.TelegramClient;

import jakarta.annotation.PostConstruct;

/**
 * botcode -&gt; TelegramClient 的路由。
 * <p>
 * 本项目只有配置里写死的两个 bot，所以这里只是一层薄壳：
 * 保留 {@code execute(botcode, req)} 的调用形状，让按 botcode 寻址的代码
 * 可以直接用；将来真要做多 bot 动态供给（比如从 Redis 取 token、
 * 走本地 Bot API server 或代理线路），只需要换掉这个类的实现，
 * 上层一行都不用动。
 * <p>
 * 可识别的 botcode：
 * <ul>
 *   <li>bot 的 username，例如 {@code lindarkBot}（不区分大小写，允许带 @）</li>
 *   <li>{@code bot_token} / {@code bot2_token} —— 配置键名，
 *       上游系统习惯直接把这两个字符串当 botcode 传</li>
 *   <li>空或无法识别 —— 回落到主 bot，并打一条 WARN</li>
 * </ul>
 */
@Component
public class BotClientRouter {

    private static final Logger log = LoggerFactory.getLogger(BotClientRouter.class);

    /** 配置键名形式的 botcode，上游会直接传这两个字符串 */
    private static final String CODE_BOT1 = "bot_token";
    private static final String CODE_BOT2 = "bot2_token";

    private final TelegramClient client1;
    private final TelegramClient client2;

    @Value("${bot.config.bot_user_name:}")
    private String botUserName1;

    @Value("${bot.config.bot2_user_name:}")
    private String botUserName2;

    /** key 一律小写，见 {@link #normalize(String)} */
    private final Map<String, TelegramClient> byCode = new LinkedHashMap<>();

    public BotClientRouter(@Qualifier("telegramClient") TelegramClient client1,
                           @Qualifier("telegramClient2") TelegramClient client2) {
        this.client1 = client1;
        this.client2 = client2;
    }

    @PostConstruct
    public void init() {
        byCode.put(CODE_BOT1, client1);
        byCode.put(CODE_BOT2, client2);

        if (StringUtils.isNotBlank(botUserName1)) {
            byCode.put(normalize(botUserName1), client1);
        }
        if (StringUtils.isNotBlank(botUserName2)) {
            byCode.put(normalize(botUserName2), client2);
        }

        log.info("[BOT-ROUTER] 已注册 botcode={} 主bot={} 备bot={}",
                byCode.keySet(),
                StringUtils.defaultIfBlank(botUserName1, "(未配置用户名)"),
                StringUtils.defaultIfBlank(botUserName2, "(未配置用户名)"));
    }

    /**
     * 取 botcode 对应的 client。
     * <p>
     * 认不出来时回落主 bot 而不是抛异常：这条链路上的调用方大多是
     * 后台批量推送，为一个 botcode 拼写问题把整批消息打死不划算。
     * 回落会打 WARN，靠日志暴露配置问题。
     */
    public TelegramClient resolve(String botcode) {
        String key = normalize(botcode);

        if (StringUtils.isBlank(key)) {
            log.warn("[BOT-ROUTER] botcode 为空，回落主 bot");
            return client1;
        }

        TelegramClient client = byCode.get(key);
        if (client != null) {
            return client;
        }

        log.warn("[BOT-ROUTER] 未知 botcode={}，回落主 bot。已注册={}", botcode, byCode.keySet());
        return client1;
    }

    /** 已注册的 botcode，只读，供诊断用 */
    public Set<String> knownCodes() {
        return byCode.keySet();
    }

    /** 主 bot 的 botcode，入站消息辨认不出来源时用它兜底 */
    public String defaultBotcode() {
        return StringUtils.isNotBlank(botUserName1) ? botUserName1 : CODE_BOT1;
    }

    private String normalize(String botcode) {
        if (botcode == null) {
            return "";
        }
        String s = botcode.trim();
        if (s.startsWith("@")) {
            s = s.substring(1);
        }
        return s.toLowerCase();
    }
}
