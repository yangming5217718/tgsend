package com.tele.mybots;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.tele.entity.CpConfig;
import com.tele.mapper.CpConfigMapper;

/**
 * bot token 的唯一来源：{@code cp_config} 表里 code = {@code bot.config.token-code}
 * 的那一行（默认 {@code bot_token}）。
 * <p>
 * 本项目只有一个 bot，所以只有一行、一个 key。换 bot 只改这行数据，
 * 既不用改代码也不用重新打包。
 * <p>
 * 以前 token 写在 application.yml 里，等于把凭据固化进了制品；
 * 而库里那行 {@code bot_token} 没有任何代码读，是个摆设。现在统一到库。
 */
@Component
public class BotTokenSource {

    private static final Logger log = LoggerFactory.getLogger(BotTokenSource.class);

    private final CpConfigMapper cpConfigMapper;

    @Value("${bot.config.token-code:bot_token}")
    private String tokenCode;

    private volatile String cached;

    public BotTokenSource(CpConfigMapper cpConfigMapper) {
        this.cpConfigMapper = cpConfigMapper;
    }

    /**
     * 取 token。取不到直接抛——没有 token 这个服务干不了任何事，
     * 与其带着半残状态启动，不如启动就失败，让人一眼看见。
     */
    public String token() {
        String t = cached;
        if (t != null) {
            return t;
        }
        synchronized (this) {
            if (cached != null) {
                return cached;
            }
            CpConfig cfg = cpConfigMapper.selectConfig(tokenCode);
            if (cfg == null || StringUtils.isBlank(cfg.getValue())) {
                throw new IllegalStateException(
                        "cp_config 里没有可用的 bot token，code=" + tokenCode + "（要求 status=1 且 value 非空）");
            }
            cached = cfg.getValue().trim();
            log.info("[BOT-TOKEN] 已从 cp_config 载入 code={} botId={}", tokenCode, botId());
            return cached;
        }
    }

    /** bot_id 是 token 冒号前那段。日志里只记它，不记 token。 */
    public String botId() {
        String t = cached;
        return t == null ? "?" : t.split(":")[0];
    }

    public String tokenCode() {
        return tokenCode;
    }
}
