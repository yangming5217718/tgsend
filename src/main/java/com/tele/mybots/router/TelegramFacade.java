package com.tele.mybots.router;

import java.io.Serializable;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.botapimethods.BotApiMethod;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.meta.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.meta.generics.TelegramClient;

/**
 * 按 botcode 发 Telegram 请求的统一入口。
 * <p>
 * 只覆盖 {@link BotApiMethod} 这一类纯 JSON 请求（编辑消息、回应 inline 查询、
 * 发文本等）。带文件上传的 SendPhoto / SendAnimation 走的是 TelegramClient 的
 * 重载方法，仍由 OutboundSender 直接调用，不从这里过。
 */
@Component
public class TelegramFacade {

    private static final Logger log = LoggerFactory.getLogger(TelegramFacade.class);

    private final BotClientRouter router;

    public TelegramFacade(BotClientRouter router) {
        this.router = router;
    }

    public <T extends Serializable, M extends BotApiMethod<T>> T execute(String botcode, M method)
            throws TelegramApiException {
        return execute(newTrace(), botcode, method);
    }

    public <T extends Serializable, M extends BotApiMethod<T>> T execute(String trace, String botcode, M method)
            throws TelegramApiException {
        if (method == null) {
            return null;
        }

        TelegramClient client = router.resolve(botcode);
        String methodName = method.getClass().getSimpleName();
        long t0 = System.currentTimeMillis();

        try {
            T result = client.execute(method);
            log.debug("[TG] [{}] ok botcode={} method={} costMs={}",
                    trace, botcode, methodName, System.currentTimeMillis() - t0);
            return result;

        } catch (TelegramApiRequestException e) {
            log.warn("[TG] [{}] fail botcode={} method={} code={} costMs={} err={}",
                    trace, botcode, methodName, e.getErrorCode(),
                    System.currentTimeMillis() - t0, safeMsg(e.getMessage()));
            throw e;

        } catch (TelegramApiException e) {
            log.warn("[TG] [{}] fail botcode={} method={} costMs={} ex={} err={}",
                    trace, botcode, methodName, System.currentTimeMillis() - t0,
                    e.getClass().getSimpleName(), safeMsg(e.getMessage()));
            throw e;
        }
    }

    /** 入站消息辨认不出 bot 来源时用的兜底 botcode */
    public String defaultBotcode() {
        return router.defaultBotcode();
    }

    private static String newTrace() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }
}
