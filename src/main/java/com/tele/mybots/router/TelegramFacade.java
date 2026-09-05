package com.tele.mybots.router;

import java.io.Serializable;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.botapimethods.BotApiMethod;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.meta.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.meta.generics.TelegramClient;

/**
 * 发 Telegram 请求的统一入口。
 * <p>
 * 本项目只有一个 bot，所以这里不做 botcode 路由——没有主备之分，
 * 也不需要注册表。将来真要接多个 bot，把这个类换成按 botcode 查表取 token
 * 即可，调用方不用动。
 * <p>
 * 只覆盖 {@link BotApiMethod} 这类纯 JSON 请求（编辑消息、回应 inline 查询、
 * 发文本等）。带文件上传的 SendPhoto / SendAnimation 走 TelegramClient 的重载
 * 方法，仍由 OutboundSender 直接调用，不从这里过。
 */
@Component
public class TelegramFacade {

    private static final Logger log = LoggerFactory.getLogger(TelegramFacade.class);

    private final TelegramClient client;

    public TelegramFacade(@Qualifier("telegramClient") TelegramClient client) {
        this.client = client;
    }

    public <T extends Serializable, M extends BotApiMethod<T>> T execute(M method)
            throws TelegramApiException {
        return execute(newTrace(), method);
    }

    public <T extends Serializable, M extends BotApiMethod<T>> T execute(String trace, M method)
            throws TelegramApiException {
        if (method == null) {
            return null;
        }

        String methodName = method.getClass().getSimpleName();
        long t0 = System.currentTimeMillis();

        try {
            T result = client.execute(method);
            log.debug("[TG] [{}] ok method={} costMs={}",
                    trace, methodName, System.currentTimeMillis() - t0);
            return result;

        } catch (TelegramApiRequestException e) {
            log.warn("[TG] [{}] fail method={} code={} costMs={} err={}",
                    trace, methodName, e.getErrorCode(),
                    System.currentTimeMillis() - t0, safeMsg(e.getMessage()));
            throw e;

        } catch (TelegramApiException e) {
            log.warn("[TG] [{}] fail method={} costMs={} ex={} err={}",
                    trace, methodName, System.currentTimeMillis() - t0,
                    e.getClass().getSimpleName(), safeMsg(e.getMessage()));
            throw e;
        }
    }

    private static String newTrace() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static String safeMsg(String s) {
        return s == null ? "null" : s.replace("\n", " ").replace("\r", " ");
    }
}
