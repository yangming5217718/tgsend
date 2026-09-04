package com.tele.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tele.callback.CallbackContext;
import com.tele.callback.CallbackRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.api.methods.AnswerCallbackQuery;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.meta.generics.TelegramClient;

@Service
public class CallbackQueryService {

    private static final Logger log = LoggerFactory.getLogger(CallbackQueryService.class);

    @Autowired
    private TelegramClient sender;

    @Autowired
    private CallbackRouter callbackRouter;

    private final ObjectMapper om = new ObjectMapper();

    public void handleCallbackQuery(String botcode, JsonNode cb) {
        String cqId = cb.path("id").asText(null);
        String data = cb.path("data").asText("");
        String chatId = cb.path("message").path("chat").path("id").asText(null);
        String fromId = cb.path("from").path("id").asText("");

        Integer originMsgId = cb.path("message").path("message_id").isMissingNode()
                ? null
                : cb.path("message").path("message_id").asInt();
        String originText = cb.path("message").path("text").asText("");

        if (log.isDebugEnabled()) {
            log.debug("Callback received botcode={}, cqId={}, fromId={}, chatId={}, data={}",
                    botcode, cqId, fromId, chatId, data);
        }

        if (cqId == null) {
            log.warn("Callback ignored because cqId is null. botcode={}, fromId={}, data={}",
                    botcode, fromId, data);
            return;
        }

        if ("noop".equalsIgnoreCase(data)) {
            AnswerCallbackQuery ack = AnswerCallbackQuery.builder()
                    .callbackQueryId(cqId)
                    .cacheTime(0)
                    .build();
            try {
                sender.execute(ack);
            } catch (TelegramApiException e) {
                log.warn("Callback noop ack failed. botcode={}, cqId={}", botcode, cqId, e);
            }
            return;
        }

        try {
            CallbackContext ctx = new CallbackContext();
            ctx.setBotcode(botcode);
            ctx.setCallbackId(cqId);
            ctx.setData(data);
            ctx.setChatId(chatId);
            ctx.setUserId(fromId);
            ctx.setMessageId(originMsgId);
            boolean handled = callbackRouter.route(ctx);
            if(handled){
                return;
            }
        } catch (Exception e) {
            log.error("Callback processing error. botcode={}, cqId={}, fromId={}, data={}",
                    botcode, cqId, fromId, data, e);
            answerAlert(botcode, cqId, "系统异常，请稍后再试");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Callback fell through to default alert. botcode={}, cqId={}, data={}",
                    botcode, cqId, data);
        }

        AnswerCallbackQuery ans = AnswerCallbackQuery.builder()
                .callbackQueryId(cqId)
                .text("点击" + data)
                .showAlert(true)
                .cacheTime(0)
                .build();
        try {
            sender.execute(ans);
        } catch (TelegramApiException e) {
            log.warn("Callback default answer failed. botcode={}, cqId={}, data={}",
                    botcode, cqId, data, e);
        }
    }

    private void answerAlert(String botcode, String callbackId, String text) {
        AnswerCallbackQuery ans = AnswerCallbackQuery.builder()
                .callbackQueryId(callbackId)
                .text(text)
                .showAlert(true)
                .cacheTime(0)
                .build();
        try {
            sender.execute(ans);
        } catch (TelegramApiException e) {
            log.warn("answerAlert failed. botcode={}, callbackId={}, text={}",
                    botcode, callbackId, text, e);
        }
    }
}