package com.tele.callback;

import lombok.Data;


@Data
public class CallbackContext {


    private String botcode;


    /**
     * Telegram callback id
     */
    private String callbackId;


    /**
     * 按钮数据
     */
    private String data;


    /**
     * 群ID
     */
    private String chatId;


    /**
     * 用户ID
     */
    private String userId;


    /**
     * 原消息ID
     */
    private Integer messageId;


}