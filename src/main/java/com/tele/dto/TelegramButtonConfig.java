package com.tele.dto;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TelegramButtonConfig {
    private String text;

    /**
     * callback 或 url
     */
    private String type;

    /**
     * callback_data 或 URL
     */
    private String value;
}
