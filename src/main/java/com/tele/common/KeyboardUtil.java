package com.tele.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tele.dto.TelegramButtonConfig;
import org.apache.commons.lang3.StringUtils;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.meta.api.objects.replykeyboard.buttons.InlineKeyboardRow;

public class KeyboardUtil {

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static InlineKeyboardMarkup createUserKeyboard(String buttonJson) {
		if (StringUtils.isBlank(buttonJson)) {
			return null;
		}

		try {
			List<List<TelegramButtonConfig>> configRows = OBJECT_MAPPER.readValue(buttonJson, new TypeReference<>() {});

			List<InlineKeyboardRow> keyboard = new ArrayList<>();

			for (List<TelegramButtonConfig> configRow : configRows) {
				if (configRow == null || configRow.isEmpty()) {
					continue;
				}

				InlineKeyboardRow keyboardRow = new InlineKeyboardRow();

				for (TelegramButtonConfig config : configRow) {
					InlineKeyboardButton button = buildButton(config);

					if (button != null) {
						keyboardRow.add(button);
					}
				}

				if (!keyboardRow.isEmpty()) {
					keyboard.add(keyboardRow);
				}
			}

			if (keyboard.isEmpty()) {
				return null;
			}

			return InlineKeyboardMarkup.builder()
					.keyboard(keyboard)
					.build();

		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Telegram按钮JSON格式错误: " + e.getMessage(),
					e
			);
		}
	}

	private static InlineKeyboardButton buildButton(TelegramButtonConfig config) {
		if (config == null || StringUtils.isBlank(config.getText())
				|| StringUtils.isBlank(config.getType())
				|| StringUtils.isBlank(config.getValue())) {
			return null;
		}

		InlineKeyboardButton button = InlineKeyboardButton.builder()
				.text(config.getText().trim())
				.build();

		String type = config.getType().trim();
		String value = config.getValue().trim();

		switch (type) {
			case "url":
				if (!value.startsWith("http://") && !value.startsWith("https://") && !value.startsWith("tg://")) {
					throw new IllegalArgumentException("按钮URL格式错误: " + value);
				}
				button.setUrl(value);
				break;
			case "callback":
				/*
				 * Telegram callback_data 有长度限制，
				 * 建议这里只存短指令，不要存大段JSON。
				 */
				if (value.length() > 64) {
					throw new IllegalArgumentException("callback_data长度不能超过64字节: " + value);
				}
				button.setCallbackData(value);
				break;
			default: throw new IllegalArgumentException("不支持的按钮类型: " + type);
		}
		return button;
	}

	public static InlineKeyboardMarkup emptyKeyboard() {
		return InlineKeyboardMarkup.builder()
				.keyboard(Collections.emptyList())
				.build();
	}
}

