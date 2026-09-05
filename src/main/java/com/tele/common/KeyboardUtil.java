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
				|| StringUtils.isBlank(config.getType())) {
			return null;
		}

		String type = config.getType().trim();

		/*
		 * switch_inline 的 value 允许为空串：
		 * 空串表示「打开分享面板但不预填文本」，是 Telegram 的合法用法。
		 * 其余类型仍然必须有值。
		 */
		if (StringUtils.isBlank(config.getValue()) && !allowsBlankValue(type)) {
			return null;
		}

		InlineKeyboardButton button = InlineKeyboardButton.builder()
				.text(config.getText().trim())
				.build();

		String value = config.getValue() == null ? "" : config.getValue().trim();

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
			case "switch_inline":
				/*
				 * 让用户选一个会话，把消息分享过去。
				 * value 是预填进 inline 输入框的文本，
				 * 我们用它带上母版 id，这样 inline_query 才知道要回什么。
				 */
				button.setSwitchInlineQuery(value);
				break;
			case "switch_inline_current":
				// 同上，但只在当前会话里分享
				button.setSwitchInlineQueryCurrentChat(value);
				break;
			default: throw new IllegalArgumentException("不支持的按钮类型: " + type);
		}
		return button;
	}

	/**
	 * switch_inline 类型允许空 value（空串=不预填文本）。
	 */
	private static boolean allowsBlankValue(String type) {
		return "switch_inline".equals(type) || "switch_inline_current".equals(type);
	}

	public static InlineKeyboardMarkup emptyKeyboard() {
		return InlineKeyboardMarkup.builder()
				.keyboard(Collections.emptyList())
				.build();
	}
}

