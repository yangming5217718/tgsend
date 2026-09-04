package com.tele.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.tele.common.KeyboardUtil;
import com.tele.entity.*;
import com.tele.mapper.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.api.methods.pinnedmessages.PinChatMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.methods.send.SendPhoto;
import org.telegram.telegrambots.meta.api.objects.InputFile;
import org.telegram.telegrambots.meta.api.objects.message.Message;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.meta.generics.TelegramClient;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import static com.tele.common.Utils.getCurrentDateTimeForyyyyMMddHHmmss;
import static com.tele.common.Utils.getCurrentDateTimePlusMinutes;

@Slf4j
@Service
public class MessageCommandService {

    private static final DateTimeFormatter LOG_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final StringRedisTemplate redis;
    private final TelegramClient sender;
    private final CpGameRoomMapper cpGameRoomMapper;
    private final CpIssueMapper cpIssueMapper;
    private final CpInstructionAllMapper cpInstructionAllMapper;
    private final CpInstructionUserMapper cpInstructionUserMapper;
    private final CpInstructionMapper cpInstructionMapper;

    private volatile List<CpInstruction> checkList = Collections.emptyList();

    public MessageCommandService(StringRedisTemplate redis, @Qualifier("telegramClient") TelegramClient sender,
                                 CpGameRoomMapper cpGameRoomMapper, CpIssueMapper cpIssueMapper,
                                 CpInstructionAllMapper cpInstructionAllMapper, CpInstructionUserMapper cpInstructionUserMapper,
                                 CpInstructionMapper cpInstructionMapper) {
        this.redis = redis;
        this.sender = sender;
        this.cpGameRoomMapper = cpGameRoomMapper;
        this.cpIssueMapper = cpIssueMapper;
        this.cpInstructionAllMapper = cpInstructionAllMapper;
        this.cpInstructionUserMapper = cpInstructionUserMapper;
        this.cpInstructionMapper = cpInstructionMapper;
        reloadRules();
    }

    public void executeMessage(JsonNode update, String trace){
        JsonNode msg = update.path("message");
        if (msg.isMissingNode()) {
            logW(trace, "no message field, update=" + jclip(update, 200));
            return;
        }

        String text = msg.path("text").asText(null);
        if (StringUtils.isBlank(text)) {
            logI(trace, "blank text -> skip msg=" + jclip(msg, 200));
            return;
        }

        JsonNode from = msg.path("from");
        JsonNode chat = msg.path("chat");

        String msgid = msg.path("message_id").asText("");
        String botid = from.path("id").asText("");
        String chatid = chat.path("id").asText("");
        String type = chat.path("type").asText("");
        String userCoding = from.has("username") ? from.path("username").asText("") : "";
        String userName = StringUtils.defaultString(from.path("first_name").asText())
                + StringUtils.defaultString(from.path("last_name").asText());
        boolean isPrivate = "private".equals(type);
        int fromtype = isPrivate ? 2 : 1;

        logI(trace, "message recv chatid=" + chatid + " fromId=" + botid
                + " type=" + type + " msgid=" + msgid
                + " text=" + ellipsis(text, 120));

        // 黑名单拦截
        try {
            Boolean inBlack = redis.opsForSet().isMember("user:blacklist", botid);
            if (Boolean.TRUE.equals(inBlack)) {
                logW(trace, "BLOCKED blacklist botid=" + botid + " chatid=" + chatid);
                return;
            }
        } catch (Exception e) {
            logW(trace, "blacklist check exception botid=" + botid + " err=" + e.getMessage());
        }

        // 500ms 内只处理一次
        try {
            String rateKey = "msg:rate:" + chatid + ":" + botid;
            Long incr = redis.opsForValue().increment(rateKey);
            if (incr != null && incr == 1L) {
                redis.expire(rateKey, Duration.ofMillis(500));
            } else if (incr != null && incr > 1L) {
                logW(trace, "rate-limit HIT botid=" + botid + " chatid=" + chatid
                        + " incr=" + incr + " text=" + ellipsis(text, 60));
                return;
            }
        } catch (Exception e) {
            logW(trace, "rate-limit check exception botid=" + botid + " err=" + e.getMessage());
        }

        if (text.length() > 200) {
            logW(trace, "text too long len=" + text.length() + " botid=" + botid
                    + " chatid=" + chatid + " text=" + ellipsis(text, 60));
            return;
        }

        // 2 秒内重复去重
        try {
            String msgKey = "msg:dedup:" + chatid + ":" + botid + ":" + text.hashCode();
            Boolean first = redis.opsForValue().setIfAbsent(msgKey, "1", Duration.ofSeconds(2));
            if (Boolean.FALSE.equals(first)) {
                logW(trace, "dedup HIT botid=" + botid + " chatid=" + chatid
                        + " text=" + ellipsis(text, 60));
                return;
            }
        } catch (Exception e) {
            logW(trace, "dedup check exception botid=" + botid + " err=" + e.getMessage());
        }

        //每个用户检测是否注册
        ensureInstructionUser(botid, userName, userCoding, fromtype, chatid, trace);

        if (isPrivate && text.startsWith("/start")) {
            String arg = "";
            try {
                String[] parts = text.trim().split("\\s+", 2);
                if (parts.length == 2) arg = parts[1].trim();
            } catch (Exception ignore) {}

            logI(trace, "start recv botid=" + botid + " chatid=" + chatid + " arg=" + arg);

            if (StringUtils.isBlank(arg)) {
                logW(trace, "私聊绑定参数为空 telegramUserId=" + botid);
                sendBotLinkError(chatid,"私聊机器人链接错误");
                return;
            }

            if (!arg.startsWith("room_")) {
                logW(trace, "私聊绑定参数非法 arg=" + arg);
                sendBotLinkError(chatid,"私聊机器人链接错误");
                return;
            }

            String roomId = arg.substring("room_".length());

            if (StringUtils.isBlank(roomId)) {
                logW(trace, "私聊绑定roomId为空 arg=" + arg + " telegramUserId=" + botid);
                sendBotLinkError(chatid,"私聊机器人链接错误");
                return;
            }
            /*
             * 如果用户是从某个房间的私聊下注按钮进来的，
             * 在这里绑定房间。
             */
            bindPrivateRoom(botid,chatid, arg, trace,roomId);

            showStartMenu(chatid, trace, userName, roomId);
            return;
        }

        // Telegram msg.date 秒级时间戳
        long ts = msg.path("date").asLong();
        LocalDateTime dt = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(ts),
                ZoneId.of("Asia/Singapore")
        );
        String createtime = dt.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        fixText(text, botid, chatid, fromtype, msgid, createtime, trace);
    }

    // ==========================
    // 下注指令解析
    // ==========================
    private void fixText(String text, String botid, String chatid, int fromtype, String msgid, String creattime, String trace) {
        try {
            CpGameRoom room;
            /*
             * fromtype:
             * 1 = 群聊
             * 2 = 私聊
             */
            if (fromtype == 1) {
                /*
                 * 群聊：
                 * 直接根据 Telegram 群ID找房间
                 */
                room = cpGameRoomMapper.selectByChatId(chatid);
            } else {
                /*
                 * 私聊：
                 * 根据用户之前点击“私聊下注”
                 * 绑定的 roomId 找房间。
                 */
                String roomId = redis.opsForValue().get("tg:private:room:" + botid);

                if (StringUtils.isBlank(roomId)) {
                    sendReplyMessage(
                            chatid,
                            msgid,
                            "请先从下注群点击“私聊下注”进入机器人"
                    );
                    return;
                }
                room = cpGameRoomMapper.selectById(roomId);
            }
            if (room == null) {
                logW(trace, "没有找到对应房间 chatId=" + chatid);
                sendReplyMessage(
                        chatid,
                        msgid,
                        "该群停止下注"
                );
                return;
            }
            String roomId = room.getId();
            String nowTime = getCurrentDateTimeForyyyyMMddHHmmss();
            for (CpInstruction cpInstruction : checkList) {
                if (!text.matches(cpInstruction.getPattern())) {
                    continue;
                }
                CpIssue cpIssue = null;
                if (Integer.valueOf(1).equals(cpInstruction.getInstructionType())) {
                    String issue = redis.opsForValue().get("cacheNextIssueKey:"+roomId);
                    cpIssue = cpIssueMapper.selectByIssueAndRoomId(roomId, issue);
                    if (cpIssue == null) {
                        sendReplyMessage(chatid, msgid, "已封盘");
                        return;
                    }
                }
                CpInstructionAll cpInstructionAll = new CpInstructionAll();
                cpInstructionAll.setGametype(cpInstruction.getGametype());
                cpInstructionAll.setTelegramUserId(botid);
                cpInstructionAll.setRoomId(roomId);
                cpInstructionAll.setInsname(cpInstruction.getInsname());
                cpInstructionAll.setInscontent(text);
                cpInstructionAll.setFromtype(fromtype);
                cpInstructionAll.setStatus(0);
                cpInstructionAll.setChatid(chatid);
                cpInstructionAll.setMsgid(msgid);
                cpInstructionAll.setCreatetime(creattime);
                cpInstructionAll.setAddtime(nowTime);
                if (cpIssue != null) {
                    cpInstructionAll.setIssue(cpIssue.getIssue());
                    cpInstructionAll.setExptime(cpIssue.getStatustime());
                }else {
                    cpInstructionAll.setExptime(getCurrentDateTimePlusMinutes(1));
                }
                cpInstructionAllMapper.insert(cpInstructionAll);
                return;
            }
        }catch (Exception e){
            logE(trace, "指令解析发生异常："
                            + " text=" + text
                            + " botid=" + botid
                            + " chatid=" + chatid
                            + " fromtype=" + fromtype
                            + " msgid=" + msgid
                            + " err=" + safeMsg(e.getMessage()),
                    e
            );
            try {
                sender.execute(SendMessage.builder()
                        .chatId(chatid)
                        .text("群异常，请稍后再试")
                        .disableWebPagePreview(true)
                        .build());
            } catch (Exception ignore) {}
        }
    }

    private void ensureInstructionUser(String telegramUserId, String userName, String userCoding,
                                       int fromtype, String chatid, String trace) {
        try {
            CpInstructionUser existsUser = cpInstructionUserMapper.selectByTelegramUserId(telegramUserId);

            if (existsUser == null) {
                CpInstructionUser user = new CpInstructionUser();
                user.setTelegramUserId(telegramUserId);
                user.setUserName(userName);
                user.setUserCoding(userCoding);
                user.setFromtype(fromtype);
                user.setCreatetime(getCurrentDateTimeForyyyyMMddHHmmss());
                user.setChatid(chatid);
                cpInstructionUserMapper.insert(user);

            } else {
                logI(trace, "用户已注册 telegramUserId=" + telegramUserId);
            }
        } catch (Exception e) {
            logE(trace, "飞机私聊注册用户异常 telegramUserId=" + telegramUserId + " chatid=" + chatid, e);
        }
    }

    private void bindPrivateRoom(String telegramUserId,String chatId, String arg, String trace,String roomId) {
        try {

            /*
             * 验证这个房间确实存在
             */
            CpGameRoom room = cpGameRoomMapper.selectById(roomId);

            if (room == null) {
                logW(trace, "私聊绑定房间不存在 roomId=" + roomId + " telegramUserId=" + telegramUserId);
                sendBotLinkError(chatId,"机器人链接错误或房间已停止");
                return;
            }
            /*
             * 用户 -> 房间
             */
            redis.opsForValue().set("tg:private:room:" + telegramUserId, roomId);
            logI(trace, "私聊房间绑定成功 telegramUserId=" + telegramUserId + " roomId=" + roomId);
        }catch (Exception e) {
            logE(trace, "私聊绑定房间异常 telegramUserId=" + telegramUserId + " arg=" + arg, e);
            sendBotLinkError(chatId,"机器人链接异常，请稍后再试");
        }
    }

    /**
     * /start 通知文本
     * @param chatId
     * @param trace
     */
    private void showStartMenu(String chatId, String trace, String userName, String roomId) {
        if (StringUtils.isBlank(chatId)) {
            logW(trace, "showStartMenu skip chatId blank");
            return;
        }

        if (StringUtils.isBlank(roomId)) {
            logW(trace, "showStartMenu roomId blank");
            sendStartFallback(chatId);
            return;
        }
        /*
         * 根据 /start room_xxx 获取房间
         */
        CpGameRoom room = cpGameRoomMapper.selectById(roomId);

        if (room == null) {
            logW(trace, "showStartMenu room empty roomId=" + roomId);
            sendStartFallback(chatId);
            return;
        }
        /*
         * 从 cp_game_room.start_config_text 获取欢迎文本
         */
        String content = room.getStartConfigText();

        if (StringUtils.isBlank(content)) {
            logW(trace, "showStartMenu startConfigText blank roomId=" + roomId);
            sendStartFallback(chatId);
            return;
        }
        String imgUrl = "";
        String replybtntext = "";
        /*
         * 当前私聊用户名称
         */
        String username = StringUtils.defaultString(userName);

        if (StringUtils.isBlank(username)) {
            username = "用户";
        }
        /*
         * 当前绑定的群信息
         */
        String roomName = StringUtils.defaultString(room.getRoomName());
        /*
         * 模板变量替换
         */
        content = content.replace("{username}", escapeMarkdownV2Text(username))
                .replace("{roomName}", escapeMarkdownV2Text(roomName));

        try {
            Message sent;
            if (StringUtils.isNotBlank(imgUrl)) {
                SendPhoto req = new SendPhoto(chatId, new InputFile(imgUrl));
                req.setCaption(content);
                req.setParseMode("MarkdownV2");
                if (StringUtils.isNotBlank(replybtntext)) {
                    req.setReplyMarkup(KeyboardUtil.createUserKeyboard(replybtntext));
                }
                sent = sender.execute(req);
            } else {
                SendMessage.SendMessageBuilder mb = SendMessage.builder()
                        .chatId(chatId)
                        .text(content)
                        .parseMode("MarkdownV2")
                        .disableWebPagePreview(true);
                if (StringUtils.isNotBlank(replybtntext)) {
                    mb.replyMarkup(KeyboardUtil.createUserKeyboard(replybtntext));
                }
                sent = sender.execute(mb.build());
            }
            if (sent != null) {
                try {
                    PinChatMessage pinReq = PinChatMessage.builder()
                            .chatId(chatId)
                            .messageId(sent.getMessageId())
                            .build();
                    sender.execute(pinReq);
                } catch (Exception e) {
                    logW(trace, "showStartMenu PIN FAIL ignore err=" + safeMsg(e.getMessage()));
                }
            }
        } catch (Exception e) {
            logE(trace, "showStartMenu send FAILED", e);
            try {
                sender.execute(SendMessage.builder()
                        .chatId(chatId)
                        .text("菜单暂不可用，请稍后再试")
                        .disableWebPagePreview(true)
                        .build());
            } catch (Exception ignore) {
            }
        }
    }

    private void sendBotLinkError(String chatId,String text) {
        try {
            sender.execute(SendMessage.builder()
                            .chatId(chatId)
                            .text(text)
                            .disableWebPagePreview(true)
                            .build());
        } catch (Exception ignore) {
        }
    }

    private static String escapeMarkdownV2Text(String text) {
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
                case '`':
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

    private void sendStartFallback(String chatId) {
        try {
            sender.execute(SendMessage.builder()
                    .chatId(chatId)
                    .text("菜单暂不可用，请稍后再试")
                    .disableWebPagePreview(true)
                    .build());
        } catch (Exception ignore) {
        }
    }

    private void sendReplyMessage(String chatId, String msgId, String text) throws TelegramApiException {
        SendMessage.SendMessageBuilder builder = SendMessage.builder()
                .chatId(chatId)
                .text(text)
                .disableWebPagePreview(true);
        try {
            if (StringUtils.isNotBlank(msgId)) {
                builder.replyToMessageId(Integer.parseInt(msgId));
            }
        } catch (Exception ignore) {
        }
        sender.execute(builder.build());
    }


    private void reloadRules() {
        try {
            List<CpInstruction> fresh = cpInstructionMapper.selectList(new QueryWrapper<CpInstruction>().eq("status", 1));
            int oldSize = checkList == null ? 0 : checkList.size();
            int newSize = fresh == null ? 0 : fresh.size();
            checkList = fresh == null ? Collections.emptyList() : fresh;
            log.info("【下注规则】规则刷新成功 原数量={} 新数量={} 原因={}", oldSize, newSize, "启动加载");
        } catch (Exception e) {
            log.error("【下注规则】规则刷新失败 原因={}", "启动加载", e);
        }
    }

    private void logW(String trace, String msg) {
        System.out.println(ts() + " [WARN] [" + trace + "] " + msg);
    }

    private String jclip(JsonNode node, int max) {
        if (node == null) return "null";
        try {
            String s = node.toString();
            return s.length() <= max ? s : s.substring(0, max) + "...";
        } catch (Exception e) {
            return "<?>";
        }
    }

    private void logI(String trace, String msg) {
        System.out.println(ts() + " [INFO] [" + trace + "] " + msg);
    }

    private static String ellipsis(String s, int limit) {
        if (s == null) return null;
        return s.length() <= limit ? s : s.substring(0, limit) + "...";
    }

    private static String ts() {
        return LocalDateTime.now().format(LOG_TS);
    }

    private void logE(String trace, String msg, Throwable t) {
        System.out.println(ts() + " [ERROR] [" + trace + "] " + msg);
        if (t != null) t.printStackTrace();
    }

    private static String safeMsg(String s) {
        if (s == null) return "null";
        return s.replace("\n", " ").replace("\r", " ");
    }
}
