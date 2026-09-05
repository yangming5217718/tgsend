package com.tele.mybots;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.tele.entity.*;
import com.tele.mapper.*;
import com.tele.service.CallbackQueryService;
import com.tele.service.InlineQueryService;
import com.tele.service.MessageCommandService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import static com.tele.common.Utils.getCurrentDateTimeForyyyyMMddHHmmss;


@Slf4j
@Component
@EnableScheduling
public class UpdateWorker {

    private final StringRedisTemplate redis;
    private final ObjectMapper om;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ExecutorService exec;

    private final CallbackQueryService callbackQueryService;
    private final MessageCommandService messageCommandService;
    private final InlineQueryService inlineQueryService;


    public UpdateWorker(StringRedisTemplate redis, ObjectMapper om, CallbackQueryService callbackQueryService,
                        MessageCommandService messageCommandService,
                        InlineQueryService inlineQueryService){
        this.redis=redis;
        this.om = om;
        this.callbackQueryService=callbackQueryService;
        this.messageCommandService=messageCommandService;
        this.inlineQueryService=inlineQueryService;
    }

    @Autowired private CpInstructionUserMapper cpInstructionUserMapper;

    @Value("${app.redis.streams.updates}")
    private String updatesStream;

    @Value("${app.redis.groups.worker}")
    private String workerGroup;

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (running.get()) return;
        log.info("【下注机器人】UpdateWorker启动，监听流={} 消费组={}", updatesStream, workerGroup);
        try {
            redis.opsForStream().createGroup(updatesStream, ReadOffset.latest(), workerGroup);
        } catch (Exception ignore) {}

        int threads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        exec = Executors.newFixedThreadPool(threads, r -> {
            Thread t=new Thread(r);
            t.setName("tg-update-worker");
            return t;
        });
        running.set(true);

        log.info("【下注机器人】消费者线程启动，线程数量={}", threads);
        for (int i = 0; i < threads; i++) {
            exec.submit(this::loop);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("【下注机器人】UpdateWorker停止");
        running.set(false);
        if (exec != null) {
            exec.shutdownNow();
            try {
                exec.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ==========================
    // 主消费循环
    // ==========================
    private void loop() {
        final String consumerName = "c-" + UUID.randomUUID();
        final StreamReadOptions opts = StreamReadOptions.empty().count(100).block(Duration.ofSeconds(2));

        int backoffMs = 200;

        while (running.get()) {
            try {
                List<MapRecord<String, Object, Object>> records =
                        redis.opsForStream().read(
                                Consumer.from(workerGroup, consumerName),
                                opts,
                                StreamOffset.create(updatesStream, ReadOffset.lastConsumed())
                        );

                backoffMs = 200;

                if (records == null || records.isEmpty()) {
                    continue;
                }

                for (MapRecord<String, Object, Object> r : records) {
                    String trace = newTrace();
                    String rid = r.getId().getValue();

                    log.info("【消息消费】收到Telegram更新 trace={} id={}", trace, rid);
                    try {
                        Object val = r.getValue().get("update");
                        if (val == null) {
                            log.warn("【消息消费】更新内容为空 trace={} id={} data={}",
                                    trace, rid, r.getValue());
                            ack(r,trace);
                            continue;
                        }
                        String body = String.valueOf(val);
                        JsonNode update = om.readTree(body);
                        dispatchUpdate(trace, update);
                        ack(r,trace);
                    } catch (Exception ex) {
                        log.error("【消息消费】处理失败 trace={} id={}", trace, rid, ex);
                        ack(r,trace);
                    }
                }
            } catch (IllegalStateException e) {
                String msg = String.valueOf(e.getMessage());
                if (!running.get() || msg.contains("STOPPING")) {
                    return;
                }
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);
            } catch (RedisSystemException e) {
                log.error("【Redis】Stream消费异常，准备退避等待 backoff={}ms", backoffMs, e);
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);
            } catch (Exception e) {
                e.printStackTrace();
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 5000);
            }
        }
    }

    private void dispatchUpdate(String trace, JsonNode update){
        if(update == null){
            log.warn("【事件分发】收到空update trace={}", trace);
            return;
        }
        // 群成员状态变化：进群、退群、被踢
        if (!update.path("chat_member").isMissingNode()) {
            log.info("【事件分发】群成员变化 trace={}", trace);
            handleChatMember(update, trace);
            return;
        }
        //按钮点击
        if(!update.path("callback_query").isMissingNode()){
            JsonNode callback = update.path("callback_query");
            log.info("【事件分发】收到按钮点击 trace={} 用户={} 数据={}",
                    trace, callback.path("from").path("id").asText(),
                    callback.path("data").asText());
            callbackQueryService.handleCallbackQuery(trace, callback);
            return;
        }
        //inline 输入框查询
        if(!update.path("inline_query").isMissingNode()){
            log.info("【事件分发】收到inline查询 trace={}", trace);
            inlineQueryService.handleInlineQuery(update.path("inline_query"), trace);
            return;
        }
        //inline 结果被选中并发出，这时才拿得到 inline_message_id
        if(!update.path("chosen_inline_result").isMissingNode()){
            log.info("【事件分发】收到inline选中结果 trace={}", trace);
            inlineQueryService.handleChosenInlineResult(update.path("chosen_inline_result"), trace);
            return;
        }
        //普通消息
        if(!update.path("message").isMissingNode()){
            JsonNode msg = update.path("message");
            log.info("【事件分发】收到文本消息 trace={} 群={} 用户={} 内容={}",
                    trace, msg.path("chat").path("id").asText(),
                    msg.path("from").path("id").asText(),
                    msg.path("text").asText(""));
            messageCommandService.executeMessage(update,trace);
            return;
        }

        log.warn("【事件分发】未识别Telegram事件 trace={} update={}", trace, update);
    }

    private void ack(MapRecord<String, Object, Object> r,String trace) {
        try {
            redis.opsForStream().acknowledge(updatesStream, workerGroup, r.getId());
        } catch (Exception e) {
            log.error("【消息确认】ACK失败 trace={} id={}", trace, r.getId(), e);
        }
    }

    private void sleepQuietly(int ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    // ==========================
    // 指令表热更新 & 监控
    // ==========================
    @Scheduled(initialDelay = 30_000L, fixedDelay = 300_000L)
    public void scheduledReloadRules() {}

    @Scheduled(initialDelay = 10_000L, fixedDelay = 10_000L)
    public void quickReloadByFlag() {
        try {
            String flag = redis.opsForValue().get("instructionRules:reload");
            if ("1".equals(flag)) {
                log.info("【下注规则】收到规则刷新通知");
                redis.opsForValue().set("instructionRules:reload", "0");
            }
        } catch (Exception ignored) {
        }
    }

    @Scheduled(initialDelay = 60_000L, fixedDelay = 30_000L)
    public void metrics() {
        try {
            Long xlen = redis.opsForStream().size(updatesStream);
            PendingMessagesSummary pending = null;
            try {
                pending = redis.opsForStream().pending(updatesStream, workerGroup);
            } catch (Exception ignore) {}

            StringBuilder sb = new StringBuilder(256);
            if (pending != null) {
                sb.append(" pending.total=").append(pending.getTotalPendingMessages());
                if (pending.getTotalPendingMessages() > 0) {
                    PendingMessages one = redis.opsForStream().pending(
                            updatesStream, workerGroup, Range.unbounded(), 1);
                    long idleMs = -1;
                    if (one != null && !one.isEmpty()) {
                        try { idleMs = one.get(0).getElapsedTimeSinceLastDelivery().toMillis(); } catch (Exception ignore) {}
                    }
                    sb.append(" oldestIdleMs=").append(idleMs);

                    try {
                        PendingMessages consumers = redis.opsForStream().pending(
                                updatesStream, workerGroup, Range.unbounded(), 1000);
                        Map<String, Integer> byConsumer = new HashMap<>();
                        if (consumers != null) {
                            for (PendingMessage pm : consumers) {
                                String c = pm.getConsumerName();
                                byConsumer.merge(c, 1, Integer::sum);
                            }
                        }
                        sb.append(" byConsumer=").append(byConsumer);
                    } catch (Exception ignore) {}
                }
            } else {
                sb.append(" pending=unavailable");
            }

        } catch (Exception ignored) {

        }
    }

    /**
     * 进群处理方法
     * @param update
     * @param trace
     */
    private void handleChatMember(JsonNode update, String trace) {
        JsonNode memberUpdate = update.path("chat_member");

        JsonNode chat = memberUpdate.path("chat");
        //oldMember：用户状态变化之前的信息
        JsonNode oldMember = memberUpdate.path("old_chat_member");
        //newMember：用户状态变化之后的信息
        JsonNode newMember = memberUpdate.path("new_chat_member");
        JsonNode targetUser = newMember.path("user");

        String chatId = chat.path("id").asText("");
        String chatType = chat.path("type").asText("");

        String telegramUserId = targetUser.path("id").asText("");
        String userCoding = targetUser.path("username").asText("");
        String firstName = targetUser.path("first_name").asText("");
        String lastName = targetUser.path("last_name").asText("");
        boolean isBot = targetUser.path("is_bot").asBoolean(false);

        String oldStatus = oldMember.path("status").asText("");
        String newStatus = newMember.path("status").asText("");

        //状态变化之前，用户是否仍然属于群成员
        //读取 JSON 中的 is_member，转换成 boolean；如果字段不存在，就默认返回 false
        boolean oldIsMember = oldMember.path("is_member").asBoolean(false);
        //状态变化之后，用户是否仍然属于群成员
        boolean newIsMember = newMember.path("is_member").asBoolean(false);

        if (StringUtils.isBlank(chatId) || StringUtils.isBlank(telegramUserId)) {
            log.warn("【群成员】数据异常 trace={} chatId={} userId={}", trace, chatId, telegramUserId);
            return;
        }
        // 不注册机器人
        if (isBot) {
            return;
        }
        // 只处理群和超级群
        if (!"group".equals(chatType) && !"supergroup".equals(chatType)) {
            return;
        }
        //wasInside：变化之前是否在群里
        boolean wasInside = isInsideGroup(oldStatus, oldIsMember);
        //nowInside：变化之后是否在群里
        boolean nowInside = isInsideGroup(newStatus, newIsMember);
        /*
         * 群外 -> 群内，才是一次进群事件。
         * left   -> member
         * kicked -> member
         * left   -> administrator
         */
        if (!wasInside && nowInside) {
            registerUserOnFirstJoin(
                    telegramUserId,
                    firstName + lastName,
                    userCoding,
                    chatId,
                    trace
            );
            return;
        }
        // 群内 -> 群外，可以更新当前在群状态，但不能删除用户
        if (wasInside && !nowInside) {
            markUserLeftGroup(telegramUserId, chatId, trace);
        }
    }

    /**
     * 成员判断
     * 普通状态比较清楚：
     * member：在群里
     * administrator：在群里
     * creator：在群里
     * left：不在群里
     * kicked：不在群里
     * 只有 restricted 比较特殊。
     * restricted 表示用户权限被限制，但这个用户可能：
     * 仍然在群里
     * 已经不在群里，只保留限制记录
     * @return
     */
    private boolean isInsideGroup(String status, boolean isMember) {
        if ("creator".equals(status) || "administrator".equals(status) || "member".equals(status)) {
            return true;
        }
        // restricted 必须结合 is_member 判断
        if ("restricted".equals(status)) {
            return isMember;
        }
        return false;
    }


    private void registerUserOnFirstJoin(String telegramUserId, String uname, String userCoding,
            String chatId, String trace) {
        try {
            CpInstructionUser user = new CpInstructionUser();
            user.setTelegramUserId(telegramUserId);
            user.setUserName(StringUtils.defaultString(uname));
            user.setUserCoding(StringUtils.defaultString(userCoding));
            // 群来源
            user.setFromtype(1);
            user.setChatid(chatId);
            user.setCreatetime(
                    getCurrentDateTimeForyyyyMMddHHmmss()
            );
            cpInstructionUserMapper.insert(user);
            log.info("【用户管理】新用户进群注册成功 trace={} 群={} 用户={}", trace, chatId, telegramUserId);
        } catch (DuplicateKeyException e) {
            // 数据库已存在，不重复注册
            log.debug("【用户管理】用户已存在，无需重复注册 trace={} 群={} 用户={}", trace, chatId, telegramUserId);
        } catch (Exception e) {
            log.error("【用户管理】用户进群注册失败 trace={} 群={} 用户={}",
                    trace, chatId, telegramUserId, e);
        }
    }

    private void markUserLeftGroup(String telegramUserId, String chatId, String trace) {
        log.info("【群成员】用户离开群聊 trace={} 群={} 用户={} 状态=left", trace, chatId, telegramUserId);
    }


    private String newTrace() {
        return System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 6);
    }
}
