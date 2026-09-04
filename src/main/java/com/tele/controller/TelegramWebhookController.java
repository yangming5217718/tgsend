package com.tele.controller;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/telegram/webhook")
public class TelegramWebhookController {

    private final ObjectMapper om = new ObjectMapper();
    private final StringRedisTemplate redis;

    public TelegramWebhookController(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private String webhookSecret="bot88888888yule123";

    @Value("${app.redis.streams.updates:updates}")
    private String updatesStream;

    
    @GetMapping
    public ResponseEntity<String> ping() {
        return ResponseEntity.ok("Webhook endpoint is up (expects POST from Telegram)");
    }
    
    
    @PostMapping
    public ResponseEntity<Void> onUpdate(
            @RequestHeader(value = "X-Telegram-Bot-Api-Secret-Token", required = false) String secret,
            @RequestBody String body) throws Exception {

        if (secret == null || !secret.equals(webhookSecret)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }

        JsonNode node = om.readTree(body);
        long updateId = node.path("update_id").asLong(-1);

        String idemKey = "tg:idm:" + updateId;
        Boolean first = redis.opsForValue().setIfAbsent(idemKey, "1", Duration.ofHours(1));
        if (Boolean.FALSE.equals(first)) {
            return ResponseEntity.ok().build();
        }

        Map<String, String> fields = new HashMap<>();
        fields.put("update", body);
        fields.put("ts", String.valueOf(System.currentTimeMillis()));

        System.out.println("获得"+body);
        
        redis.opsForStream().add(StreamRecords.newRecord().ofMap(fields).withStreamKey(updatesStream));

        return ResponseEntity.ok().build();
    }
}
