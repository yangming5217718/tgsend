package com.tele.common;

import java.util.Collections;
import java.util.concurrent.Semaphore;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

@Component
public class RedisSlidingWindowRateLimiter {

    private final StringRedisTemplate redis;
    private final RedisScript<Long> tokenBucketScript;
    private final RedisScript<Long> tokenBucketBatchScript;

    // ✅ Redis异常 fallback（每秒最多放行 N 条，防止全停）
    private static final int FALLBACK_QPS = 20; // 可调：10/20/30
    private final Semaphore fallbackPermits = new Semaphore(0);
    private volatile long fallbackResetAtMs = 0;

    // 单次 1 令牌
    private static final String LUA_TOKEN_BUCKET =
        "local key = KEYS[1] \n" +
        "local now = tonumber(ARGV[1]) \n" +
        "local rate = tonumber(ARGV[2]) \n" +
        "local cap = tonumber(ARGV[3]) \n" +
        "local ttl = tonumber(ARGV[4]) \n" +
        "local data = redis.call('HMGET', key, 'tokens', 'ts') \n" +
        "local tokens = tonumber(data[1]) \n" +
        "local ts = tonumber(data[2]) \n" +
        "if tokens == nil then tokens = cap end \n" +
        "if ts == nil then ts = now end \n" +
        "local delta = now - ts \n" +
        "if delta > 0 then \n" +
        "  local add = (delta / 1000.0) * rate \n" +
        "  tokens = math.min(cap, tokens + add) \n" +
        "  ts = now \n" +
        "end \n" +
        "if tokens >= 1.0 then \n" +
        "  tokens = tokens - 1.0 \n" +
        "  redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(ts)) \n" +
        "  redis.call('PEXPIRE', key, ttl) \n" +
        "  return 1 \n" +
        "else \n" +
        "  redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(ts)) \n" +
        "  redis.call('PEXPIRE', key, ttl) \n" +
        "  return 0 \n" +
        "end \n";

    private static final String LUA_TOKEN_BUCKET_BATCH =
        "local key = KEYS[1] \n" +
        "local now = tonumber(ARGV[1]) \n" +
        "local rate = tonumber(ARGV[2]) \n" +
        "local cap = tonumber(ARGV[3]) \n" +
        "local ttl = tonumber(ARGV[4]) \n" +
        "local want = tonumber(ARGV[5]) \n" +
        "if want < 1 then return 0 end \n" +
        "local data = redis.call('HMGET', key, 'tokens', 'ts') \n" +
        "local tokens = tonumber(data[1]) \n" +
        "local ts = tonumber(data[2]) \n" +
        "if tokens == nil then tokens = cap end \n" +
        "if ts == nil then ts = now end \n" +
        "local delta = now - ts \n" +
        "if delta > 0 then \n" +
        "  local add = (delta / 1000.0) * rate \n" +
        "  tokens = math.min(cap, tokens + add) \n" +
        "  ts = now \n" +
        "end \n" +
        "local grant = math.floor(math.min(tokens, want)) \n" +
        "if grant > 0 then \n" +
        "  tokens = tokens - grant \n" +
        "end \n" +
        "redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(ts)) \n" +
        "redis.call('PEXPIRE', key, ttl) \n" +
        "return grant \n";

    public RedisSlidingWindowRateLimiter(StringRedisTemplate redis) {
        this.redis = redis;
        this.tokenBucketScript = RedisScript.of(LUA_TOKEN_BUCKET, Long.class);
        this.tokenBucketBatchScript = RedisScript.of(LUA_TOKEN_BUCKET_BATCH, Long.class);
    }

    public boolean allowOnceBurst(String key, double ratePerSec, int burstCapacity) {
        long now = System.currentTimeMillis();
        long ttl = Math.max(2000L, (long) (Math.ceil(burstCapacity / Math.max(0.1, ratePerSec)) * 1000L) + 2000L);

        try {
            Long ok = redis.execute(
                tokenBucketScript,
                Collections.singletonList(key),
                String.valueOf(now),
                String.valueOf(ratePerSec),
                String.valueOf(burstCapacity),
                String.valueOf(ttl)
            );
            return ok != null && ok == 1L;
        } catch (DataAccessException e) {
            // ✅ fail-soft：Redis挂了也能继续发，但速度保守
            refillFallback(now);
            return fallbackPermits.tryAcquire();
        }
    }

    public int borrowBatch(String key, double ratePerSec, int burstCapacity, int want) {
        long now = System.currentTimeMillis();
        long ttl = Math.max(2000L, (long) (Math.ceil(burstCapacity / Math.max(0.1, ratePerSec)) * 1000L) + 2000L);

        try {
            Long got = redis.execute(
                tokenBucketBatchScript,
                Collections.singletonList(key),
                String.valueOf(now),
                String.valueOf(ratePerSec),
                String.valueOf(burstCapacity),
                String.valueOf(ttl),
                String.valueOf(Math.max(1, want))
            );
            return got == null ? 0 : got.intValue();
        } catch (DataAccessException e) {
            refillFallback(now);
            int granted = 0;
            for (int i = 0; i < Math.max(1, want); i++) {
                if (fallbackPermits.tryAcquire()) granted++;
                else break;
            }
            return granted;
        }
    }

    private void refillFallback(long now) {
        if (now - fallbackResetAtMs >= 1000) {
            fallbackResetAtMs = now;
            fallbackPermits.drainPermits();
            fallbackPermits.release(FALLBACK_QPS);
        }
    }

    public boolean allowOnce(String key, int limit, long windowMillis) {
        if (windowMillis <= 0) return false;
        double rate = (double) limit / ((double) windowMillis / 1000.0);
        int cap = Math.max(1, limit);
        return allowOnceBurst(key, rate, cap);
    }

    public void warmup(String sampleKey) {
        allowOnceBurst(sampleKey != null ? sampleKey : "rl:warmup", 1.0, 1);
    }
}
