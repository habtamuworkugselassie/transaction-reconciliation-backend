package com.reconciliation.infrastructure.cache;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.UploadRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.time.Duration;
import java.util.List;

/**
 * Fixed-window counter in Redis: {@code rl:upload:<provider>:<window index>}.
 *
 * <p>The increment and the expiry run in one Lua script, so they are atomic. A crash
 * between a separate INCR and EXPIRE would leave a counter without a TTL that blocks the
 * provider forever. Keys expire one second after their window ends, so Redis holds at most
 * one or two keys per active provider.
 *
 * <p>Fixed windows allow up to 2x the limit across a window boundary; that is acceptable for
 * fairness between providers, and it keeps the check to one round trip. Fails open (see
 * {@link UploadRateLimiter}).
 */
public final class RedisUploadRateLimiter implements UploadRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RedisUploadRateLimiter.class);
    static final String PREFIX = "rl:upload:";

    private static final DefaultRedisScript<Long> INCREMENT = new DefaultRedisScript<>("""
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then
              redis.call('PEXPIRE', KEYS[1], ARGV[1])
            end
            return count
            """, Long.class);

    private final StringRedisTemplate redis;
    private final Clock clock;
    private final int limit;
    private final Duration window;

    public RedisUploadRateLimiter(StringRedisTemplate redis, Clock clock, int limit, Duration window) {
        if (limit < 1 || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("limit must be >= 1 and window positive");
        }
        this.redis = redis;
        this.clock = clock;
        this.limit = limit;
        this.window = window;
    }

    @Override
    public Decision tryAcquire(String providerId) {
        long nowMs = clock.now().toEpochMilli();
        long windowMs = window.toMillis();
        long index = nowMs / windowMs;
        long windowEndMs = (index + 1) * windowMs;
        String key = PREFIX + providerId + ":" + index;
        try {
            Long count = redis.execute(INCREMENT, List.of(key), String.valueOf(windowEndMs - nowMs + 1000));
            if (count == null || count <= limit) {
                return Decision.ALLOW;
            }
            long retryAfter = Math.max(1, (windowEndMs - nowMs + 999) / 1000);
            return new Decision(false, retryAfter);
        } catch (Exception e) {
            log.warn("Upload rate limit check failed; admitting the upload: {}", e.toString());
            return Decision.ALLOW;
        }
    }
}
