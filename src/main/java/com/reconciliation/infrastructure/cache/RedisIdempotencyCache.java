package com.reconciliation.infrastructure.cache;

import com.reconciliation.application.port.IdempotencyCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/** {@code idem:<key> -> importId}. Fail-soft; the database unique constraint is the guarantee. */
public final class RedisIdempotencyCache implements IdempotencyCache {

    private static final Logger log = LoggerFactory.getLogger(RedisIdempotencyCache.class);
    static final String PREFIX = "idem:";

    private final StringRedisTemplate redis;

    public RedisIdempotencyCache(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public Optional<UUID> find(String idempotencyKey) {
        try {
            String value = redis.opsForValue().get(PREFIX + idempotencyKey);
            return value == null ? Optional.empty() : Optional.of(UUID.fromString(value));
        } catch (Exception e) {
            log.warn("Idempotency cache read failed; using database: {}", e.toString());
            return Optional.empty();
        }
    }

    @Override
    public void remember(String idempotencyKey, UUID importId, Duration ttl) {
        try {
            redis.opsForValue().set(PREFIX + idempotencyKey, importId.toString(), ttl);
        } catch (Exception e) {
            log.warn("Idempotency cache write failed: {}", e.toString());
        }
    }
}
