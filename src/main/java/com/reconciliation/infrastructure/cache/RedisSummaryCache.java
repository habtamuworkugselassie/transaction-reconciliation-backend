package com.reconciliation.infrastructure.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.domain.model.ImportSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Summaries as JSON strings in Redis under {@code summary:<importId>}. Every operation
 * is wrapped so that Redis being down costs a log line and a database read, nothing
 * more. Keys carry a TTL and Redis runs with an LRU eviction policy, so the cache
 * can never grow without bound.
 */
public final class RedisSummaryCache implements SummaryCache {

    private static final Logger log = LoggerFactory.getLogger(RedisSummaryCache.class);
    static final String PREFIX = "summary:";

    private final StringRedisTemplate redis;
    private final ObjectMapper json;

    public RedisSummaryCache(StringRedisTemplate redis, ObjectMapper json) {
        this.redis = redis;
        this.json = json;
    }

    @Override
    public Optional<ImportSummary> get(UUID importId) {
        try {
            String value = redis.opsForValue().get(PREFIX + importId);
            return value == null ? Optional.empty() : Optional.of(json.readValue(value, ImportSummary.class));
        } catch (Exception e) {
            log.warn("Summary cache read failed; falling back to database: {}", e.toString());
            return Optional.empty();
        }
    }

    @Override
    public void put(UUID importId, ImportSummary summary, Duration ttl) {
        try {
            redis.opsForValue().set(PREFIX + importId, json.writeValueAsString(summary), ttl);
        } catch (Exception e) {
            log.warn("Summary cache write failed: {}", e.toString());
        }
    }

    @Override
    public void evict(UUID importId) {
        try {
            redis.delete(PREFIX + importId);
        } catch (Exception e) {
            log.warn("Summary cache evict failed: {}", e.toString());
        }
    }
}
