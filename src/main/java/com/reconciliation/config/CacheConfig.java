package com.reconciliation.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.IdempotencyCache;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.application.port.UploadRateLimiter;
import com.reconciliation.infrastructure.cache.RedisUploadRateLimiter;
import com.reconciliation.infrastructure.cache.RedisIdempotencyCache;
import com.reconciliation.infrastructure.cache.RedisSummaryCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Redis-backed caches. Both adapters fail soft, so Redis is a performance dependency, not a correctness one. */
@Configuration
public class CacheConfig {

    @Bean
    SummaryCache summaryCache(StringRedisTemplate redis, ObjectMapper objectMapper) {
        return new RedisSummaryCache(redis, objectMapper);
    }

    @Bean
    IdempotencyCache idempotencyCache(StringRedisTemplate redis) {
        return new RedisIdempotencyCache(redis);
    }

    /** Per-provider upload rate limit; off (unlimited) unless app.upload.provider-rate-limit > 0. */
    @Bean
    UploadRateLimiter uploadRateLimiter(StringRedisTemplate redis, Clock clock, AppProperties props) {
        int limit = props.upload().providerRateLimit();
        return limit > 0
                ? new RedisUploadRateLimiter(redis, clock, limit, props.upload().providerRateWindow())
                : UploadRateLimiter.UNLIMITED;
    }
}
