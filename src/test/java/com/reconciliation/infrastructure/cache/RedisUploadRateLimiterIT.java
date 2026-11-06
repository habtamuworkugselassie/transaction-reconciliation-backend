package com.reconciliation.infrastructure.cache;

import com.reconciliation.application.port.UploadRateLimiter.Decision;
import com.reconciliation.testing.FixedClock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fixed-window limiter against a real Redis: the Lua script's atomicity, the key TTL,
 * the per-provider isolation and failing open.
 *
 * <p>Redis comes from {@code TEST_REDIS_HOST}/{@code TEST_REDIS_PORT} (e.g. a CI service
 * container; the database is flushed) or else a Testcontainers {@code redis:7-alpine}.
 */
class RedisUploadRateLimiterIT {

    private static GenericContainer<?> container;
    private static LettuceConnectionFactory connections;
    private static StringRedisTemplate redis;

    private final FixedClock clock = new FixedClock(Instant.parse("2026-07-24T08:00:00Z"));

    @BeforeAll
    static void startRedis() {
        String host = System.getenv("TEST_REDIS_HOST");
        int port;
        if (host != null && !host.isBlank()) {
            String p = System.getenv("TEST_REDIS_PORT");
            port = p == null || p.isBlank() ? 6379 : Integer.parseInt(p);
        } else {
            container = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
            container.start();
            host = container.getHost();
            port = container.getMappedPort(6379);
        }
        connections = new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port));
        connections.afterPropertiesSet();
        connections.start();
        redis = new StringRedisTemplate(connections);
    }

    @AfterAll
    static void stopRedis() {
        if (connections != null) {
            connections.destroy();
        }
        if (container != null) {
            container.stop();
        }
    }

    @BeforeEach
    void flush() {
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) c -> {
            c.serverCommands().flushDb();
            return null;
        });
    }

    @Test
    void admitsUpToTheLimitPerWindowThenRefusesWithTheTimeToTheNextWindow() {
        clock.set(Instant.parse("2026-07-24T08:00:15Z")); // 15 s into a 1-minute window
        RedisUploadRateLimiter limiter = new RedisUploadRateLimiter(redis, clock, 3, Duration.ofMinutes(1));

        assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
        assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
        assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
        Decision fourth = limiter.tryAcquire("acme");

        assertThat(fourth.allowed()).isFalse();
        assertThat(fourth.retryAfterSeconds()).isEqualTo(45);
    }

    @Test
    void providersHaveSeparateBudgets() {
        RedisUploadRateLimiter limiter = new RedisUploadRateLimiter(redis, clock, 1, Duration.ofMinutes(1));

        assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
        assertThat(limiter.tryAcquire("acme").allowed()).isFalse();
        assertThat(limiter.tryAcquire("globex").allowed()).isTrue();
    }

    @Test
    void theNextWindowStartsAFreshBudget() {
        RedisUploadRateLimiter limiter = new RedisUploadRateLimiter(redis, clock, 1, Duration.ofMinutes(1));
        assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
        assertThat(limiter.tryAcquire("acme").allowed()).isFalse();

        clock.advance(Duration.ofMinutes(1));

        assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
    }

    @Test
    void everyCounterKeyHasATtl() {
        new RedisUploadRateLimiter(redis, clock, 5, Duration.ofMinutes(1)).tryAcquire("acme");

        var keys = redis.keys(RedisUploadRateLimiter.PREFIX + "*");
        assertThat(keys).hasSize(1);
        Long ttlMs = redis.getExpire(keys.iterator().next(), TimeUnit.MILLISECONDS);
        assertThat(ttlMs).isPositive().isLessThanOrEqualTo(61_000L);
    }

    @Test
    void concurrentRequestsNeverExceedTheLimit() throws Exception {
        RedisUploadRateLimiter limiter = new RedisUploadRateLimiter(redis, clock, 10, Duration.ofMinutes(1));

        List<Decision> decisions = concurrently(40, () -> limiter.tryAcquire("acme"));

        assertThat(decisions.stream().filter(Decision::allowed).count()).isEqualTo(10);
    }

    @Test
    void failsOpenWhenRedisIsUnreachable() {
        LettuceConnectionFactory dead = new LettuceConnectionFactory(new RedisStandaloneConfiguration("127.0.0.1", 1));
        dead.afterPropertiesSet();
        dead.start();
        try {
            RedisUploadRateLimiter limiter = new RedisUploadRateLimiter(new StringRedisTemplate(dead), clock, 1,
                    Duration.ofMinutes(1));
            assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
            assertThat(limiter.tryAcquire("acme").allowed()).isTrue();
        } finally {
            dead.destroy();
        }
    }

    private static <T> List<T> concurrently(int threads, Callable<T> action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return action.call();
                }));
            }
            start.countDown();
            List<T> out = new ArrayList<>();
            for (Future<T> f : futures) {
                out.add(f.get(10, TimeUnit.SECONDS));
            }
            return out;
        } finally {
            pool.shutdownNow();
        }
    }
}
