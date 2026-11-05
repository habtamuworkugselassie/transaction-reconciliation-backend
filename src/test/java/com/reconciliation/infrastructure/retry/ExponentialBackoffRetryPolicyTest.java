package com.reconciliation.infrastructure.retry;

import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.application.processing.CancellationToken;
import com.reconciliation.testing.RecordingMetrics;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExponentialBackoffRetryPolicyTest {

    private final RecordingMetrics metrics = new RecordingMetrics();
    private final List<Duration> sleeps = new ArrayList<>();

    private ExponentialBackoffRetryPolicy policy(int maxAttempts, double jitter) {
        return new ExponentialBackoffRetryPolicy(maxAttempts, Duration.ofMillis(100), Duration.ofMillis(1000), 2.0,
                jitter, t -> t instanceof IOException, metrics, sleeps::add);
    }

    @Test
    void returnsResultWithoutSleepingWhenFirstAttemptSucceeds() throws Exception {
        String result = policy(3, 0).execute("op", new CancellationToken(), () -> "ok");

        assertThat(result).isEqualTo("ok");
        assertThat(sleeps).isEmpty();
        assertThat(metrics.events).isEmpty();
    }

    @Test
    void retriesTransientFailuresWithExponentialBackoffAndRecordsEachAttempt() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        String result = policy(5, 0).execute("batch-write", new CancellationToken(), () -> {
            if (calls.incrementAndGet() < 4) {
                throw new IOException("blip " + calls.get());
            }
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(calls.get()).isEqualTo(4);
        assertThat(sleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400));
        assertThat(metrics.events).containsExactly("retryAttempt:batch-write:1", "retryAttempt:batch-write:2",
                "retryAttempt:batch-write:3");
    }

    @Test
    void givesUpAfterMaxAttemptsAndRethrowsTheOriginalException() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> policy(3, 0).execute("op", new CancellationToken(), () -> {
            calls.incrementAndGet();
            throw new IOException("still down");
        })).isInstanceOf(IOException.class).hasMessage("still down");

        assertThat(calls.get()).isEqualTo(3);
        assertThat(sleeps).hasSize(2);
        assertThat(metrics.events).containsExactly("retryAttempt:op:1", "retryAttempt:op:2", "retryExhausted:op");
    }

    @Test
    void nonTransientFailuresAreNotRetried() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> policy(5, 0).execute("op", new CancellationToken(), () -> {
            calls.incrementAndGet();
            throw new IllegalStateException("bug");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(sleeps).isEmpty();
        assertThat(metrics.events).isEmpty();
    }

    @Test
    void leaseLossIsNeverRetriedEvenIfWrappedTransient() {
        ExponentialBackoffRetryPolicy policy = new ExponentialBackoffRetryPolicy(5, Duration.ofMillis(1),
                Duration.ofMillis(10), 2, 0, new SqlStateFailureClassifier(), metrics, sleeps::add);
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> policy.execute("op", new CancellationToken(), () -> {
            calls.incrementAndGet();
            throw new LeaseLostException(UUID.randomUUID(), "w1");
        })).isInstanceOf(LeaseLostException.class);

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void cancellationStopsTheRetrySequence() {
        CancellationToken token = new CancellationToken();
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> policy(5, 0).execute("op", token, () -> {
            calls.incrementAndGet();
            token.cancel(CancellationToken.Reason.SHUTDOWN);
            throw new IOException("blip");
        })).isInstanceOf(CancellationException.class);

        assertThat(calls.get()).isEqualTo(1);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void backoffIsCappedAtMaxBackoff() {
        ExponentialBackoffRetryPolicy policy = policy(10, 0);

        assertThat(policy.delayFor(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.delayFor(4)).isEqualTo(Duration.ofMillis(800));
        assertThat(policy.delayFor(5)).isEqualTo(Duration.ofMillis(1000));
        assertThat(policy.delayFor(20)).isEqualTo(Duration.ofMillis(1000));
    }

    @Test
    void jitterSpreadsDelaysWithinTheConfiguredBand() {
        ExponentialBackoffRetryPolicy policy = policy(10, 0.25);
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            long ms = policy.delayFor(2).toMillis(); // base 200 ms, band 150..250
            assertThat(ms).isBetween(150L, 250L);
            samples.add(ms);
        }
        assertThat(samples.stream().distinct().count()).as("jitter actually randomises").isGreaterThan(10);
    }

    @Test
    void rejectsZeroAttempts() {
        assertThatThrownBy(() -> policy(0, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
