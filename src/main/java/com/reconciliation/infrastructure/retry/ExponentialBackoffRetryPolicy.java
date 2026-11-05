package com.reconciliation.infrastructure.retry;

import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.RetryPolicy;
import com.reconciliation.application.port.TransientFailureClassifier;
import com.reconciliation.application.processing.CancellationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry with capped exponential backoff and full jitter.
 *
 * <p>delay(n) = min(maxBackoff, initial * multiplier^(n-1)) * (1 - jitter + 2 * jitter * U[0,1))
 *
 * <p>Only failures the classifier deems transient are retried; validation errors,
 * duplicate-key violations, lease loss and programming errors propagate on the first
 * attempt. A cancelled token stops the sequence before the next attempt. After the
 * final attempt the original exception is rethrown unchanged so callers can classify
 * it themselves. Jitter spreads retries from many workers over time so a database
 * blip does not produce a synchronised retry storm.
 */
public final class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private static final Logger log = LoggerFactory.getLogger(ExponentialBackoffRetryPolicy.class);

    /** Sleeps between attempts; injectable so tests do not wait. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration d) throws InterruptedException;
    }

    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double multiplier;
    private final double jitter;
    private final TransientFailureClassifier classifier;
    private final MetricsRecorder metrics;
    private final Sleeper sleeper;

    public ExponentialBackoffRetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff, double multiplier,
                                         double jitter, TransientFailureClassifier classifier, MetricsRecorder metrics,
                                         Sleeper sleeper) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.multiplier = multiplier;
        this.jitter = jitter;
        this.classifier = classifier;
        this.metrics = metrics;
        this.sleeper = sleeper;
    }

    @Override
    public <T> T execute(String operation, CancellationToken token, Callable<T> action) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                if (!classifier.isTransient(e)) {
                    throw e;
                }
                if (attempt >= maxAttempts) {
                    metrics.retryExhausted(operation);
                    log.warn("{} failed after {} attempts; giving up: {}", operation, attempt, e.toString());
                    throw e;
                }
                if (token != null && token.isCancelled()) {
                    log.info("{} failed and work is cancelled ({}); not retrying", operation, token.reason());
                    throw new CancellationException("Cancelled before retry of " + operation);
                }
                Duration delay = delayFor(attempt);
                metrics.retryAttempt(operation, attempt);
                MDC.put("attempt", String.valueOf(attempt + 1));
                log.warn("{} failed (attempt {}/{}), retrying in {} ms: {}", operation, attempt, maxAttempts,
                        delay.toMillis(), e.toString());
                MDC.remove("attempt");
                sleeper.sleep(delay);
            }
        }
    }

    Duration delayFor(int attempt) {
        double base = Math.min(maxBackoff.toMillis(), initialBackoff.toMillis() * Math.pow(multiplier, attempt - 1));
        double factor = 1 - jitter + 2 * jitter * ThreadLocalRandom.current().nextDouble();
        return Duration.ofMillis(Math.max(0, Math.round(base * factor)));
    }
}
