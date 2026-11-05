package com.reconciliation.infrastructure.metrics;

import com.reconciliation.application.port.MetricsRecorder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Micrometer adapter. Tag values are drawn from small fixed sets (reason, result,
 * outcome, operation); ids never become tags.
 */
public final class MicrometerMetricsRecorder implements MetricsRecorder {

    private final MeterRegistry registry;
    private final Counter importsCreated;
    private final Counter importsReplayed;
    private final DistributionSummary uploadBytes;
    private final Counter recordsParsed;
    private final Counter accepted;
    private final Counter rejected;
    private final Counter duplicates;
    private final Timer batchDuration;

    public MicrometerMetricsRecorder(MeterRegistry registry) {
        this.registry = registry;
        this.importsCreated = Counter.builder("imports.created").description("Imports accepted for processing").register(registry);
        this.importsReplayed = Counter.builder("imports.replayed").description("Create requests answered from an existing idempotency key").register(registry);
        this.uploadBytes = DistributionSummary.builder("import.upload.bytes").baseUnit("bytes").description("Size of accepted uploads").register(registry);
        this.recordsParsed = Counter.builder("import.records.parsed").description("Lines parsed (before persistence)").register(registry);
        this.accepted = Counter.builder("import.records").tag("result", "accepted").description("Records by processing result").register(registry);
        this.rejected = Counter.builder("import.records").tag("result", "rejected").register(registry);
        this.duplicates = Counter.builder("import.records").tag("result", "duplicate").register(registry);
        this.batchDuration = Timer.builder("import.batch.duration").description("Scoring wait + persistence per batch")
                .publishPercentiles(0.5, 0.95, 0.99).register(registry);
    }

    @Override
    public void importCreated(long bytes) {
        importsCreated.increment();
        uploadBytes.record(bytes);
    }

    @Override
    public void importReplayed() {
        importsReplayed.increment();
    }

    @Override
    public void uploadRejected(String reason) {
        registry.counter("uploads.rejected", "reason", reason).increment();
    }

    @Override
    public void recordsParsed(int count) {
        recordsParsed.increment(count);
    }

    @Override
    public void batchWritten(int acceptedCount, int rejectedCount, int duplicateCount, long durationNanos) {
        accepted.increment(acceptedCount);
        rejected.increment(rejectedCount);
        duplicates.increment(duplicateCount);
        batchDuration.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void importFinished(String outcome, long durationNanos) {
        Timer.builder("import.duration").tag("outcome", outcome).description("Wall time of one processing attempt")
                .register(registry).record(Duration.ofNanos(durationNanos));
    }

    @Override
    public void retryAttempt(String operation, int attempt) {
        registry.counter("retries", "operation", operation).increment();
    }

    @Override
    public void retryExhausted(String operation) {
        registry.counter("retries.exhausted", "operation", operation).increment();
    }
}
