package com.reconciliation.config;

import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.UploadLimiter;
import com.reconciliation.infrastructure.health.StorageHealthIndicator;
import com.reconciliation.infrastructure.health.WorkerHealthIndicator;
import com.reconciliation.infrastructure.metrics.MicrometerMetricsRecorder;
import com.reconciliation.infrastructure.risk.WorkerPoolRiskScorer;
import com.reconciliation.infrastructure.worker.ImportWorker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics, health and shutdown signalling. Gauges that need a database query are
 * sampled by a cheap cached supplier rather than on every scrape.
 */
@Configuration
public class ObservabilityConfig {

    private static final long QUEUE_GAUGE_TTL_NANOS = 5_000_000_000L;

    @Bean
    MetricsRecorder metricsRecorder(MeterRegistry registry) {
        return new MicrometerMetricsRecorder(registry);
    }

    @Bean
    StorageHealthIndicator storageHealthIndicator(FileStorage storage, AppProperties props) {
        return new StorageHealthIndicator(storage, Path.of(props.storage().root()), 100L * 1024 * 1024);
    }

    @Bean
    WorkerHealthIndicator workerHealthIndicator(ObjectProvider<ImportWorker> worker) {
        return new WorkerHealthIndicator(worker.getIfAvailable());
    }

    /** Holder bean: Micrometer references gauge targets weakly, so the sampled objects must stay reachable. */
    public record PipelineGauges(CachedGauge queued, CachedGauge inFlight) {}

    @Bean
    PipelineGauges pipelineGauges(MeterRegistry registry, ImportJobQueue queue, UploadLimiter uploads) {
        PipelineGauges gauges = new PipelineGauges(new CachedGauge(queue::queuedCount), new CachedGauge(queue::inFlightCount));
        Gauge.builder("import.queue.depth", gauges.queued(), CachedGauge::value)
                .description("Imports waiting to be claimed (pending)").register(registry);
        Gauge.builder("import.queue.inflight", gauges.inFlight(), CachedGauge::value)
                .description("Imports leased by any worker").register(registry);
        Gauge.builder("upload.slots.available", uploads, UploadLimiter::available)
                .description("Concurrent upload permits currently free").register(registry);
        Gauge.builder("upload.slots.capacity", uploads, UploadLimiter::capacity).register(registry);
        return gauges;
    }

    public record WorkerGauges(ImportWorker worker, WorkerPoolRiskScorer scorer) {}

    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    WorkerGauges workerGauges(MeterRegistry registry, ImportWorker worker, WorkerPoolRiskScorer scorer) {
        Gauge.builder("import.worker.active", worker, ImportWorker::activeImports)
                .description("Imports being processed by this instance").register(registry);
        Gauge.builder("import.worker.capacity", worker, ImportWorker::capacity).register(registry);
        Gauge.builder("risk.scorer.queue.depth", scorer, WorkerPoolRiskScorer::queueDepth)
                .description("Scoring chunks waiting for a CPU thread").register(registry);
        Gauge.builder("risk.scorer.active", scorer, WorkerPoolRiskScorer::activeWorkers)
                .description("CPU threads currently scoring").register(registry);
        Gauge.builder("risk.scorer.pool.size", scorer, WorkerPoolRiskScorer::poolSize).register(registry);
        return new WorkerGauges(worker, scorer);
    }

    /**
     * Fires at the very start of shutdown, before lifecycle phases run: stop claiming
     * imports and tell active ones to wind down while the HTTP server is still draining.
     */
    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    ShutdownSignal shutdownSignal(ImportWorker worker) {
        return new ShutdownSignal(worker);
    }

    public record ShutdownSignal(ImportWorker worker) {
        @EventListener(ContextClosedEvent.class)
        public void onClose() {
            worker.signalShutdown();
        }
    }

    /** Samples a supplier at most every few seconds; scrapes in between reuse the value. */
    public static final class CachedGauge {
        private final java.util.function.LongSupplier source;
        private final AtomicLong sampledAt = new AtomicLong();
        private volatile long value;

        CachedGauge(java.util.function.LongSupplier source) {
            this.source = source;
        }

        double value() {
            long now = System.nanoTime();
            long last = sampledAt.get();
            if (now - last > QUEUE_GAUGE_TTL_NANOS && sampledAt.compareAndSet(last, now)) {
                try {
                    value = source.getAsLong();
                } catch (RuntimeException e) {
                    // keep the previous sample; readiness reports database trouble separately
                }
            }
            return value;
        }
    }
}
