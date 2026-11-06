package com.reconciliation.config;

import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.RetryPolicy;
import com.reconciliation.application.port.RiskScorer;
import com.reconciliation.application.port.TransientFailureClassifier;
import com.reconciliation.application.usecase.ProcessImportUseCase;
import com.reconciliation.application.usecase.PurgeExpiredUploadsUseCase;
import com.reconciliation.infrastructure.worker.RetentionScheduler;
import com.reconciliation.domain.processing.RiskScoreCalculator;
import com.reconciliation.infrastructure.retry.ExponentialBackoffRetryPolicy;
import com.reconciliation.infrastructure.retry.SqlStateFailureClassifier;
import com.reconciliation.infrastructure.risk.WorkerPoolRiskScorer;
import com.reconciliation.infrastructure.worker.ImportWorker;
import io.micrometer.context.ContextSnapshotFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.util.concurrent.ThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the processing side. The CPU pool, the use case and the worker only exist when
 * {@code app.processing.enabled=true}; an API-only instance never scores anything and
 * never joins the consumer group.
 */
@Configuration
public class ProcessingConfig {

    @Bean
    TransientFailureClassifier transientFailureClassifier() {
        return new SqlStateFailureClassifier();
    }

    @Bean
    RetryPolicy retryPolicy(TransientFailureClassifier classifier, MetricsRecorder metrics, AppProperties props) {
        var r = props.retry();
        return new ExponentialBackoffRetryPolicy(r.maxAttempts(), r.initialBackoff(), r.maxBackoff(), r.multiplier(),
                r.jitter(), classifier, metrics, d -> Thread.sleep(d.toMillis()));
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    WorkerPoolRiskScorer riskScorer(AppProperties props) {
        int threads = props.risk().workerThreads() > 0
                ? props.risk().workerThreads()
                : Math.max(1, Runtime.getRuntime().availableProcessors());
        return new WorkerPoolRiskScorer(threads, props.risk().queueCapacity(),
                new RiskScoreCalculator(props.risk().hashIterations()));
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    ProcessImportUseCase processImportUseCase(ImportRepository imports, ImportJobQueue queue, FileStorage storage,
                                              RiskScorer scorer, BatchWriter batchWriter, RetryPolicy retry,
                                              TransientFailureClassifier classifier, Clock clock,
                                              MetricsRecorder metrics, AppProperties props) {
        var p = props.processing();
        // The writer thread starts on the claiming (Kafka listener) thread, which runs inside the
        // consumer's trace. Capturing the context there and restoring it on the writer makes
        // every batch-write span a child of that trace.
        ContextSnapshotFactory snapshots = ContextSnapshotFactory.builder().build();
        ThreadFactory writerThreads = task -> Thread.ofVirtual().unstarted(snapshots.captureAll().wrap(task));
        return new ProcessImportUseCase(imports, queue, storage, scorer, batchWriter, retry, classifier, clock, metrics,
                new ProcessImportUseCase.Settings(p.batchSize(), p.maxInFlightBatches(), p.maxLineLength(),
                        p.maxRawValueLength(), p.maxAttempts()), writerThreads);
    }

    @Bean
    @ConditionalOnProperty(prefix = "app.processing", name = "enabled", havingValue = "true", matchIfMissing = true)
    ImportWorker importWorker(ImportJobQueue queue, ImportRepository imports, ProcessImportUseCase processImport,
                              Clock clock, AppProperties props) {
        var p = props.processing();
        return new ImportWorker(queue, imports, processImport, clock, p.maxConcurrentImports(), p.heartbeatInterval(),
                p.staleUploadTimeout(), p.redispatchInterval(), props.kafka().redeliveryDelay(),
                props.shutdown().gracePeriod());
    }

    /** Retention runs on workers only, and only when enabled (off by default). */
    @Bean
    @ConditionalOnProperty(prefix = "app", name = {"processing.enabled", "retention.enabled"}, havingValue = "true")
    RetentionScheduler retentionScheduler(ImportRepository imports, FileStorage storage, MetricsRecorder metrics,
                                          Clock clock, AppProperties props) {
        var r = props.retention();
        return new RetentionScheduler(new PurgeExpiredUploadsUseCase(imports, storage, metrics, clock,
                r.uploadedFilesAfter(), r.batchSize()), r.interval());
    }
}
