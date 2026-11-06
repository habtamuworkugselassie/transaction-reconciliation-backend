package com.reconciliation.application.usecase;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Re-queues a failed import ({@code POST /v1/imports/{id}/retry}).
 *
 * <p>Processing resumes from the checkpoint: counters, accepted transactions and
 * rejections committed by earlier attempts are kept, and the unique constraint on
 * {@code (provider_id, transaction_id)} plus the checkpoint guard in every batch make it
 * impossible to count a record twice. The import gets a fresh automatic-attempt budget,
 * and the number of manual retries is capped ({@code maxRetries}), so retrying never
 * creates unbounded work.
 *
 * <p>Only imports whose file was stored completely can be retried. An upload that failed
 * (empty, too large, wrong type, interrupted) has no file; the client re-uploads with the
 * same {@code Idempotency-Key} instead. An import whose file was deleted by retention
 * cannot be retried either.
 *
 * <p>Idempotent: retrying an import that a previous retry already re-queued returns its
 * current state.
 */
public class RetryImportUseCase {

    private static final Logger log = LoggerFactory.getLogger(RetryImportUseCase.class);

    private final ImportRepository imports;
    private final ImportJobQueue queue;
    private final SummaryCache summaryCache;
    private final MetricsRecorder metrics;
    private final Clock clock;
    private final int maxRetries;

    public RetryImportUseCase(ImportRepository imports, ImportJobQueue queue, SummaryCache summaryCache,
                              MetricsRecorder metrics, Clock clock, int maxRetries) {
        this.imports = imports;
        this.queue = queue;
        this.summaryCache = summaryCache;
        this.metrics = metrics;
        this.clock = clock;
        this.maxRetries = maxRetries;
    }

    public ImportJob execute(UUID id) {
        Optional<ImportJob> retried = imports.retryFailed(id, maxRetries, clock.now());
        if (retried.isPresent()) {
            ImportJob job = retried.get();
            // The final summary of the failed attempt is no longer final. The summary use case
            // also checks stored and cached totals against the live counters, so a concurrent
            // reader cannot resurrect it; evicting just avoids serving it until then.
            summaryCache.evict(id);
            metrics.importRetried();
            log.info("Import re-queued by client importId={} retry={}/{} resumeAfterLine={}",
                    id, job.retryCount(), maxRetries, job.lastLineCommitted());
            // Publishing is best effort: the row is already pending, and the worker's sweep
            // re-publishes pending imports if this event is lost.
            queue.redispatch(id);
            return job;
        }

        ImportJob current = imports.findById(id).orElseThrow(() -> AppException.notFound("Import", id));
        if (current.retryCount() > 0 && !current.status().isTerminal()) {
            return current; // an earlier retry already re-queued it
        }
        throw new AppException(ErrorCode.IMPORT_NOT_RETRYABLE, reason(current), null,
                Map.of("importId", id.toString(), "status", current.status().name(),
                        "retryCount", current.retryCount()));
    }

    private String reason(ImportJob job) {
        if (job.status() != ImportStatus.FAILED) {
            return "Only failed imports can be retried; this import is '" + job.status().wireName() + "'";
        }
        if (job.storageKey() == null) {
            return "The upload never completed; upload the file again with the same Idempotency-Key";
        }
        if (job.filePurgedAt() != null) {
            return "The uploaded file has been deleted by the retention policy; upload it again as a new import";
        }
        if (job.retryCount() >= maxRetries) {
            return "Retry limit reached (" + maxRetries + ")";
        }
        return "Import cannot be retried right now; read its status and try again";
    }
}
