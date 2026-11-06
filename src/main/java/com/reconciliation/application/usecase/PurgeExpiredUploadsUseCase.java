package com.reconciliation.application.usecase;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.domain.model.ImportJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Data retention: deletes the uploaded file of imports that have been terminal for longer
 * than {@code retainFor}.
 *
 * <p>What is kept: the import row, its counters, accepted transactions, rejections and the
 * summary. Those are the reconciliation result and stay queryable. Only the raw upload is
 * deleted, because it is the bulk of the storage and nothing reads it once an import is
 * finished, except a manual retry of a failed import, which is refused after purging.
 *
 * <p>Order per import: first the conditional {@code markFilePurged} (terminal and not yet
 * purged), then the file deletion. The mark is the claim:
 * <ul>
 *   <li>it is mutually exclusive with a client retry, which requires {@code file_purged_at IS NULL},
 *       so a retry can never start on a file that is being deleted;</li>
 *   <li>several workers running retention at once purge each import exactly once.</li>
 * </ul>
 * A crash between the mark and the deletion leaves an orphaned file whose import already
 * says "purged". That is safe: nothing will read the file, it is only wasted space. The
 * failure is logged with the storage key for manual cleanup.
 *
 * <p>Bounded: at most {@code batchSize} imports per run, oldest first.
 */
public class PurgeExpiredUploadsUseCase {

    private static final Logger log = LoggerFactory.getLogger(PurgeExpiredUploadsUseCase.class);

    private final ImportRepository imports;
    private final FileStorage storage;
    private final MetricsRecorder metrics;
    private final Clock clock;
    private final Duration retainFor;
    private final int batchSize;

    public PurgeExpiredUploadsUseCase(ImportRepository imports, FileStorage storage, MetricsRecorder metrics,
                                      Clock clock, Duration retainFor, int batchSize) {
        this.imports = imports;
        this.storage = storage;
        this.metrics = metrics;
        this.clock = clock;
        this.retainFor = retainFor;
        this.batchSize = batchSize;
    }

    /** One bounded run; returns how many files were deleted. */
    public int execute() {
        Instant now = clock.now();
        List<ImportJob> candidates = imports.findPurgeable(now.minus(retainFor), batchSize);
        int purged = 0;
        for (ImportJob job : candidates) {
            if (!imports.markFilePurged(job.id(), now)) {
                continue; // retried, or purged by another worker, since it was listed
            }
            try {
                storage.delete(job.storageKey());
                purged++;
            } catch (IOException e) {
                log.warn("Retention marked import {} purged but could not delete key={}: {}", job.id(),
                        job.storageKey(), e.toString());
            }
        }
        if (purged > 0) {
            metrics.uploadFilesPurged(purged);
            log.info("Retention deleted {} uploaded file(s) older than {}", purged, retainFor);
        }
        return purged;
    }
}
