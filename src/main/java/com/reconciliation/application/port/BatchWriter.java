package com.reconciliation.application.port;

import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.ScoredTransaction;

import java.util.List;
import java.util.UUID;

/**
 * Persists the results of one batch atomically: accepted transactions, rejections,
 * duplicate classification, progress counters and the checkpoint. Either all of it
 * is committed or none of it, which is what makes a crash at any point recoverable
 * by simply re-reading from the checkpoint.
 */
public interface BatchWriter {

    record BatchWrite(
            UUID importId,
            String workerId,
            int batchNumber,
            /** Highest physical line number covered by this batch; becomes the checkpoint. */
            long lastLineNumber,
            List<ScoredTransaction> candidates,
            List<Rejection> rejections
    ) {}

    record BatchOutcome(int accepted, int rejected, int duplicates, ImportProgress progressAfter,
                        /** True when the batch had already been committed by an earlier attempt. */
                        boolean alreadyCommitted) {}

    /**
     * @throws LeaseLostException if the worker no longer owns the import; nothing is written.
     */
    BatchOutcome write(BatchWrite batch);
}
