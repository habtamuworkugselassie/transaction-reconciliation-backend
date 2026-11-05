package com.reconciliation.testing;

import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.domain.model.ScoredTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Mirrors the jOOQ batch writer's contract without a database: lease check, checkpoint
 * guard, a (provider, transactionId) uniqueness map standing in for the unique
 * constraint, and atomic counter/checkpoint advancement. A test can inject a failure
 * for a given batch number to simulate a database error.
 */
public final class InMemoryBatchWriter implements BatchWriter {

    public final List<ScoredTransaction> accepted = new CopyOnWriteArrayList<>();
    public final List<Rejection> rejections = new CopyOnWriteArrayList<>();
    public final List<Integer> writtenBatches = new CopyOnWriteArrayList<>();
    /** batch number -> hooks run before the write, each consumed once; a returned exception is thrown, null proceeds. */
    public final Map<Integer, List<Callable<Exception>>> failures = new HashMap<>();

    private final InMemoryImportRepository imports;
    private final Clock clock;
    private final Map<String, String> fingerprintsByKey = new HashMap<>();

    public InMemoryBatchWriter(InMemoryImportRepository imports, Clock clock) {
        this.imports = imports;
        this.clock = clock;
    }

    public void failBatch(int batchNumber, Exception failure) {
        failures.computeIfAbsent(batchNumber, k -> new ArrayList<>()).add(() -> failure);
    }

    @Override
    public synchronized BatchOutcome write(BatchWrite batch) {
        List<Callable<Exception>> pending = failures.get(batch.batchNumber());
        if (pending != null && !pending.isEmpty()) {
            Exception e;
            try {
                e = pending.remove(0).call();
            } catch (Exception ex) {
                e = ex;
            }
            if (e instanceof RuntimeException re) {
                throw re;
            }
            if (e != null) {
                throw new RuntimeException(e);
            }
            // A hook that returns null is a side effect only (e.g. "cancel arrives now"): carry on.
        }
        ImportJob row = imports.get(batch.importId());
        boolean leased = row.status() == ImportStatus.PROCESSING || row.status() == ImportStatus.CANCELLING;
        if (!row.isOwnedBy(batch.workerId()) || !leased) {
            throw new LeaseLostException(batch.importId(), batch.workerId());
        }
        if (row.lastLineCommitted() >= batch.lastLineNumber()) {
            return new BatchOutcome(0, 0, 0, row.progress(), true);
        }
        int acceptedCount = 0;
        int duplicates = 0;
        List<Rejection> batchRejections = new ArrayList<>(batch.rejections());
        for (ScoredTransaction s : batch.candidates()) {
            String key = s.transaction().providerId() + "\u0000" + s.transaction().transactionId();
            String existing = fingerprintsByKey.putIfAbsent(key, s.fingerprint());
            if (existing == null) {
                accepted.add(s);
                acceptedCount++;
            } else {
                boolean identical = existing.equals(s.fingerprint());
                batchRejections.add(new Rejection(s.transaction().lineNumber(),
                        identical ? RejectionReason.DUPLICATE_TRANSACTION : RejectionReason.DUPLICATE_TRANSACTION_CONFLICT,
                        identical ? "identical" : "conflict", Map.of("transactionId", s.transaction().transactionId())));
                duplicates++;
            }
        }
        rejections.addAll(batchRejections);
        writtenBatches.add(batch.batchNumber());
        ImportJob after = imports.advance(batch.importId(), batch.lastLineNumber(), acceptedCount,
                batch.rejections().size(), duplicates, clock.now());
        return new BatchOutcome(acceptedCount, batch.rejections().size(), duplicates, after.progress(), false);
    }
}
