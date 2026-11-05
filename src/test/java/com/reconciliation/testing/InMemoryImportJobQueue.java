package com.reconciliation.testing;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Queue fake: records published ids and delegates ownership to the in-memory
 * repository, exactly like the Kafka adapter delegates to PostgreSQL.
 */
public final class InMemoryImportJobQueue implements ImportJobQueue {

    public final List<UUID> published = new ArrayList<>();
    public volatile RuntimeException failEnqueueWith;

    private final InMemoryImportRepository imports;
    private final Clock clock;
    private final Duration leaseDuration;

    public InMemoryImportJobQueue(InMemoryImportRepository imports, Clock clock, Duration leaseDuration) {
        this.imports = imports;
        this.clock = clock;
        this.leaseDuration = leaseDuration;
    }

    @Override
    public synchronized boolean enqueue(UUID importId) {
        ImportJob j = imports.findById(importId).orElse(null);
        if (j == null || j.status() != ImportStatus.UPLOADING) {
            return false;
        }
        imports.put(new ImportJob(j.id(), j.idempotencyKey(), j.providerId(), ImportStatus.PENDING, j.originalFilename(),
                j.storageKey(), j.fileSizeBytes(), j.contentSha256(), ImportProgress.ZERO, 0, false, 0, null, null,
                null, null, j.createdAt(), clock.now(), null, null));
        if (failEnqueueWith != null) {
            throw failEnqueueWith;
        }
        published.add(importId);
        return true;
    }

    @Override
    public synchronized void redispatch(UUID importId) {
        published.add(importId);
    }

    @Override
    public int redispatchStale(Duration pendingOlderThan, int limit) {
        return 0;
    }

    @Override
    public Optional<ImportJob> claim(UUID importId, String workerId) {
        return imports.claim(importId, workerId, leaseDuration, clock.now());
    }

    @Override
    public boolean heartbeat(UUID importId, String workerId) {
        return imports.extendLease(importId, workerId, leaseDuration, clock.now());
    }

    @Override
    public boolean release(UUID importId, String workerId) {
        return imports.release(importId, workerId, clock.now());
    }

    @Override
    public long queuedCount() {
        return 0;
    }

    @Override
    public long inFlightCount() {
        return 0;
    }
}
