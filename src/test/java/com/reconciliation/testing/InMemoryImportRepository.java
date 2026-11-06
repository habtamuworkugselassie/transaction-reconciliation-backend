package com.reconciliation.testing;

import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Faithful in-memory model of the imports table: every mutator is a conditional
 * transition that mirrors the SQL {@code WHERE} clauses of the jOOQ adapter, so use
 * cases see the same "applied / not applied" answers they get from PostgreSQL.
 * All methods are synchronized, which stands in for the row locks.
 */
public final class InMemoryImportRepository implements ImportRepository {

    public record Attempt(long rowId, UUID importId, int attempt, String workerId, String outcome, String failureMessage) {}

    private final Map<UUID, ImportJob> rows = new LinkedHashMap<>();
    private final List<Attempt> attempts = new ArrayList<>();
    private long nextAttemptId = 1;

    // ---- test helpers -------------------------------------------------------

    public synchronized void put(ImportJob job) {
        rows.put(job.id(), job);
    }

    public synchronized ImportJob get(UUID id) {
        ImportJob job = rows.get(id);
        if (job == null) {
            throw new IllegalStateException("no import " + id);
        }
        return job;
    }

    public synchronized int size() {
        return rows.size();
    }

    public synchronized List<Attempt> attempts() {
        return List.copyOf(attempts);
    }

    /** Builds a persisted-looking job in the given state. */
    public static ImportJob job(UUID id, ImportStatus status, String providerId, String storageKey, Instant now) {
        return new ImportJob(id, "key-" + id, providerId, status, "file.ndjson", storageKey, 100L, "sha",
                ImportProgress.ZERO, 0, false, 0, null, null, null, null, now, now, null, null);
    }

    // ---- ImportRepository -------------------------------------------------

    @Override
    public synchronized Reservation reserve(ImportJob job) {
        for (ImportJob existing : rows.values()) {
            if (existing.idempotencyKey().equals(job.idempotencyKey())) {
                return new Reservation.Existing(existing);
            }
        }
        rows.put(job.id(), job);
        return new Reservation.Created(job);
    }

    @Override
    public synchronized Optional<ImportJob> reopenFailedUpload(UUID id, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || j.status() != ImportStatus.FAILED || j.storageKey() != null) {
            return Optional.empty();
        }
        ImportJob reopened = new ImportJob(j.id(), j.idempotencyKey(), j.providerId(), ImportStatus.UPLOADING,
                j.originalFilename(), null, null, null, j.progress(), j.lastLineCommitted(), j.cancelRequested(),
                j.attemptCount(), j.leaseOwner(), j.leaseExpiresAt(), null, null, now, now, j.startedAt(), null,
                j.retryCount(), j.attemptBase(), j.filePurgedAt());
        rows.put(id, reopened);
        return Optional.of(reopened);
    }

    @Override
    public synchronized Optional<ImportJob> findById(UUID id) {
        return Optional.ofNullable(rows.get(id));
    }

    @Override
    public synchronized boolean markUploaded(UUID id, String storageKey, long sizeBytes, String sha256, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || j.status() != ImportStatus.UPLOADING) {
            return false;
        }
        rows.put(id, with(j, j.status(), storageKey, sizeBytes, sha256, j.failureReason(), j.failureMessage(),
                j.leaseOwner(), j.leaseExpiresAt(), j.cancelRequested(), j.attemptCount(), j.startedAt(), j.completedAt(), now));
        return true;
    }

    @Override
    public synchronized boolean markUploadFailed(UUID id, String reason, String message, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || j.status() != ImportStatus.UPLOADING) {
            return false;
        }
        rows.put(id, with(j, ImportStatus.FAILED, j.storageKey(), j.fileSizeBytes(), j.contentSha256(), reason, message,
                j.leaseOwner(), j.leaseExpiresAt(), j.cancelRequested(), j.attemptCount(), j.startedAt(), now, now));
        return true;
    }

    @Override
    public synchronized int failStaleUploads(Instant olderThan, Instant now) {
        int n = 0;
        for (ImportJob j : List.copyOf(rows.values())) {
            if (j.status() == ImportStatus.UPLOADING && j.createdAt().isBefore(olderThan)) {
                markUploadFailed(j.id(), "UPLOAD_INTERRUPTED", "Upload did not complete", now);
                n++;
            }
        }
        return n;
    }

    @Override
    public synchronized Optional<ImportJob> claim(UUID id, String workerId, Duration leaseDuration, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || !claimable(j, now)) {
            return Optional.empty();
        }
        return Optional.of(doClaim(j, workerId, leaseDuration, now));
    }

    @Override
    public synchronized Optional<ImportJob> claimNext(String workerId, Duration leaseDuration, Instant now) {
        return rows.values().stream()
                .filter(j -> claimable(j, now))
                .min(Comparator.comparing(ImportJob::createdAt))
                .map(j -> doClaim(j, workerId, leaseDuration, now));
    }

    private static boolean claimable(ImportJob j, Instant now) {
        if (j.status() == ImportStatus.PENDING) {
            return true;
        }
        boolean leased = j.status() == ImportStatus.PROCESSING || j.status() == ImportStatus.CANCELLING;
        return leased && j.leaseExpiresAt() != null && j.leaseExpiresAt().isBefore(now);
    }

    private ImportJob doClaim(ImportJob j, String workerId, Duration leaseDuration, Instant now) {
        ImportStatus status = j.status() == ImportStatus.CANCELLING ? ImportStatus.CANCELLING : ImportStatus.PROCESSING;
        ImportJob claimed = with(j, status, j.storageKey(), j.fileSizeBytes(), j.contentSha256(), j.failureReason(),
                j.failureMessage(), workerId, now.plus(leaseDuration), j.cancelRequested(), j.attemptCount() + 1,
                j.startedAt() == null ? now : j.startedAt(), j.completedAt(), now);
        rows.put(j.id(), claimed);
        return claimed;
    }

    @Override
    public synchronized boolean extendLease(UUID id, String workerId, Duration leaseDuration, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || !j.isOwnedBy(workerId)
                || !(j.status() == ImportStatus.PROCESSING || j.status() == ImportStatus.CANCELLING)) {
            return false;
        }
        rows.put(id, with(j, j.status(), j.storageKey(), j.fileSizeBytes(), j.contentSha256(), j.failureReason(),
                j.failureMessage(), workerId, now.plus(leaseDuration), j.cancelRequested(), j.attemptCount(),
                j.startedAt(), j.completedAt(), now));
        return true;
    }

    @Override
    public synchronized Optional<ImportJob> requestCancel(UUID id, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || !(j.status() == ImportStatus.PENDING || j.status() == ImportStatus.PROCESSING)) {
            return Optional.empty();
        }
        boolean pending = j.status() == ImportStatus.PENDING;
        ImportJob updated = with(j, pending ? ImportStatus.CANCELLED : ImportStatus.CANCELLING, j.storageKey(),
                j.fileSizeBytes(), j.contentSha256(), j.failureReason(), j.failureMessage(), j.leaseOwner(),
                j.leaseExpiresAt(), true, j.attemptCount(), j.startedAt(), pending ? now : j.completedAt(), now);
        rows.put(id, updated);
        return Optional.of(updated);
    }

    @Override
    public synchronized boolean isCancelRequested(UUID id) {
        ImportJob j = rows.get(id);
        return j == null || j.cancelRequested();
    }

    @Override
    public synchronized boolean complete(UUID id, String workerId, Instant now) {
        return terminal(id, workerId, ImportStatus.COMPLETED, null, null, now, ImportStatus.PROCESSING);
    }

    @Override
    public synchronized boolean fail(UUID id, String workerId, String reason, String message, Instant now) {
        return terminal(id, workerId, ImportStatus.FAILED, reason, message, now, ImportStatus.PROCESSING, ImportStatus.CANCELLING);
    }

    @Override
    public synchronized boolean markCancelled(UUID id, String workerId, Instant now) {
        return terminal(id, workerId, ImportStatus.CANCELLED, null, null, now, ImportStatus.PROCESSING, ImportStatus.CANCELLING);
    }

    private boolean terminal(UUID id, String workerId, ImportStatus to, String reason, String message, Instant now,
                             ImportStatus... from) {
        ImportJob j = rows.get(id);
        if (j == null || !j.isOwnedBy(workerId) || !List.of(from).contains(j.status())) {
            return false;
        }
        rows.put(id, with(j, to, j.storageKey(), j.fileSizeBytes(), j.contentSha256(), reason, message, null, null,
                j.cancelRequested(), j.attemptCount(), j.startedAt(), now, now));
        return true;
    }

    @Override
    public synchronized boolean release(UUID id, String workerId, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || !j.isOwnedBy(workerId) || j.status() != ImportStatus.PROCESSING) {
            return false;
        }
        rows.put(id, with(j, ImportStatus.PENDING, j.storageKey(), j.fileSizeBytes(), j.contentSha256(),
                j.failureReason(), j.failureMessage(), null, null, j.cancelRequested(), j.attemptCount(),
                j.startedAt(), j.completedAt(), now));
        return true;
    }

    @Override
    public synchronized long recordAttemptStart(UUID id, int attempt, String workerId, Instant now) {
        long rowId = nextAttemptId++;
        attempts.add(new Attempt(rowId, id, attempt, workerId, null, null));
        return rowId;
    }

    @Override
    public synchronized void recordAttemptEnd(long attemptRowId, String outcome, String failureMessage, Instant now) {
        for (int i = 0; i < attempts.size(); i++) {
            Attempt a = attempts.get(i);
            if (a.rowId() == attemptRowId) {
                attempts.set(i, new Attempt(a.rowId(), a.importId(), a.attempt(), a.workerId(), outcome, failureMessage));
            }
        }
    }

    @Override
    public synchronized Optional<ImportJob> retryFailed(UUID id, int maxRetries, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || j.status() != ImportStatus.FAILED || j.storageKey() == null || j.filePurgedAt() != null
                || j.retryCount() >= maxRetries) {
            return Optional.empty();
        }
        ImportJob retried = new ImportJob(j.id(), j.idempotencyKey(), j.providerId(), ImportStatus.PENDING,
                j.originalFilename(), j.storageKey(), j.fileSizeBytes(), j.contentSha256(), j.progress(),
                j.lastLineCommitted(), false, j.attemptCount(), null, null, null, null, j.createdAt(), now,
                j.startedAt(), null, j.retryCount() + 1, j.attemptCount(), null);
        rows.put(id, retried);
        return Optional.of(retried);
    }

    @Override
    public synchronized List<ImportJob> findPurgeable(Instant completedBefore, int limit) {
        return rows.values().stream()
                .filter(j -> j.status().isTerminal() && j.storageKey() != null && j.filePurgedAt() == null
                        && j.completedAt() != null && j.completedAt().isBefore(completedBefore))
                .sorted(java.util.Comparator.comparing(ImportJob::completedAt))
                .limit(limit)
                .toList();
    }

    @Override
    public synchronized boolean markFilePurged(UUID id, Instant now) {
        ImportJob j = rows.get(id);
        if (j == null || !j.status().isTerminal() || j.filePurgedAt() != null) {
            return false;
        }
        rows.put(id, new ImportJob(j.id(), j.idempotencyKey(), j.providerId(), j.status(), j.originalFilename(),
                j.storageKey(), j.fileSizeBytes(), j.contentSha256(), j.progress(), j.lastLineCommitted(),
                j.cancelRequested(), j.attemptCount(), j.leaseOwner(), j.leaseExpiresAt(), j.failureReason(),
                j.failureMessage(), j.createdAt(), now, j.startedAt(), j.completedAt(), j.retryCount(),
                j.attemptBase(), now));
        return true;
    }

    // ---- used by InMemoryBatchWriter ---------------------------------------

    synchronized ImportJob advance(UUID id, long lastLine, int accepted, int rejected, int duplicates, Instant now) {
        ImportJob j = get(id);
        ImportJob updated = new ImportJob(j.id(), j.idempotencyKey(), j.providerId(), j.status(), j.originalFilename(),
                j.storageKey(), j.fileSizeBytes(), j.contentSha256(), j.progress().plus(accepted, rejected, duplicates),
                lastLine, j.cancelRequested(), j.attemptCount(), j.leaseOwner(), j.leaseExpiresAt(), j.failureReason(),
                j.failureMessage(), j.createdAt(), now, j.startedAt(), j.completedAt(), j.retryCount(), j.attemptBase(),
                j.filePurgedAt());
        rows.put(id, updated);
        return updated;
    }

    private static ImportJob with(ImportJob j, ImportStatus status, String storageKey, Long size, String sha,
                                  String failureReason, String failureMessage, String leaseOwner, Instant leaseExpiresAt,
                                  boolean cancelRequested, int attemptCount, Instant startedAt, Instant completedAt,
                                  Instant updatedAt) {
        return new ImportJob(j.id(), j.idempotencyKey(), j.providerId(), status, j.originalFilename(), storageKey, size,
                sha, j.progress(), j.lastLineCommitted(), cancelRequested, attemptCount, leaseOwner, leaseExpiresAt,
                failureReason, failureMessage, j.createdAt(), updatedAt, startedAt, completedAt, j.retryCount(),
                j.attemptBase(), j.filePurgedAt());
    }
}
