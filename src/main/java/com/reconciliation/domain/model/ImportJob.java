package com.reconciliation.domain.model;

import java.time.Instant;
import java.util.UUID;

/**
 * An import as persisted: upload metadata, processing progress and job-control state.
 * Immutable; the repository returns a fresh snapshot after each state change.
 */
public record ImportJob(
        UUID id,
        String idempotencyKey,
        String providerId,
        ImportStatus status,
        String originalFilename,
        String storageKey,
        Long fileSizeBytes,
        String contentSha256,
        ImportProgress progress,
        long lastLineCommitted,
        boolean cancelRequested,
        int attemptCount,
        String leaseOwner,
        Instant leaseExpiresAt,
        String failureReason,
        String failureMessage,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt,
        /** Times a client re-queued this import after it failed (POST .../retry). */
        int retryCount,
        /** attemptCount at the last manual retry; the automatic attempt budget counts from here. */
        int attemptBase,
        /** When the retention job deleted the uploaded file; null while the file exists. */
        Instant filePurgedAt
) {

    /** An import that was never retried and whose file has not been purged. */
    public ImportJob(UUID id, String idempotencyKey, String providerId, ImportStatus status, String originalFilename,
                     String storageKey, Long fileSizeBytes, String contentSha256, ImportProgress progress,
                     long lastLineCommitted, boolean cancelRequested, int attemptCount, String leaseOwner,
                     Instant leaseExpiresAt, String failureReason, String failureMessage, Instant createdAt,
                     Instant updatedAt, Instant startedAt, Instant completedAt) {
        this(id, idempotencyKey, providerId, status, originalFilename, storageKey, fileSizeBytes, contentSha256,
                progress, lastLineCommitted, cancelRequested, attemptCount, leaseOwner, leaseExpiresAt, failureReason,
                failureMessage, createdAt, updatedAt, startedAt, completedAt, 0, 0, null);
    }

    /** Processing attempts since the import was created or last manually retried. */
    public int attemptsSinceRetry() {
        return attemptCount - attemptBase;
    }

    /** A freshly reserved import: key claimed, body not yet stored. */
    public static ImportJob reserved(UUID id, String idempotencyKey, String providerId,
                                     String originalFilename, Instant now) {
        return new ImportJob(id, idempotencyKey, providerId, ImportStatus.UPLOADING, originalFilename,
                null, null, null, ImportProgress.ZERO, 0, false, 0, null, null, null, null,
                now, now, null, null);
    }

    public boolean isOwnedBy(String workerId) {
        return workerId != null && workerId.equals(leaseOwner);
    }
}
