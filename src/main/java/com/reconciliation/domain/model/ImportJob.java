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
        Instant completedAt
) {

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
