package com.reconciliation.application.port;

import com.reconciliation.domain.model.ImportJob;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Persistence of imports and their job-control state.
 *
 * <p>Every mutating method is a conditional state transition that returns whether it
 * applied. Callers treat {@code false} as "someone else got there first" and re-read
 * the row rather than assuming their view is current. This is how two workers, an
 * API cancel request and a shutdown can all touch the same import safely.
 */
public interface ImportRepository {

    /** Outcome of reserving an idempotency key. */
    sealed interface Reservation {
        ImportJob job();

        /** The key was free; the given job row now exists in {@code uploading} state. */
        record Created(ImportJob job) implements Reservation {}

        /** The key was already used; {@code job} is the existing import (any state). */
        record Existing(ImportJob job) implements Reservation {}
    }

    /**
     * Atomically reserve {@code job.idempotencyKey()}. Concurrent calls with the same key
     * all observe the same winner because the unique constraint serialises them.
     */
    Reservation reserve(ImportJob job);

    /**
     * Re-claim a key whose import failed before a complete file was stored (interrupted,
     * too large, wrong type, empty), so a client may retry the same request. Returns the
     * re-opened job, or empty if the import is in any other state.
     */
    Optional<ImportJob> reopenFailedUpload(UUID id, Instant now);

    Optional<ImportJob> findById(UUID id);

    /** uploading -> pending with the stored file's metadata. */
    boolean markUploaded(UUID id, String storageKey, long sizeBytes, String sha256, Instant now);

    /** uploading -> failed. */
    boolean markUploadFailed(UUID id, String reason, String message, Instant now);

    /** Mark imports stuck in {@code uploading} since before {@code olderThan} as failed. */
    int failStaleUploads(Instant olderThan, Instant now);

    // ---- job control -------------------------------------------------------

    /**
     * Claim a specific import for {@code workerId} if it is claimable: {@code pending}, or
     * {@code processing}/{@code cancelling} with an expired lease (abandoned by a crashed
     * worker). Returns empty when another worker holds a live lease or the import is
     * terminal, which is how a re-delivered queue message becomes a harmless no-op.
     */
    Optional<ImportJob> claim(UUID id, String workerId, Duration leaseDuration, Instant now);

    /**
     * Claim the oldest claimable import regardless of id. Used only as a safety net when
     * the message queue is unavailable; uses row-level locking so concurrent workers
     * never claim the same row.
     */
    Optional<ImportJob> claimNext(String workerId, Duration leaseDuration, Instant now);

    /** Heartbeat. Returns false if the lease is no longer ours (then stop working). */
    boolean extendLease(UUID id, String workerId, Duration leaseDuration, Instant now);

    /**
     * Request cancellation. pending -> cancelled immediately (no worker holds it);
     * processing -> cancelling and sets {@code cancel_requested} for the worker to observe.
     * Returns the resulting job, or empty if the import is not cancellable.
     */
    Optional<ImportJob> requestCancel(UUID id, Instant now);

    /** Cheap poll used by the worker between batches. */
    boolean isCancelRequested(UUID id);

    /** processing -> completed (owner only). */
    boolean complete(UUID id, String workerId, Instant now);

    /** processing|cancelling -> failed (owner only). */
    boolean fail(UUID id, String workerId, String reason, String message, Instant now);

    /** cancelling|processing -> cancelled (owner only). */
    boolean markCancelled(UUID id, String workerId, Instant now);

    /** processing -> pending, releasing the lease so another attempt can run (owner only). */
    boolean release(UUID id, String workerId, Instant now);

    /**
     * failed -> pending for a client-requested retry, keeping counters and the checkpoint
     * so processing resumes where it stopped. Applies only if the file was stored completely
     * ({@code storage_key} set), has not been purged by retention, and fewer than
     * {@code maxRetries} retries have been made. Increments {@code retry_count} and moves
     * {@code attempt_base} to the current {@code attempt_count}, which gives the import a
     * fresh automatic-attempt budget. Returns the re-queued job, or empty if not applicable.
     */
    Optional<ImportJob> retryFailed(UUID id, int maxRetries, Instant now);

    // ---- retention ---------------------------------------------------------

    /** Terminal imports completed before {@code completedBefore} whose file is still stored, oldest first. */
    List<ImportJob> findPurgeable(Instant completedBefore, int limit);

    /**
     * Records that the uploaded file is being deleted. Conditional on the import being
     * terminal and not yet purged, which makes it mutually exclusive with
     * {@link #retryFailed}: whichever commits first wins.
     */
    boolean markFilePurged(UUID id, Instant now);

    long recordAttemptStart(UUID id, int attempt, String workerId, Instant now);

    void recordAttemptEnd(long attemptRowId, String outcome, String failureMessage, Instant now);
}
