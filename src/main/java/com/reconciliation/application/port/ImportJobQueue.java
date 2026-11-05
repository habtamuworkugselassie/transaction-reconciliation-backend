package com.reconciliation.application.port;

import com.reconciliation.domain.model.ImportJob;

import java.util.Optional;
import java.util.UUID;

/**
 * Dispatch of imports to workers (ADR-001).
 *
 * <p>Two mechanisms cooperate. A message broker (Kafka) carries "import requested"
 * events so workers learn about work immediately and the load fans out across a
 * consumer group. The database row is the source of truth for ownership: a worker
 * that receives an event must {@link #claim} the import, and only the claim's
 * conditional update decides who processes it. Re-delivered or duplicated events are
 * therefore harmless, and the lease makes abandoned work recoverable.
 */
public interface ImportJobQueue {

    /** uploading -> pending and publish the event. Returns false if the import was not uploading. */
    boolean enqueue(UUID importId);

    /** Re-publish the event for an import that is pending or whose lease expired (recovery). */
    void redispatch(UUID importId);

    /**
     * Recovery sweep: re-publish every import that is still pending after
     * {@code pendingOlderThan} (event lost, or no consumer was running) or whose lease
     * has expired (worker died). Returns how many were re-published.
     */
    int redispatchStale(java.time.Duration pendingOlderThan, int limit);

    /** Take ownership of an import; empty if someone else holds it or it is finished. */
    Optional<ImportJob> claim(UUID importId, String workerId);

    /** Renew the lease. False means the lease is gone: the worker must stop. */
    boolean heartbeat(UUID importId, String workerId);

    /** Give a processing import back to the queue (transient failure, shutdown). */
    boolean release(UUID importId, String workerId);

    /** Imports waiting to be claimed; exposed as a metric. */
    long queuedCount();

    /** Imports currently leased; exposed as a metric. */
    long inFlightCount();
}
