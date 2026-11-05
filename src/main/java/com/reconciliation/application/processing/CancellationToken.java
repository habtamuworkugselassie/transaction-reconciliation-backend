package com.reconciliation.application.processing;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Cooperative cancellation shared between the worker loop (shutdown, heartbeat) and
 * the processing pipeline. The reason determines what happens to the import:
 * a user cancel ends it as {@code cancelled}; a shutdown or lost lease releases it
 * so another worker resumes from the checkpoint.
 */
public final class CancellationToken {

    public enum Reason { CANCEL_REQUESTED, SHUTDOWN, LEASE_LOST }

    private final AtomicReference<Reason> reason = new AtomicReference<>();

    public boolean isCancelled() {
        return reason.get() != null;
    }

    public Reason reason() {
        return reason.get();
    }

    /** First reason wins; later calls are ignored. */
    public void cancel(Reason why) {
        reason.compareAndSet(null, why);
    }
}
