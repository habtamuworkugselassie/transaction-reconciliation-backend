package com.reconciliation.application.processing;

/** How one processing attempt ended. Mirrors the import_attempts.outcome column. */
public enum ProcessingOutcome {
    COMPLETED,
    CANCELLED,
    FAILED,
    /** Transient failure or shutdown: returned to the queue for another attempt. */
    RELEASED,
    /** Another worker took the lease; this attempt wrote nothing further and changed no state. */
    ABANDONED;

    public String wireName() {
        return name().toLowerCase();
    }
}
