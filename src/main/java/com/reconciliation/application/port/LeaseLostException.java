package com.reconciliation.application.port;

import java.util.UUID;

/** The worker's lease on an import was taken over or revoked. Never retried. */
public class LeaseLostException extends RuntimeException {
    public LeaseLostException(UUID importId, String workerId) {
        super("Lease on import " + importId + " is no longer held by " + workerId, null, false, false);
    }
}
