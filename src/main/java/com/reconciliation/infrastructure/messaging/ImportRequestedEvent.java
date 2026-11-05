package com.reconciliation.infrastructure.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Published when an import is ready to be processed. Deliberately tiny: the
 * database row holds the state, the event only says "look at this id". Version
 * field allows the schema to evolve without breaking in-flight consumers.
 */
public record ImportRequestedEvent(int version, UUID importId, String providerId, Instant requestedAt) {

    public static final int CURRENT_VERSION = 1;

    public static ImportRequestedEvent of(UUID importId, String providerId, Instant now) {
        return new ImportRequestedEvent(CURRENT_VERSION, importId, providerId, now);
    }
}
