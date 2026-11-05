package com.reconciliation.application.port;

import java.util.UUID;

/** Generates identifiers for imports and jobs. Injected so tests can be deterministic. */
@FunctionalInterface
public interface IdGenerator {
    UUID newId();
}
