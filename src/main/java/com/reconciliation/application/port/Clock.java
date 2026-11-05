package com.reconciliation.application.port;

import java.time.Instant;

/** Source of time, injected so use cases and status transitions are testable. */
@FunctionalInterface
public interface Clock {
    Instant now();
}
