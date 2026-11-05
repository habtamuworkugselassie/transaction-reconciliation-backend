package com.reconciliation.infrastructure.time;

import com.reconciliation.application.port.Clock;

import java.time.Instant;

public final class SystemClock implements Clock {
    @Override
    public Instant now() {
        return Instant.now();
    }
}
