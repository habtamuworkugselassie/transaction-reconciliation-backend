package com.reconciliation.testing;

import com.reconciliation.application.port.Clock;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/** A clock the test moves by hand, so lease expiry and timestamps are deterministic. */
public final class FixedClock implements Clock {

    public static final Instant START = Instant.parse("2026-07-24T08:00:00Z");

    private final AtomicReference<Instant> now;

    public FixedClock() {
        this(START);
    }

    public FixedClock(Instant start) {
        this.now = new AtomicReference<>(start);
    }

    @Override
    public Instant now() {
        return now.get();
    }

    public void advance(Duration d) {
        now.updateAndGet(t -> t.plus(d));
    }

    public void set(Instant t) {
        now.set(t);
    }
}
