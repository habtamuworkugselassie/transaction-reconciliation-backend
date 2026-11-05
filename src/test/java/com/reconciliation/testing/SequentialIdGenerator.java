package com.reconciliation.testing;

import com.reconciliation.application.port.IdGenerator;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Predictable ids: 00000000-0000-0000-0000-000000000001, ...2, ... */
public final class SequentialIdGenerator implements IdGenerator {

    private final AtomicLong next = new AtomicLong(1);

    @Override
    public UUID newId() {
        return new UUID(0L, next.getAndIncrement());
    }

    public static UUID id(long n) {
        return new UUID(0L, n);
    }
}
