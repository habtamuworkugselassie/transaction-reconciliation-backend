package com.reconciliation.infrastructure.time;

import com.reconciliation.application.port.IdGenerator;

import java.util.UUID;

public final class RandomUuidGenerator implements IdGenerator {
    @Override
    public UUID newId() {
        return UUID.randomUUID();
    }
}
