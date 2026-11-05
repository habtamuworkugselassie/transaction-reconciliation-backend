package com.reconciliation.infrastructure.health;

import com.reconciliation.application.port.FileStorage;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

/** Readiness contribution: upload storage must be writable and have head-room. */
public final class StorageHealthIndicator implements HealthIndicator {

    private final FileStorage storage;
    private final Path root;
    private final long thresholdBytes;

    public StorageHealthIndicator(FileStorage storage, Path root, long thresholdBytes) {
        this.storage = storage;
        this.root = root;
        this.thresholdBytes = thresholdBytes;
    }

    @Override
    public Health health() {
        try {
            if (!storage.isWritable()) {
                return Health.down().withDetail("reason", "storage root not writable").build();
            }
            FileStore store = Files.getFileStore(root);
            long free = store.getUsableSpace();
            Health.Builder b = free >= thresholdBytes ? Health.up() : Health.down().withDetail("reason", "low disk space");
            return b.withDetail("freeBytes", free).withDetail("thresholdBytes", thresholdBytes).build();
        } catch (IOException e) {
            return Health.down().withDetail("reason", e.getClass().getSimpleName()).build();
        }
    }
}
