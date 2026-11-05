package com.reconciliation.infrastructure.health;

import com.reconciliation.infrastructure.worker.ImportWorker;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Readiness contribution: DOWN once worker shutdown has begun. On an API-only
 * instance ({@code worker == null}) it reports the role and is always UP.
 */
public final class WorkerHealthIndicator implements HealthIndicator {

    private final ImportWorker worker;

    public WorkerHealthIndicator(ImportWorker worker) {
        this.worker = worker;
    }

    @Override
    public Health health() {
        if (worker == null) {
            return Health.up().withDetail("role", "api-only").build();
        }
        Health.Builder b = worker.isShuttingDown() ? Health.down().withDetail("reason", "shutting down")
                : worker.isRunning() ? Health.up() : Health.down().withDetail("reason", "not started");
        return b.withDetail("workerId", worker.workerId())
                .withDetail("activeImports", worker.activeImports())
                .withDetail("capacity", worker.capacity())
                .build();
    }
}
