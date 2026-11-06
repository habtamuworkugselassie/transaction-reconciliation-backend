package com.reconciliation.infrastructure.worker;

import com.reconciliation.application.usecase.PurgeExpiredUploadsUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs {@link PurgeExpiredUploadsUseCase} every {@code interval} on one background thread
 * (worker role, only when retention is enabled). A failing run is logged and the next one
 * tries again; runs never overlap. Stops with the worker, before the database pool closes.
 */
public final class RetentionScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RetentionScheduler.class);

    private final PurgeExpiredUploadsUseCase purge;
    private final Duration interval;
    private ScheduledExecutorService executor;
    private volatile boolean running;

    public RetentionScheduler(PurgeExpiredUploadsUseCase purge, Duration interval) {
        this.purge = purge;
        this.interval = interval;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retention");
            t.setDaemon(true);
            return t;
        });
        long ms = interval.toMillis();
        // First run after one minute (or the interval, if shorter), not at startup, so a fleet
        // restart does not run retention on every instance at the same moment.
        executor.scheduleWithFixedDelay(this::runOnce, Math.min(ms, 60_000), ms, TimeUnit.MILLISECONDS);
        running = true;
        log.info("Retention enabled: every {}", interval);
    }

    void runOnce() {
        try {
            purge.execute();
        } catch (RuntimeException e) {
            log.warn("Retention run failed; will retry next interval: {}", e.toString());
        }
    }

    @Override
    public synchronized void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return ImportWorker.PHASE;
    }
}
