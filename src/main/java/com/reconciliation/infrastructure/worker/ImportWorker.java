package com.reconciliation.infrastructure.worker;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.processing.CancellationToken;
import com.reconciliation.application.processing.ProcessingOutcome;
import com.reconciliation.application.usecase.ProcessImportUseCase;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.infrastructure.messaging.ImportRequestedEvent;
import com.reconciliation.infrastructure.messaging.InvalidImportEventException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The worker role: a Kafka consumer group member that turns "import requested" events
 * into processing attempts, heartbeats the database lease while it works, and a
 * housekeeping thread that re-dispatches stale work and reaps abandoned uploads.
 *
 * <p>Concurrency is bounded by the listener container ({@code maxConcurrentImports}
 * consumer threads, one record per poll), each import by {@code maxInFlightBatches},
 * and scoring by the CPU pool. There is no per-record thread or task.
 *
 * <p>Ownership is decided by the database, not by Kafka: every event leads to a
 * conditional claim, so duplicated or re-delivered events are no-ops when another
 * worker holds a live lease. Offsets are acknowledged only once an attempt has reached
 * a terminal outcome; a released attempt is negatively acknowledged with a delay so the
 * same partition redelivers it, and a shutdown leaves the offset uncommitted so the
 * next consumer gets it.
 *
 * <p>Shutdown ({@link #stop()}): stop the container (no new records), signal active
 * imports with {@code SHUTDOWN} (they finish in-flight batches and release the job),
 * wait up to the grace period, then force-release whatever remains. The lease check
 * inside every batch write guarantees a straggler cannot commit after release.
 */
public final class ImportWorker implements SmartLifecycle, AcknowledgingMessageListener<String, ImportRequestedEvent> {

    private static final Logger log = LoggerFactory.getLogger(ImportWorker.class);

    /** Stop after the web server has drained (its phase is DEFAULT_PHASE - 1024). */
    public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 2048;

    private record Active(ImportJob job, CancellationToken token, ScheduledFuture<?> heartbeat) {}

    private final ImportJobQueue queue;
    private final ImportRepository imports;
    private final ProcessImportUseCase processImport;
    private final Clock clock;
    private final int maxConcurrentImports;
    private final Duration heartbeatInterval;
    private final Duration staleUploadTimeout;
    private final Duration redispatchInterval;
    private final Duration redeliveryDelay;
    private final Duration gracePeriod;
    private final String workerId;

    private final Map<UUID, Active> active = new ConcurrentHashMap<>();
    private final AtomicInteger activeCount = new AtomicInteger();
    private MessageListenerContainer container;
    private volatile boolean running;
    private volatile boolean shuttingDown;
    private ScheduledExecutorService housekeeping;

    public ImportWorker(ImportJobQueue queue, ImportRepository imports, ProcessImportUseCase processImport, Clock clock,
                        int maxConcurrentImports, Duration heartbeatInterval, Duration staleUploadTimeout,
                        Duration redispatchInterval, Duration redeliveryDelay, Duration gracePeriod) {
        this.queue = queue;
        this.imports = imports;
        this.processImport = processImport;
        this.clock = clock;
        this.maxConcurrentImports = maxConcurrentImports;
        this.heartbeatInterval = heartbeatInterval;
        this.staleUploadTimeout = staleUploadTimeout;
        this.redispatchInterval = redispatchInterval;
        this.redeliveryDelay = redeliveryDelay;
        this.gracePeriod = gracePeriod;
        this.workerId = hostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /** Called by the composition root; the container is created there with this worker as listener. */
    public void attach(MessageListenerContainer container) {
        this.container = container;
    }

    // ---- lifecycle -----------------------------------------------------------

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        shuttingDown = false;
        housekeeping = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "import-housekeeping");
            t.setDaemon(true);
            return t;
        });
        housekeeping.scheduleAtFixedRate(this::reapStaleUploads, 30, 60, TimeUnit.SECONDS);
        housekeeping.scheduleAtFixedRate(this::redispatchStale, redispatchInterval.toSeconds(),
                redispatchInterval.toSeconds(), TimeUnit.SECONDS);
        if (container != null && !container.isRunning()) {
            container.start();
        }
        log.info("Import worker started workerId={} maxConcurrentImports={}", workerId, maxConcurrentImports);
    }

    @Override
    public void stop() {
        signalShutdown();
        long deadline = System.nanoTime() + gracePeriod.toNanos();
        if (container != null) {
            container.stop(); // waits for listener threads up to the container's shutdown timeout
        }
        while (activeCount.get() > 0 && System.nanoTime() < deadline) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (activeCount.get() > 0) {
            log.warn("Grace period of {} expired with {} import(s) still active; releasing them", gracePeriod, activeCount.get());
        }
        active.forEach((id, a) -> {
            a.heartbeat().cancel(false);
            boolean released = queue.release(id, workerId);
            log.warn("Forced release of import {} after grace period: released={}", id, released);
        });
        active.clear();
        housekeeping.shutdownNow();
        running = false;
        MDC.remove("shutdown");
        log.info("Import worker stopped workerId={}", workerId);
    }

    /** Stops taking new records and tells active imports to wind down. Idempotent, safe to call early. */
    public void signalShutdown() {
        if (shuttingDown) {
            return;
        }
        shuttingDown = true;
        MDC.put("shutdown", "true");
        log.info("Shutdown requested: no new imports will be claimed; {} active", activeCount.get());
        if (container != null) {
            container.pause(); // stop fetching; the current record (if any) keeps running
        }
        active.values().forEach(a -> a.token().cancel(CancellationToken.Reason.SHUTDOWN));
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    // ---- Kafka listener ------------------------------------------------------

    @Override
    public void onMessage(ConsumerRecord<String, ImportRequestedEvent> record, Acknowledgment ack) {
        ImportRequestedEvent event = record.value();
        if (event == null || event.importId() == null) {
            // Not retryable and not ours to drop silently: the container's error handler
            // publishes it to the dead-letter topic and commits the offset.
            log.warn("Malformed import event at {}-{}@{}; sending to dead-letter topic", record.topic(),
                    record.partition(), record.offset());
            throw new InvalidImportEventException("Import event without importId at " + record.topic() + "-"
                    + record.partition() + "@" + record.offset());
        }
        if (shuttingDown) {
            return; // not acknowledged: redelivered to the next consumer
        }
        UUID id = event.importId();
        Optional<ImportJob> claimed;
        try {
            claimed = queue.claim(id, workerId);
        } catch (RuntimeException e) {
            // Database unavailable: leave the record unacknowledged and back off; the sweep
            // and redelivery will bring it back.
            log.warn("Claim failed for import {}; will retry after {}: {}", id, redeliveryDelay, e.toString());
            ack.nack(redeliveryDelay);
            return;
        }
        if (claimed.isEmpty()) {
            log.debug("Import {} not claimable (owned elsewhere or finished); acknowledging", id);
            ack.acknowledge();
            return;
        }
        ProcessingOutcome outcome = runImport(claimed.get());
        if (outcome == ProcessingOutcome.RELEASED) {
            if (shuttingDown) {
                return; // uncommitted offset: redelivered after rebalance
            }
            ack.nack(redeliveryDelay);
        } else {
            ack.acknowledge();
        }
    }

    private ProcessingOutcome runImport(ImportJob job) {
        CancellationToken token = new CancellationToken();
        if (shuttingDown) {
            token.cancel(CancellationToken.Reason.SHUTDOWN);
        }
        ScheduledFuture<?> heartbeat = housekeeping.scheduleAtFixedRate(() -> heartbeat(job.id(), token),
                heartbeatInterval.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
        active.put(job.id(), new Active(job, token, heartbeat));
        activeCount.incrementAndGet();
        MDC.put("workerId", workerId);
        long attemptRow = -1;
        ProcessingOutcome outcome = ProcessingOutcome.ABANDONED;
        String failure = null;
        try {
            attemptRow = imports.recordAttemptStart(job.id(), job.attemptCount(), workerId, clock.now());
            log.info("Processing import importId={} attempt={} resumeAfterLine={}", job.id(), job.attemptCount(),
                    job.lastLineCommitted());
            outcome = processImport.execute(job, workerId, token);
        } catch (Throwable t) {
            failure = t.getClass().getSimpleName();
            log.error("Unexpected failure processing import {}", job.id(), t);
            if (!imports.fail(job.id(), workerId, "PROCESSING_FAILED", failure, clock.now())) {
                queue.release(job.id(), workerId);
            }
            outcome = ProcessingOutcome.FAILED;
        } finally {
            heartbeat.cancel(false);
            active.remove(job.id());
            activeCount.decrementAndGet();
            if (attemptRow >= 0) {
                try {
                    imports.recordAttemptEnd(attemptRow, outcome.wireName(), failure, clock.now());
                } catch (RuntimeException e) {
                    log.warn("Could not record attempt end for import {}: {}", job.id(), e.toString());
                }
            }
            MDC.remove("workerId");
        }
        return outcome;
    }

    private void heartbeat(UUID importId, CancellationToken token) {
        try {
            if (!queue.heartbeat(importId, workerId)) {
                log.warn("Lease on import {} is gone; cancelling local work", importId);
                token.cancel(CancellationToken.Reason.LEASE_LOST);
            }
        } catch (RuntimeException e) {
            log.warn("Heartbeat failed for import {}: {}", importId, e.toString());
        }
    }

    private void reapStaleUploads() {
        try {
            int n = imports.failStaleUploads(clock.now().minus(staleUploadTimeout), clock.now());
            if (n > 0) {
                log.info("Marked {} stale upload(s) as failed", n);
            }
        } catch (RuntimeException e) {
            log.warn("Stale upload reaper failed: {}", e.toString());
        }
    }

    private void redispatchStale() {
        if (shuttingDown) {
            return;
        }
        try {
            queue.redispatchStale(redispatchInterval.multipliedBy(2), 100);
        } catch (RuntimeException e) {
            log.warn("Re-dispatch sweep failed: {}", e.toString());
        }
    }

    // ---- introspection for metrics / health --------------------------------------

    public int activeImports() {
        return activeCount.get();
    }

    public int capacity() {
        return maxConcurrentImports;
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }

    public String workerId() {
        return workerId;
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "worker";
        }
    }
}
