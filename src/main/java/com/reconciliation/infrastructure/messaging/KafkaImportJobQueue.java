package com.reconciliation.infrastructure.messaging;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.infrastructure.persistence.JooqImportJobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka-backed dispatch with database leases.
 *
 * <p>{@link #enqueue} first commits the status change (so the import is durably
 * "pending" even if the broker is down), then publishes synchronously with a bounded
 * wait. If publishing fails the import stays pending and the periodic sweep
 * re-publishes it, so the broker being briefly unavailable delays work instead of
 * losing it. Events are keyed by import id; ordering across imports is irrelevant.
 */
public class KafkaImportJobQueue implements ImportJobQueue {

    private static final Logger log = LoggerFactory.getLogger(KafkaImportJobQueue.class);

    private final KafkaTemplate<String, ImportRequestedEvent> kafka;
    private final String topic;
    private final Duration publishTimeout;
    private final JooqImportJobStore store;
    private final ImportRepository imports;
    private final Clock clock;
    private final Duration leaseDuration;

    public KafkaImportJobQueue(KafkaTemplate<String, ImportRequestedEvent> kafka, String topic, Duration publishTimeout,
                               JooqImportJobStore store, ImportRepository imports, Clock clock, Duration leaseDuration) {
        this.kafka = kafka;
        this.topic = topic;
        this.publishTimeout = publishTimeout;
        this.store = store;
        this.imports = imports;
        this.clock = clock;
        this.leaseDuration = leaseDuration;
    }

    @Override
    public boolean enqueue(UUID importId) {
        if (!store.markPending(importId, clock.now())) {
            return false;
        }
        ImportJob job = imports.findById(importId).orElse(null);
        publish(importId, job == null ? "unknown" : job.providerId(), true);
        return true;
    }

    @Override
    public void redispatch(UUID importId) {
        ImportJob job = imports.findById(importId).orElse(null);
        if (job != null) {
            publish(importId, job.providerId(), false);
        }
    }

    private void publish(UUID importId, String providerId, boolean failLoud) {
        ImportRequestedEvent event = ImportRequestedEvent.of(importId, providerId, clock.now());
        try {
            kafka.send(topic, importId.toString(), event).get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AppException(ErrorCode.INTERNAL_ERROR, "Interrupted while publishing", e);
        } catch (ExecutionException | TimeoutException e) {
            // The row is already pending; the sweep will re-publish. Surface loudly on the
            // request path so the operator sees broker trouble, but the import is not lost.
            log.warn("Could not publish import event importId={} (will be re-dispatched by the sweep): {}",
                    importId, e.getCause() == null ? e.toString() : e.getCause().toString());
            if (failLoud) {
                throw new AppException(ErrorCode.QUEUE_UNAVAILABLE,
                        "Import accepted but queueing is delayed; it will start automatically", e,
                        Map.of("importId", importId.toString()));
            }
        }
    }

    @Override
    public int redispatchStale(Duration pendingOlderThan, int limit) {
        Instant now = clock.now();
        List<UUID> stale = store.findNeedingDispatch(now, now.minus(pendingOlderThan), limit);
        int published = 0;
        for (UUID id : stale) {
            try {
                redispatch(id);
                published++;
            } catch (RuntimeException e) {
                log.warn("Re-dispatch of import {} failed: {}", id, e.toString());
            }
        }
        if (published > 0) {
            log.info("Re-dispatched {} stale import(s)", published);
        }
        return published;
    }

    @Override
    public Optional<ImportJob> claim(UUID importId, String workerId) {
        return imports.claim(importId, workerId, leaseDuration, clock.now());
    }

    @Override
    public boolean heartbeat(UUID importId, String workerId) {
        return imports.extendLease(importId, workerId, leaseDuration, clock.now());
    }

    @Override
    public boolean release(UUID importId, String workerId) {
        return imports.release(importId, workerId, clock.now());
    }

    @Override
    public long queuedCount() {
        return store.queuedCount();
    }

    @Override
    public long inFlightCount() {
        return store.inFlightCount(clock.now());
    }
}
