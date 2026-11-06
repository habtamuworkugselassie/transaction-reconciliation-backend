package com.reconciliation.api;

import com.reconciliation.api.dto.ImportStatusResponse;
import com.reconciliation.application.usecase.GetImportUseCase;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server-sent progress events for {@code GET /v1/imports/{id}/events}.
 *
 * <p>The stream reports exactly what {@code GET /v1/imports/{id}} would: the persisted
 * status and counters. It works by polling the database, not by pushing from the worker.
 * The API and the worker are separate processes, and the persisted counters are the only
 * progress that is guaranteed to be true. A push channel would need a broker fan-out and
 * could still show progress that was later rolled back.
 *
 * <p>Bounded resources:
 * <ul>
 *   <li>at most {@code maxConnections} open streams per instance; the next request gets 429
 *       with {@code Retry-After};</li>
 *   <li>one scheduler thread polls every {@code pollInterval}, with <b>one read per import</b>,
 *       however many clients watch it, so database load is bounded by the number of
 *       distinct imports being watched;</li>
 *   <li>an event is sent only when the status or a counter changed, otherwise a comment
 *       line every {@code heartbeatInterval} keeps proxies from closing an idle connection;</li>
 *   <li>a stream ends when the import is terminal, after {@code timeout} (the client
 *       reconnects), when the client goes away, or at shutdown.</li>
 * </ul>
 *
 * <p>At shutdown every stream is completed as soon as the context starts closing, so open
 * streams never hold up the web server's graceful drain.
 */
public final class ImportProgressStreamer implements ApplicationListener<ContextClosedEvent>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ImportProgressStreamer.class);

    private final GetImportUseCase getImport;
    private final int maxConnections;
    private final Duration heartbeatInterval;
    private final Duration timeout;
    private final ScheduledExecutorService poller;

    private final Map<UUID, Set<Stream>> streamsByImport = new ConcurrentHashMap<>();
    private final AtomicInteger open = new AtomicInteger();
    private volatile boolean closing;

    private final class Stream {
        final UUID importId;
        final SseEmitter emitter;
        final AtomicBoolean closed = new AtomicBoolean();
        volatile ImportStatusResponse lastSent;
        volatile long lastSentAtNanos = System.nanoTime();

        Stream(UUID importId, SseEmitter emitter) {
            this.importId = importId;
            this.emitter = emitter;
        }

        /** Unregisters exactly once, whichever of completion, timeout, error or shutdown comes first. */
        void unregister() {
            if (closed.compareAndSet(false, true)) {
                open.decrementAndGet();
                streamsByImport.computeIfPresent(importId, (id, set) -> {
                    set.remove(this);
                    return set.isEmpty() ? null : set;
                });
            }
        }
    }

    public ImportProgressStreamer(GetImportUseCase getImport, int maxConnections, Duration pollInterval,
                                  Duration heartbeatInterval, Duration timeout) {
        this.getImport = getImport;
        this.maxConnections = maxConnections;
        this.heartbeatInterval = heartbeatInterval;
        this.timeout = timeout;
        this.poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sse-progress-poller");
            t.setDaemon(true);
            return t;
        });
        long pollMs = Math.max(50, pollInterval.toMillis());
        poller.scheduleWithFixedDelay(this::tick, pollMs, pollMs, TimeUnit.MILLISECONDS);
    }

    /** Opens a stream for an import the caller has already loaded (and authorised). */
    public SseEmitter open(ImportJob job) {
        if (closing) {
            throw new AppException(ErrorCode.SERVICE_SHUTTING_DOWN, "The service is shutting down; reconnect shortly");
        }
        if (open.incrementAndGet() > maxConnections) {
            open.decrementAndGet();
            throw new AppException(ErrorCode.TOO_MANY_EVENT_STREAMS, "Too many open progress streams; retry shortly",
                    null, Map.of("capacity", maxConnections, "retryAfterSeconds", 5));
        }
        SseEmitter emitter = new SseEmitter(timeout.toMillis());
        Stream stream = new Stream(job.id(), emitter);
        emitter.onCompletion(stream::unregister);
        emitter.onError(e -> stream.unregister());
        emitter.onTimeout(() -> {
            stream.unregister();
            emitter.complete();
        });
        streamsByImport.computeIfAbsent(job.id(), id -> ConcurrentHashMap.newKeySet()).add(stream);
        publish(stream, ImportStatusResponse.from(job), job.status().isTerminal());
        return emitter;
    }

    public int openStreams() {
        return open.get();
    }

    /** One read per watched import, then fan out to its streams. */
    void tick() {
        for (UUID importId : streamsByImport.keySet()) {
            Set<Stream> streams = streamsByImport.get(importId);
            if (streams == null || streams.isEmpty()) {
                continue;
            }
            ImportJob job;
            try {
                job = getImport.execute(importId);
            } catch (AppException e) {
                if (e.code() == ErrorCode.IMPORT_NOT_FOUND) {
                    streams.forEach(s -> finish(s));
                }
                continue; // a transient read failure: try again next tick
            } catch (RuntimeException e) {
                log.warn("Progress poll failed for import {}: {}", importId, e.toString());
                continue;
            }
            ImportStatusResponse current = ImportStatusResponse.from(job);
            boolean terminal = job.status().isTerminal();
            for (Stream s : streams) {
                if (!current.equals(s.lastSent)) {
                    publish(s, current, terminal);
                } else if (System.nanoTime() - s.lastSentAtNanos >= heartbeatInterval.toNanos()) {
                    heartbeat(s);
                }
            }
        }
    }

    private void publish(Stream s, ImportStatusResponse status, boolean terminal) {
        try {
            s.emitter.send(SseEmitter.event()
                    .name("progress")
                    .id(String.valueOf(status.progress().processed()))
                    .data(status, MediaType.APPLICATION_JSON));
            s.lastSent = status;
            s.lastSentAtNanos = System.nanoTime();
            if (terminal) {
                finish(s);
            }
        } catch (IOException | IllegalStateException e) {
            s.unregister(); // client went away; the emitter's error callback completes it
        }
    }

    private void heartbeat(Stream s) {
        try {
            s.emitter.send(SseEmitter.event().comment("keep-alive"));
            s.lastSentAtNanos = System.nanoTime();
        } catch (IOException | IllegalStateException e) {
            s.unregister();
        }
    }

    private void finish(Stream s) {
        s.unregister();
        try {
            s.emitter.complete();
        } catch (RuntimeException ignored) {
            // already completed
        }
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        close();
    }

    @Override
    public void close() {
        if (closing) {
            return;
        }
        closing = true;
        poller.shutdownNow();
        streamsByImport.values().forEach(set -> set.forEach(this::finish));
        log.info("Progress streams closed for shutdown");
    }
}
