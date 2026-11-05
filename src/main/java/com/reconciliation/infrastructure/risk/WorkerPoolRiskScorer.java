package com.reconciliation.infrastructure.risk;

import com.reconciliation.application.port.RiskScorer;
import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.domain.processing.RiskScoreCalculator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Risk scoring on a fixed pool of platform threads, sized to the CPU count.
 *
 * <p>Why a dedicated platform-thread pool and not virtual threads or the request
 * threads: scoring is pure CPU work. Virtual threads give no benefit for CPU-bound
 * tasks (they still need a carrier), and running it on Tomcat's threads would make
 * API latency track import throughput. A pool of {@code cores} threads means the
 * scorer can never use more than the machine's CPUs, leaving the scheduler free to
 * serve API requests promptly.
 *
 * <p>Bounded queue: when the queue is full, submission blocks the producer (the
 * import's parsing thread) instead of growing memory or throwing. That is the
 * back-pressure path from CPU saturation back to file reading.
 */
public final class WorkerPoolRiskScorer implements RiskScorer, AutoCloseable {

    private static final int CHUNK = 100;

    private final ThreadPoolExecutor pool;
    private final RiskScoreCalculator calculator;

    public WorkerPoolRiskScorer(int threads, int queueCapacity, RiskScoreCalculator calculator) {
        this.calculator = calculator;
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "risk-scorer-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.pool = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity), factory, new BlockingSubmit());
    }

    @Override
    public CompletableFuture<List<ScoredTransaction>> scoreAsync(List<NormalizedTransaction> batch) {
        // Split into chunks so a 1000-record batch spreads over all threads instead of
        // occupying one of them for the whole batch.
        int chunks = (batch.size() + CHUNK - 1) / CHUNK;
        @SuppressWarnings("unchecked")
        CompletableFuture<List<ScoredTransaction>>[] parts = new CompletableFuture[chunks];
        for (int c = 0; c < chunks; c++) {
            List<NormalizedTransaction> slice = batch.subList(c * CHUNK, Math.min(batch.size(), (c + 1) * CHUNK));
            parts[c] = CompletableFuture.supplyAsync(() -> scoreChunk(slice), pool);
        }
        return CompletableFuture.allOf(parts).thenApply(ignored -> {
            List<ScoredTransaction> out = new ArrayList<>(batch.size());
            Arrays.stream(parts).forEach(p -> out.addAll(p.join()));
            return out;
        });
    }

    private List<ScoredTransaction> scoreChunk(List<NormalizedTransaction> slice) {
        List<ScoredTransaction> out = new ArrayList<>(slice.size());
        for (NormalizedTransaction t : slice) {
            out.add(calculator.score(t));
        }
        return out;
    }

    @Override
    public int queueDepth() {
        return pool.getQueue().size();
    }

    @Override
    public int activeWorkers() {
        return pool.getActiveCount();
    }

    public int poolSize() {
        return pool.getCorePoolSize();
    }

    @Override
    public void close() {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
        }
    }

    /** Rejection handler that blocks the submitter until queue space frees up. */
    private static final class BlockingSubmit implements java.util.concurrent.RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("Risk scorer is shut down");
            }
            try {
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("Interrupted while waiting for scorer capacity", e);
            }
        }
    }
}
