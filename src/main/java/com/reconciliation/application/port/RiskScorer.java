package com.reconciliation.application.port;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.ScoredTransaction;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * CPU-bound scoring, executed off the calling thread on a bounded pool. The future
 * completes with results in the same order as the input. Submitting blocks when the
 * pool's queue is full, which is the back-pressure signal to the parsing loop.
 */
public interface RiskScorer {

    CompletableFuture<List<ScoredTransaction>> scoreAsync(List<NormalizedTransaction> batch);

    /** Tasks waiting for a scoring thread; exposed as a metric. */
    int queueDepth();

    /** Threads currently scoring; exposed as a metric. */
    int activeWorkers();
}
