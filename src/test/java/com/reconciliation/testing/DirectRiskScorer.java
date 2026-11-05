package com.reconciliation.testing;

import com.reconciliation.application.port.RiskScorer;
import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.domain.processing.RiskScoreCalculator;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Scores synchronously on the calling thread with a cheap calculator. */
public final class DirectRiskScorer implements RiskScorer {

    private final RiskScoreCalculator calculator = new RiskScoreCalculator(1);

    @Override
    public CompletableFuture<List<ScoredTransaction>> scoreAsync(List<NormalizedTransaction> batch) {
        return CompletableFuture.completedFuture(batch.stream().map(calculator::score).toList());
    }

    @Override
    public int queueDepth() {
        return 0;
    }

    @Override
    public int activeWorkers() {
        return 0;
    }
}
