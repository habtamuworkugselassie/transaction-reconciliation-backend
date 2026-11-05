package com.reconciliation.infrastructure.risk;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.domain.processing.RiskScoreCalculator;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerPoolRiskScorerTest {

    private static List<NormalizedTransaction> batch(int n) {
        List<NormalizedTransaction> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(new NormalizedTransaction(i + 1, Fixtures.PROVIDER, "txn-" + i, "acc", "m" + (i % 5),
                    Fixtures.normalized("x").amount(), "USD", Fixtures.normalized("x").timestamp(), "d" + i));
        }
        return out;
    }

    @Test
    void scoresInInputOrderAcrossChunksAndMatchesTheSequentialCalculator() throws Exception {
        RiskScoreCalculator calculator = new RiskScoreCalculator(2);
        try (WorkerPoolRiskScorer scorer = new WorkerPoolRiskScorer(3, 10, calculator)) {
            List<NormalizedTransaction> input = batch(1000); // 10 chunks of 100 over 3 threads

            List<ScoredTransaction> scored = scorer.scoreAsync(input).get(30, TimeUnit.SECONDS);

            assertThat(scored).hasSize(1000);
            for (int i = 0; i < input.size(); i++) {
                assertThat(scored.get(i).transaction()).isSameAs(input.get(i));
                assertThat(scored.get(i)).isEqualTo(calculator.score(input.get(i)));
            }
        }
    }

    @Test
    void emptyBatchCompletesImmediately() throws Exception {
        try (WorkerPoolRiskScorer scorer = new WorkerPoolRiskScorer(1, 1, new RiskScoreCalculator(1))) {
            assertThat(scorer.scoreAsync(List.of()).get(1, TimeUnit.SECONDS)).isEmpty();
        }
    }

    @Test
    void submissionIsBoundedByTheQueueCapacityButNeverFails() throws Exception {
        // 1 thread, queue of 2: submitting many chunks must block the producer, not throw or grow unbounded.
        try (WorkerPoolRiskScorer scorer = new WorkerPoolRiskScorer(1, 2, new RiskScoreCalculator(1000))) {
            List<CompletableFuture<List<ScoredTransaction>>> futures = new ArrayList<>();
            int observedMaxQueue = 0;
            for (int i = 0; i < 20; i++) {
                futures.add(scorer.scoreAsync(batch(100)));
                observedMaxQueue = Math.max(observedMaxQueue, scorer.queueDepth());
            }
            assertThat(observedMaxQueue).isLessThanOrEqualTo(2);
            assertThat(scorer.poolSize()).isEqualTo(1);
            for (var f : futures) {
                assertThat(f.get(60, TimeUnit.SECONDS)).hasSize(100);
            }
        }
    }

    @Test
    void refusesWorkAfterClose() {
        WorkerPoolRiskScorer scorer = new WorkerPoolRiskScorer(1, 1, new RiskScoreCalculator(1));
        scorer.close();

        assertThatThrownBy(() -> scorer.scoreAsync(batch(1))).isInstanceOf(RejectedExecutionException.class);
    }
}
