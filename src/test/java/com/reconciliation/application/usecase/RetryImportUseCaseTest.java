package com.reconciliation.application.usecase;

import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.ImportSummary;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryImportJobQueue;
import com.reconciliation.testing.InMemoryImportRepository;
import com.reconciliation.testing.RecordingMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryImportUseCaseTest {

    private static final int MAX_RETRIES = 2;

    private final FixedClock clock = new FixedClock();
    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final InMemoryImportJobQueue queue = new InMemoryImportJobQueue(imports, clock, Duration.ofSeconds(30));
    private final RecordingMetrics metrics = new RecordingMetrics();
    private final List<UUID> evicted = new ArrayList<>();
    private final SummaryCache cache = new SummaryCache() {
        @Override public Optional<ImportSummary> get(UUID importId) { return Optional.empty(); }
        @Override public void put(UUID importId, ImportSummary summary, Duration ttl) { }
        @Override public void evict(UUID importId) { evicted.add(importId); }
    };
    private final RetryImportUseCase useCase = new RetryImportUseCase(imports, queue, cache, metrics, clock, MAX_RETRIES);
    private final UUID id = UUID.randomUUID();

    /** A failed import after {@code attempts} attempts that committed 40 lines. */
    private void seedFailed(String storageKey, int attempts, int retryCount, Instant purgedAt) {
        imports.put(new ImportJob(id, "key", Fixtures.PROVIDER, ImportStatus.FAILED, "f.ndjson", storageKey, 100L, "sha",
                new ImportProgress(40, 38, 1, 1), 40, false, attempts, null, null, "PROCESSING_FAILED", "boom",
                clock.now(), clock.now(), clock.now(), clock.now(), retryCount, 0, purgedAt));
    }

    @Test
    void failedImportIsRequeuedFromItsCheckpointWithAFreshAttemptBudget() {
        seedFailed("imports/x.ndjson", 3, 0, null);

        ImportJob retried = useCase.execute(id);

        assertThat(retried.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(retried.lastLineCommitted()).isEqualTo(40);
        assertThat(retried.progress()).isEqualTo(new ImportProgress(40, 38, 1, 1));
        assertThat(retried.retryCount()).isEqualTo(1);
        assertThat(retried.attemptCount()).isEqualTo(3);           // history keeps counting
        assertThat(retried.attemptsSinceRetry()).isZero();         // ...but the budget is fresh
        assertThat(retried.failureReason()).isNull();
        assertThat(retried.completedAt()).isNull();
        assertThat(queue.published).containsExactly(id);
        assertThat(evicted).containsExactly(id);
        assertThat(metrics.events).contains("importRetried");
    }

    @Test
    void retryingAnImportThatIsAlreadyRequeuedReturnsItUnchanged() {
        seedFailed("imports/x.ndjson", 1, 0, null);
        ImportJob first = useCase.execute(id);

        ImportJob again = useCase.execute(id);

        assertThat(again.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(again.retryCount()).isEqualTo(first.retryCount());
        assertThat(queue.published).hasSize(1);
    }

    @Test
    void retriesAreCapped() {
        seedFailed("imports/x.ndjson", 1, MAX_RETRIES, null);

        assertThatThrownBy(() -> useCase.execute(id))
                .isInstanceOfSatisfying(AppException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.IMPORT_NOT_RETRYABLE);
                    assertThat(e.getMessage()).contains("Retry limit");
                });
        assertThat(imports.get(id).status()).isEqualTo(ImportStatus.FAILED);
        assertThat(queue.published).isEmpty();
    }

    @Test
    void anUploadThatNeverCompletedCannotBeRetriedHere() {
        seedFailed(null, 0, 0, null);

        assertThatThrownBy(() -> useCase.execute(id))
                .isInstanceOfSatisfying(AppException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.IMPORT_NOT_RETRYABLE);
                    assertThat(e.getMessage()).contains("same Idempotency-Key");
                });
    }

    @Test
    void anImportWhoseFileWasPurgedCannotBeRetried() {
        seedFailed("imports/x.ndjson", 1, 0, clock.now());

        assertThatThrownBy(() -> useCase.execute(id))
                .isInstanceOfSatisfying(AppException.class, e -> assertThat(e.getMessage()).contains("retention"));
    }

    @Test
    void onlyFailedImportsCanBeRetried() {
        imports.put(InMemoryImportRepository.job(id, ImportStatus.COMPLETED, Fixtures.PROVIDER, "imports/x", clock.now()));

        assertThatThrownBy(() -> useCase.execute(id))
                .isInstanceOfSatisfying(AppException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.IMPORT_NOT_RETRYABLE);
                    assertThat(e.getMessage()).contains("'completed'");
                });
    }

    @Test
    void unknownImportIs404() {
        assertThatThrownBy(() -> useCase.execute(UUID.randomUUID()))
                .isInstanceOfSatisfying(AppException.class, e -> assertThat(e.code()).isEqualTo(ErrorCode.IMPORT_NOT_FOUND));
    }
}
