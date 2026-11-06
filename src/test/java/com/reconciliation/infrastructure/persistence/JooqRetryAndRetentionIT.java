package com.reconciliation.infrastructure.persistence;

import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.reconciliation.infrastructure.persistence.ItFixtures.LEASE;
import static org.assertj.core.api.Assertions.assertThat;

/** Manual retry and retention as conditional updates on the imports row (migration V2). */
class JooqRetryAndRetentionIT {

    private final PostgresSupport pg = PostgresSupport.get();
    private final JooqImportRepository repo = new JooqImportRepository(pg.db());
    private final JooqImportJobStore store = new JooqImportJobStore(pg.db());
    private final FixedClock clock = new FixedClock();

    @BeforeEach
    void clean() {
        pg.truncateAll();
    }

    private ImportJob failedAfterProcessing(String key) {
        ImportJob job = ItFixtures.processing(repo, store, key, Fixtures.PROVIDER, "w1", clock.now());
        assertThat(repo.fail(job.id(), "w1", "PROCESSING_FAILED", "boom", clock.now())).isTrue();
        return repo.findById(job.id()).orElseThrow();
    }

    @Test
    void retryMovesAFailedImportBackToPendingWithAFreshBudget() {
        ImportJob failed = failedAfterProcessing("k1");
        clock.advance(Duration.ofMinutes(5));

        ImportJob retried = repo.retryFailed(failed.id(), 3, clock.now()).orElseThrow();

        assertThat(retried.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(retried.retryCount()).isEqualTo(1);
        assertThat(retried.attemptBase()).isEqualTo(failed.attemptCount());
        assertThat(retried.attemptsSinceRetry()).isZero();
        assertThat(retried.failureReason()).isNull();
        assertThat(retried.completedAt()).isNull();
        assertThat(retried.leaseOwner()).isNull();

        // and it is claimable again, continuing the attempt numbering
        ImportJob claimed = repo.claim(failed.id(), "w2", LEASE, clock.now()).orElseThrow();
        assertThat(claimed.attemptCount()).isEqualTo(failed.attemptCount() + 1);
        assertThat(claimed.attemptsSinceRetry()).isEqualTo(1);
    }

    @Test
    void retryIsRefusedForNonFailedUnstoredPurgedOrExhaustedImports() {
        ImportJob pending = ItFixtures.pending(repo, store, "pending", Fixtures.PROVIDER, clock.now());
        assertThat(repo.retryFailed(pending.id(), 3, clock.now())).isEmpty();

        ImportJob uploadFailed = ItFixtures.reserved(repo, "upload-failed", Fixtures.PROVIDER, clock.now());
        repo.markUploadFailed(uploadFailed.id(), "IMPORT_FILE_EMPTY", "empty", clock.now());
        assertThat(repo.retryFailed(uploadFailed.id(), 3, clock.now())).isEmpty();

        ImportJob purged = failedAfterProcessing("purged");
        assertThat(repo.markFilePurged(purged.id(), clock.now())).isTrue();
        assertThat(repo.retryFailed(purged.id(), 3, clock.now())).isEmpty();

        ImportJob exhausted = failedAfterProcessing("exhausted");
        assertThat(repo.retryFailed(exhausted.id(), 1, clock.now())).isPresent();
        repo.claim(exhausted.id(), "w1", LEASE, clock.now()).orElseThrow();
        repo.fail(exhausted.id(), "w1", "PROCESSING_FAILED", "again", clock.now());
        assertThat(repo.retryFailed(exhausted.id(), 1, clock.now())).isEmpty();
    }

    @Test
    void concurrentRetriesOfTheSameImportApplyOnce() throws Exception {
        ImportJob failed = failedAfterProcessing("race");

        List<Optional<ImportJob>> results = concurrently(8, () -> repo.retryFailed(failed.id(), 3, clock.now()));

        assertThat(results.stream().filter(Optional::isPresent).count()).isEqualTo(1);
        assertThat(repo.findById(failed.id()).orElseThrow().retryCount()).isEqualTo(1);
    }

    @Test
    void purgeCandidatesAreTerminalStoredUnpurgedAndOldEnough() {
        ImportJob old = failedAfterProcessing("old");
        clock.advance(Duration.ofDays(10));
        ImportJob recent = failedAfterProcessing("recent");
        ItFixtures.processing(repo, store, "running", Fixtures.PROVIDER, "w9", clock.now());

        List<ImportJob> candidates = repo.findPurgeable(clock.now().minus(Duration.ofDays(7)), 100);

        assertThat(candidates).extracting(ImportJob::id).containsExactly(old.id());
        assertThat(recent.id()).isNotIn(candidates.stream().map(ImportJob::id).toList());
    }

    @Test
    void purgeAndRetryAreMutuallyExclusive() throws Exception {
        for (int round = 0; round < 20; round++) {
            ImportJob failed = failedAfterProcessing("x-" + round);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<Boolean> purge = pool.submit(() -> {
                    start.await();
                    return repo.markFilePurged(failed.id(), clock.now());
                });
                Future<Boolean> retry = pool.submit(() -> {
                    start.await();
                    return repo.retryFailed(failed.id(), 3, clock.now()).isPresent();
                });
                start.countDown();
                boolean purged = purge.get(10, TimeUnit.SECONDS);
                boolean retried = retry.get(10, TimeUnit.SECONDS);

                // Never both: a retried import is pending (not terminal) so it cannot be purged,
                // and a purged one cannot be retried.
                assertThat(purged && retried).as("round " + round).isFalse();
                assertThat(purged || retried).as("round " + round).isTrue();
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void markingAPurgeIsIdempotent() {
        ImportJob failed = failedAfterProcessing("once");

        assertThat(repo.markFilePurged(failed.id(), clock.now())).isTrue();
        assertThat(repo.markFilePurged(failed.id(), clock.now())).isFalse();
        ImportJob after = repo.findById(failed.id()).orElseThrow();
        assertThat(after.filePurgedAt()).isEqualTo(clock.now());
        assertThat(after.storageKey()).isNotNull();
        assertThat(repo.findPurgeable(clock.now().plusSeconds(1), 10)).isEmpty();
    }

    private static <T> List<T> concurrently(int threads, Callable<T> action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return action.call();
                }));
            }
            start.countDown();
            List<T> out = new java.util.ArrayList<>();
            for (Future<T> f : futures) {
                out.add(f.get(10, TimeUnit.SECONDS));
            }
            return out;
        } finally {
            pool.shutdownNow();
        }
    }
}
