package com.reconciliation.infrastructure.persistence;

import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.ImportRepository.Reservation;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.reconciliation.infrastructure.persistence.ItFixtures.LEASE;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORT_ATTEMPTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The imports table as the correctness model: idempotency through the unique key,
 * ownership through conditional updates and leases, and SKIP LOCKED claiming.
 */
class JooqImportRepositoryIT {

    private final PostgresSupport pg = PostgresSupport.get();
    private final JooqImportRepository repo = new JooqImportRepository(pg.db());
    private final JooqImportJobStore store = new JooqImportJobStore(pg.db());
    private final FixedClock clock = new FixedClock();

    @BeforeEach
    void clean() {
        pg.truncateAll();
    }

    // ---- idempotency -------------------------------------------------------

    @Test
    void reservingAFreeKeyCreatesAnUploadingRow() {
        ImportJob job = ItFixtures.reserved(repo, "k1", Fixtures.PROVIDER, clock.now());

        ImportJob stored = repo.findById(job.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(ImportStatus.UPLOADING);
        assertThat(stored.idempotencyKey()).isEqualTo("k1");
        assertThat(stored.providerId()).isEqualTo(Fixtures.PROVIDER);
        assertThat(stored.storageKey()).isNull();
        assertThat(stored.createdAt()).isEqualTo(clock.now());
    }

    @Test
    void reservingAUsedKeyReturnsTheExistingRowWhateverItsState() {
        ImportJob first = ItFixtures.reserved(repo, "k1", Fixtures.PROVIDER, clock.now());
        repo.markUploaded(first.id(), "imports/x", 1, "h".repeat(64), clock.now());
        store.markPending(first.id(), clock.now());

        Reservation again = repo.reserve(ImportJob.reserved(UUID.randomUUID(), "k1", "other", "g", clock.now()));

        assertThat(again).isInstanceOf(Reservation.Existing.class);
        assertThat(again.job().id()).isEqualTo(first.id());
        assertThat(again.job().status()).isEqualTo(ImportStatus.PENDING);
        assertThat(pg.db().fetchCount(IMPORTS)).isEqualTo(1);
    }

    @Test
    void concurrentReservationsOfTheSameKeyProduceExactlyOneRow() throws Exception {
        int threads = 16;
        List<Reservation> results = runConcurrently(threads, () ->
                repo.reserve(ImportJob.reserved(UUID.randomUUID(), "race", Fixtures.PROVIDER, "f", clock.now())));

        long created = results.stream().filter(r -> r instanceof Reservation.Created).count();
        Set<UUID> ids = results.stream().map(r -> r.job().id()).collect(Collectors.toSet());

        assertThat(created).isEqualTo(1);
        assertThat(ids).hasSize(1);
        assertThat(pg.db().fetchCount(IMPORTS)).isEqualTo(1);
    }

    @Test
    void failedUploadCanBeReopenedExactlyOnceForARetry() {
        ImportJob job = ItFixtures.reserved(repo, "k1", Fixtures.PROVIDER, clock.now());
        assertThat(repo.markUploadFailed(job.id(), "UPLOAD_INTERRUPTED", "boom", clock.now())).isTrue();
        clock.advance(Duration.ofMinutes(1));

        Optional<ImportJob> reopened = repo.reopenFailedUpload(job.id(), clock.now());
        Optional<ImportJob> again = repo.reopenFailedUpload(job.id(), clock.now());

        assertThat(reopened).isPresent();
        assertThat(reopened.get().status()).isEqualTo(ImportStatus.UPLOADING);
        assertThat(reopened.get().failureReason()).isNull();
        assertThat(reopened.get().createdAt()).as("stale-upload clock restarts").isEqualTo(clock.now());
        assertThat(again).as("already uploading: a second retry must not take over").isEmpty();
    }

    @Test
    void completedImportIsNeverReopened() {
        ImportJob job = ItFixtures.processing(repo, store, "k1", Fixtures.PROVIDER, "w1", clock.now());
        repo.fail(job.id(), "w1", "PROCESSING_FAILED", "x", clock.now());

        assertThat(repo.reopenFailedUpload(job.id(), clock.now())).as("has a stored file; failed in processing").isEmpty();
    }

    // ---- upload transitions ---------------------------------------------------

    @Test
    void uploadTransitionsOnlyApplyFromUploading() {
        ImportJob job = ItFixtures.reserved(repo, "k1", Fixtures.PROVIDER, clock.now());

        assertThat(repo.markUploaded(job.id(), "imports/a", 10, "s".repeat(64), clock.now())).isTrue();
        assertThat(store.markPending(job.id(), clock.now())).isTrue();
        assertThat(store.markPending(job.id(), clock.now())).as("already pending").isFalse();
        assertThat(repo.markUploaded(job.id(), "imports/b", 20, "t".repeat(64), clock.now())).as("not uploading any more").isFalse();
        assertThat(repo.markUploadFailed(job.id(), "X", "y", clock.now())).isFalse();

        ImportJob stored = repo.findById(job.id()).orElseThrow();
        assertThat(stored.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(stored.storageKey()).isEqualTo("imports/a");
        assertThat(stored.fileSizeBytes()).isEqualTo(10);
    }

    @Test
    void staleUploadsAreReapedAndRecentOnesLeftAlone() {
        ImportJob old = ItFixtures.reserved(repo, "old", Fixtures.PROVIDER, clock.now());
        clock.advance(Duration.ofMinutes(20));
        ImportJob fresh = ItFixtures.reserved(repo, "fresh", Fixtures.PROVIDER, clock.now());

        int reaped = repo.failStaleUploads(clock.now().minus(Duration.ofMinutes(15)), clock.now());

        assertThat(reaped).isEqualTo(1);
        assertThat(repo.findById(old.id()).orElseThrow().status()).isEqualTo(ImportStatus.FAILED);
        assertThat(repo.findById(old.id()).orElseThrow().failureReason()).isEqualTo("UPLOAD_INTERRUPTED");
        assertThat(repo.findById(fresh.id()).orElseThrow().status()).isEqualTo(ImportStatus.UPLOADING);
    }

    // ---- claiming and leases -----------------------------------------------------

    @Test
    void claimingAPendingImportLeasesItAndCountsTheAttempt() {
        ImportJob pending = ItFixtures.pending(repo, store, "k1", Fixtures.PROVIDER, clock.now());

        ImportJob claimed = repo.claim(pending.id(), "w1", LEASE, clock.now()).orElseThrow();

        assertThat(claimed.status()).isEqualTo(ImportStatus.PROCESSING);
        assertThat(claimed.leaseOwner()).isEqualTo("w1");
        assertThat(claimed.leaseExpiresAt()).isEqualTo(clock.now().plus(LEASE));
        assertThat(claimed.attemptCount()).isEqualTo(1);
        assertThat(claimed.startedAt()).isEqualTo(clock.now());
    }

    @Test
    void aLiveLeaseCannotBeClaimedByAnotherWorkerButAnExpiredOneCan() {
        ImportJob job = ItFixtures.processing(repo, store, "k1", Fixtures.PROVIDER, "w1", clock.now());
        Instant firstStart = job.startedAt();

        assertThat(repo.claim(job.id(), "w2", LEASE, clock.now())).isEmpty();
        assertThat(repo.claim(job.id(), "w2", LEASE, clock.now().plus(LEASE).minusSeconds(1))).isEmpty();

        clock.advance(LEASE.plusSeconds(1));
        ImportJob takenOver = repo.claim(job.id(), "w2", LEASE, clock.now()).orElseThrow();

        assertThat(takenOver.leaseOwner()).isEqualTo("w2");
        assertThat(takenOver.attemptCount()).isEqualTo(2);
        assertThat(takenOver.startedAt()).as("first start is kept").isEqualTo(firstStart);
        assertThat(repo.extendLease(job.id(), "w1", LEASE, clock.now())).as("the old owner is locked out").isFalse();
        assertThat(repo.complete(job.id(), "w1", clock.now())).isFalse();
    }

    @Test
    void terminalImportsAreNeverClaimable() {
        ImportJob job = ItFixtures.processing(repo, store, "k1", Fixtures.PROVIDER, "w1", clock.now());
        repo.complete(job.id(), "w1", clock.now());
        clock.advance(Duration.ofDays(1));

        assertThat(repo.claim(job.id(), "w2", LEASE, clock.now())).isEmpty();
        assertThat(repo.claimNext("w2", LEASE, clock.now())).isEmpty();
    }

    @Test
    void concurrentWorkersNeverClaimTheSameImport() throws Exception {
        for (int i = 0; i < 5; i++) {
            ItFixtures.pending(repo, store, "k" + i, Fixtures.PROVIDER, clock.now().plusSeconds(i));
        }
        int workers = 10;
        List<Optional<ImportJob>> claims = runConcurrently(workers, () -> {
            String worker = "w-" + Thread.currentThread().getId();
            return repo.claimNext(worker, LEASE, clock.now());
        });

        List<UUID> claimedIds = claims.stream().flatMap(Optional::stream).map(ImportJob::id).toList();
        assertThat(claimedIds).hasSize(5);
        assertThat(Set.copyOf(claimedIds)).hasSize(5);
        assertThat(store.queuedCount()).isZero();
        assertThat(store.inFlightCount(clock.now())).isEqualTo(5);
    }

    @Test
    void claimNextTakesTheOldestPendingFirst() {
        ImportJob older = ItFixtures.pending(repo, store, "older", Fixtures.PROVIDER, clock.now());
        clock.advance(Duration.ofSeconds(5));
        ItFixtures.pending(repo, store, "newer", Fixtures.PROVIDER, clock.now());

        assertThat(repo.claimNext("w1", LEASE, clock.now()).orElseThrow().id()).isEqualTo(older.id());
    }

    @Test
    void heartbeatExtendsOnlyTheOwnersLease() {
        ImportJob job = ItFixtures.processing(repo, store, "k1", Fixtures.PROVIDER, "w1", clock.now());
        clock.advance(Duration.ofSeconds(10));

        assertThat(repo.extendLease(job.id(), "w1", LEASE, clock.now())).isTrue();
        assertThat(repo.extendLease(job.id(), "w2", LEASE, clock.now())).isFalse();
        assertThat(repo.findById(job.id()).orElseThrow().leaseExpiresAt()).isEqualTo(clock.now().plus(LEASE));
    }

    // ---- outcomes ------------------------------------------------------------------

    @Test
    void completeFailReleaseAndCancelAreOwnerOnlyAndClearTheLease() {
        ImportJob a = ItFixtures.processing(repo, store, "a", Fixtures.PROVIDER, "w1", clock.now());
        ImportJob b = ItFixtures.processing(repo, store, "b", Fixtures.PROVIDER, "w1", clock.now());
        ImportJob c = ItFixtures.processing(repo, store, "c", Fixtures.PROVIDER, "w1", clock.now());
        ImportJob d = ItFixtures.processing(repo, store, "d", Fixtures.PROVIDER, "w1", clock.now());

        assertThat(repo.complete(a.id(), "intruder", clock.now())).isFalse();
        assertThat(repo.complete(a.id(), "w1", clock.now())).isTrue();
        assertThat(repo.fail(b.id(), "w1", "PROCESSING_FAILED", "m".repeat(600), clock.now())).isTrue();
        assertThat(repo.release(c.id(), "w1", clock.now())).isTrue();
        assertThat(repo.markCancelled(d.id(), "w1", clock.now())).isTrue();

        ImportJob completed = repo.findById(a.id()).orElseThrow();
        assertThat(completed.status()).isEqualTo(ImportStatus.COMPLETED);
        assertThat(completed.leaseOwner()).isNull();
        assertThat(completed.leaseExpiresAt()).isNull();
        assertThat(completed.completedAt()).isEqualTo(clock.now());

        ImportJob failed = repo.findById(b.id()).orElseThrow();
        assertThat(failed.status()).isEqualTo(ImportStatus.FAILED);
        assertThat(failed.failureReason()).isEqualTo("PROCESSING_FAILED");
        assertThat(failed.failureMessage()).hasSize(512);

        ImportJob released = repo.findById(c.id()).orElseThrow();
        assertThat(released.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(released.leaseOwner()).isNull();
        assertThat(released.attemptCount()).as("attempt history survives a release").isEqualTo(1);

        assertThat(repo.findById(d.id()).orElseThrow().status()).isEqualTo(ImportStatus.CANCELLED);

        // Terminal rows accept no further transition from anyone.
        assertThat(repo.complete(a.id(), "w1", clock.now())).isFalse();
        assertThat(repo.release(a.id(), "w1", clock.now())).isFalse();
        assertThat(repo.fail(a.id(), "w1", "X", "y", clock.now())).isFalse();
    }

    @Test
    void cancelRequestOnPendingIsImmediateAndOnProcessingIsAFlagForTheWorker() {
        ImportJob pending = ItFixtures.pending(repo, store, "p", Fixtures.PROVIDER, clock.now());
        ImportJob processing = ItFixtures.processing(repo, store, "q", Fixtures.PROVIDER, "w1", clock.now());
        ImportJob done = ItFixtures.processing(repo, store, "r", Fixtures.PROVIDER, "w1", clock.now());
        repo.complete(done.id(), "w1", clock.now());

        ImportJob cancelled = repo.requestCancel(pending.id(), clock.now()).orElseThrow();
        ImportJob cancelling = repo.requestCancel(processing.id(), clock.now()).orElseThrow();

        assertThat(cancelled.status()).isEqualTo(ImportStatus.CANCELLED);
        assertThat(cancelled.completedAt()).isEqualTo(clock.now());
        assertThat(cancelling.status()).isEqualTo(ImportStatus.CANCELLING);
        assertThat(cancelling.cancelRequested()).isTrue();
        assertThat(cancelling.completedAt()).isNull();
        assertThat(cancelling.leaseOwner()).as("the worker still owns it").isEqualTo("w1");
        assertThat(repo.isCancelRequested(processing.id())).isTrue();
        assertThat(repo.isCancelRequested(UUID.randomUUID())).as("vanished row means stop").isTrue();
        assertThat(repo.requestCancel(processing.id(), clock.now())).as("already cancelling").isEmpty();
        assertThat(repo.requestCancel(done.id(), clock.now())).isEmpty();

        // The owner finalises; a cancelling import cannot be released back to the queue.
        assertThat(repo.release(processing.id(), "w1", clock.now())).isFalse();
        assertThat(repo.extendLease(processing.id(), "w1", LEASE, clock.now())).isTrue();
        assertThat(repo.markCancelled(processing.id(), "w1", clock.now())).isTrue();
    }

    @Test
    void abandonedCancellingImportIsReclaimedStillCancelling() {
        ImportJob job = ItFixtures.processing(repo, store, "k1", Fixtures.PROVIDER, "w1", clock.now());
        repo.requestCancel(job.id(), clock.now());
        clock.advance(LEASE.plusSeconds(1));

        ImportJob reclaimed = repo.claim(job.id(), "w2", LEASE, clock.now()).orElseThrow();

        assertThat(reclaimed.status()).isEqualTo(ImportStatus.CANCELLING);
        assertThat(reclaimed.cancelRequested()).isTrue();
        assertThat(reclaimed.leaseOwner()).isEqualTo("w2");
    }

    @Test
    void staleWorkIsFoundForRedispatch() {
        ImportJob unclaimed = ItFixtures.pending(repo, store, "a", Fixtures.PROVIDER, clock.now());
        ImportJob dead = ItFixtures.processing(repo, store, "b", Fixtures.PROVIDER, "w1", clock.now());
        ItFixtures.processing(repo, store, "c", Fixtures.PROVIDER, "w1", clock.now().plus(LEASE)); // live lease
        clock.advance(LEASE.plusSeconds(1));
        ItFixtures.pending(repo, store, "d", Fixtures.PROVIDER, clock.now()); // just enqueued

        List<UUID> stale = store.findNeedingDispatch(clock.now(), clock.now().minusSeconds(10), 10);

        assertThat(stale).containsExactlyInAnyOrder(unclaimed.id(), dead.id());
    }

    @Test
    void attemptsAreRecordedAndUpsertedPerAttemptNumber() {
        ImportJob job = ItFixtures.processing(repo, store, "k1", Fixtures.PROVIDER, "w1", clock.now());

        long row = repo.recordAttemptStart(job.id(), 1, "w1", clock.now());
        repo.recordAttemptEnd(row, "released", null, clock.now());
        long sameAttemptAgain = repo.recordAttemptStart(job.id(), 1, "w2", clock.now());

        assertThat(sameAttemptAgain).isEqualTo(row);
        var record = pg.db().selectFrom(IMPORT_ATTEMPTS).where(IMPORT_ATTEMPTS.ID.eq(row)).fetchSingle();
        assertThat(record.getWorkerId()).isEqualTo("w2");
        assertThat(record.getOutcome()).isNull();
        assertThat(pg.db().fetchCount(IMPORT_ATTEMPTS)).isEqualTo(1);
    }

    // ---- helpers ------------------------------------------------------------------

    private static <T> List<T> runConcurrently(int threads, Callable<T> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch go = new CountDownLatch(1);
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    go.await();
                    return task.call();
                }));
            }
            go.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get(30, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }
}
