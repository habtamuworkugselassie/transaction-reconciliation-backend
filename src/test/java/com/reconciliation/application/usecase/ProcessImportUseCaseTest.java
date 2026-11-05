package com.reconciliation.application.usecase;

import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.application.port.RetryPolicy;
import com.reconciliation.application.processing.CancellationToken;
import com.reconciliation.application.processing.ProcessingOutcome;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.infrastructure.retry.ExponentialBackoffRetryPolicy;
import com.reconciliation.infrastructure.retry.SqlStateFailureClassifier;
import com.reconciliation.testing.DirectRiskScorer;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryBatchWriter;
import com.reconciliation.testing.InMemoryFileStorage;
import com.reconciliation.testing.InMemoryImportJobQueue;
import com.reconciliation.testing.InMemoryImportRepository;
import com.reconciliation.testing.RecordingMetrics;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The processing pipeline end to end on fakes: streaming, batching, back-pressure,
 * checkpoint resumption, duplicate handling, cancellation, lease loss, retry and
 * failure classification. The only real collaborators are the parser/validator and
 * the retry policy; storage, repository, queue, batch writer and scorer are in-memory.
 */
class ProcessImportUseCaseTest {

    private static final String WORKER = "worker-1";
    private static final Duration LEASE = Duration.ofSeconds(30);

    private final FixedClock clock = new FixedClock();
    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final InMemoryFileStorage storage = new InMemoryFileStorage();
    private final InMemoryImportJobQueue queue = new InMemoryImportJobQueue(imports, clock, LEASE);
    private final InMemoryBatchWriter writer = new InMemoryBatchWriter(imports, clock);
    private final RecordingMetrics metrics = new RecordingMetrics();
    private final SqlStateFailureClassifier classifier = new SqlStateFailureClassifier();
    private final AtomicInteger sleeps = new AtomicInteger();
    private final RetryPolicy retry = new ExponentialBackoffRetryPolicy(3, Duration.ofMillis(1), Duration.ofMillis(2),
            2, 0, classifier, metrics, d -> sleeps.incrementAndGet());

    private ProcessImportUseCase useCase(int batchSize, int maxAttempts) {
        return new ProcessImportUseCase(imports, queue, storage, new DirectRiskScorer(), writer, retry, classifier,
                clock, metrics, new ProcessImportUseCase.Settings(batchSize, 2, 512, 64, maxAttempts));
    }

    /** Stores the file, creates a pending import for it and claims it for the worker. */
    private ImportJob claimed(String ndjson) {
        UUID id = UUID.randomUUID();
        String key = "imports/" + id + ".ndjson";
        storage.put(key, ndjson);
        imports.put(InMemoryImportRepository.job(id, ImportStatus.PENDING, Fixtures.PROVIDER, key, clock.now()));
        return queue.claim(id, WORKER).orElseThrow();
    }

    @Test
    void processesEveryLineInBatchesAndCompletes() {
        ImportJob job = claimed(Fixtures.validLines(10));

        ProcessingOutcome outcome = useCase(4, 3).execute(job, WORKER, new CancellationToken());

        assertThat(outcome).isEqualTo(ProcessingOutcome.COMPLETED);
        ImportJob after = imports.get(job.id());
        assertThat(after.status()).isEqualTo(ImportStatus.COMPLETED);
        assertThat(after.progress()).isEqualTo(new ImportProgress(10, 10, 0, 0));
        assertThat(after.lastLineCommitted()).isEqualTo(10);
        assertThat(after.leaseOwner()).isNull();
        assertThat(after.completedAt()).isEqualTo(clock.now());
        assertThat(writer.writtenBatches).containsExactly(1, 2, 3);
        assertThat(writer.accepted).hasSize(10);
        assertThat(writer.accepted).allSatisfy(s -> {
            assertThat(s.riskScore()).isBetween(0, 100);
            assertThat(s.fingerprint()).hasSize(64);
        });
        assertThat(metrics.events).contains("recordsParsed:4", "recordsParsed:2", "importFinished:completed");
    }

    @Test
    void invalidRecordsAreRejectedIndividuallyAndProcessingContinues() {
        String file = Fixtures.line("txn-1") + "\n"
                + "not json\n"
                + "\n"
                + Fixtures.line("txn-2", "-1", "USD", "2026-07-20T10:25:00Z", "neg") + "\n"
                + "[1,2,3]\n"
                + "{\"transactionId\":\"txn-3\",\"accountId\":\"a\",\"merchantId\":\"m\",\"amount\":1,\"currency\":\"XXY\",\"timestamp\":\"2026-07-20T10:25:00Z\"}\n"
                + "x".repeat(600) + "\n"
                + Fixtures.line("txn-4"); // no trailing newline

        ImportJob job = claimed(file);
        ProcessingOutcome outcome = useCase(3, 3).execute(job, WORKER, new CancellationToken());

        assertThat(outcome).isEqualTo(ProcessingOutcome.COMPLETED);
        ImportJob after = imports.get(job.id());
        assertThat(after.progress()).isEqualTo(new ImportProgress(7, 2, 5, 0));
        assertThat(after.lastLineCommitted()).as("physical line of the last record").isEqualTo(8);
        assertThat(writer.rejections).extracting(r -> r.lineNumber() + ":" + r.reason())
                .containsExactlyInAnyOrder("2:" + RejectionReason.INVALID_JSON, "4:" + RejectionReason.INVALID_AMOUNT,
                        "5:" + RejectionReason.NOT_AN_OBJECT, "6:" + RejectionReason.INVALID_CURRENCY,
                        "7:" + RejectionReason.LINE_TOO_LONG);
        assertThat(writer.accepted).extracting(s -> s.transaction().transactionId()).containsExactly("txn-1", "txn-4");
    }

    @Test
    void duplicatesWithinAFileAndAcrossImportsAreCountedNotAccepted() {
        ImportJob first = claimed(Fixtures.line("txn-1") + "\n" + Fixtures.line("txn-2") + "\n");
        useCase(10, 3).execute(first, WORKER, new CancellationToken());

        String second = Fixtures.line("txn-2") + "\n"                                            // across imports, identical
                + Fixtures.line("txn-1", "999.00", "USD", "2026-07-20T10:25:00Z", "changed") + "\n" // across imports, conflict
                + Fixtures.line("txn-3") + "\n"
                + Fixtures.line("txn-3") + "\n";                                                 // within the file
        ImportJob job = claimed(second);
        useCase(10, 3).execute(job, WORKER, new CancellationToken());

        ImportJob after = imports.get(job.id());
        assertThat(after.progress()).isEqualTo(new ImportProgress(4, 1, 0, 3));
        assertThat(writer.rejections).extracting(r -> r.lineNumber() + ":" + r.reason())
                .containsExactlyInAnyOrder("1:" + RejectionReason.DUPLICATE_TRANSACTION,
                        "2:" + RejectionReason.DUPLICATE_TRANSACTION_CONFLICT, "4:" + RejectionReason.DUPLICATE_TRANSACTION);
        assertThat(writer.accepted).extracting(s -> s.transaction().transactionId()).containsExactly("txn-1", "txn-2", "txn-3");
    }

    @Test
    void emptyFileCompletesWithZeroCounts() {
        ImportJob job = claimed("\n\n");
        assertThat(useCase(5, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.COMPLETED);
        assertThat(imports.get(job.id()).progress()).isEqualTo(ImportProgress.ZERO);
        assertThat(writer.writtenBatches).isEmpty();
    }

    @Test
    void resumesAfterTheCheckpointWithoutReprocessingCommittedLines() {
        // Attempt 1: crash after the first batch (simulated by a non-transient writer failure on batch 2).
        ImportJob job = claimed(Fixtures.validLines(9));
        writer.failBatch(2, new IllegalStateException("simulated crash"));
        assertThat(useCase(3, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.FAILED);
        assertThat(imports.get(job.id()).lastLineCommitted()).isEqualTo(3);
        assertThat(imports.get(job.id()).progress().processed()).isEqualTo(3);

        // Put it back to pending as a released job would be, and let another worker resume.
        ImportJob failed = imports.get(job.id());
        imports.put(new ImportJob(failed.id(), failed.idempotencyKey(), failed.providerId(), ImportStatus.PENDING,
                failed.originalFilename(), failed.storageKey(), failed.fileSizeBytes(), failed.contentSha256(),
                failed.progress(), failed.lastLineCommitted(), false, failed.attemptCount(), null, null, null, null,
                failed.createdAt(), clock.now(), failed.startedAt(), null));
        ImportJob resumed = queue.claim(job.id(), "worker-2").orElseThrow();
        assertThat(resumed.lastLineCommitted()).isEqualTo(3);
        int parsedBefore = (int) metrics.count("recordsParsed");

        assertThat(useCase(3, 3).execute(resumed, "worker-2", new CancellationToken())).isEqualTo(ProcessingOutcome.COMPLETED);

        ImportJob after = imports.get(job.id());
        assertThat(after.progress()).isEqualTo(new ImportProgress(9, 9, 0, 0));
        assertThat(after.lastLineCommitted()).isEqualTo(9);
        assertThat(metrics.count("recordsParsed") - parsedBefore).as("only two more batches parsed").isEqualTo(2);
        assertThat(writer.accepted).extracting(s -> s.transaction().transactionId())
                .containsExactly("txn-1", "txn-2", "txn-3", "txn-4", "txn-5", "txn-6", "txn-7", "txn-8", "txn-9");
    }

    @Test
    void transientWriteFailuresAreRetriedThenTheImportCompletes() {
        ImportJob job = claimed(Fixtures.validLines(4));
        writer.failBatch(1, new IOException("connection reset"));
        writer.failBatch(1, new java.sql.SQLException("deadlock", "40P01"));

        assertThat(useCase(2, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.COMPLETED);

        assertThat(sleeps.get()).isEqualTo(2);
        assertThat(metrics.events).contains("retryAttempt:batch-write:1", "retryAttempt:batch-write:2");
        assertThat(imports.get(job.id()).progress()).isEqualTo(new ImportProgress(4, 4, 0, 0));
    }

    @Test
    void exhaustedTransientRetriesReleaseTheImportForAnotherAttempt() {
        ImportJob job = claimed(Fixtures.validLines(2));
        for (int i = 0; i < 3; i++) {
            writer.failBatch(1, new IOException("still down " + i));
        }

        assertThat(useCase(5, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.RELEASED);

        ImportJob after = imports.get(job.id());
        assertThat(after.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(after.leaseOwner()).isNull();
        assertThat(after.attemptCount()).isEqualTo(1);
        assertThat(after.progress()).isEqualTo(ImportProgress.ZERO);
        assertThat(metrics.events).contains("retryExhausted:batch-write", "importFinished:released");
    }

    @Test
    void transientFailureOnTheLastAllowedAttemptFailsTheImportPermanently() {
        ImportJob job = claimed(Fixtures.validLines(2));
        for (int i = 0; i < 3; i++) {
            writer.failBatch(1, new IOException("still down " + i));
        }
        ImportJob lastAttempt = new ImportJob(job.id(), job.idempotencyKey(), job.providerId(), job.status(),
                job.originalFilename(), job.storageKey(), job.fileSizeBytes(), job.contentSha256(), job.progress(),
                job.lastLineCommitted(), false, 3, WORKER, job.leaseExpiresAt(), null, null, job.createdAt(),
                job.updatedAt(), job.startedAt(), null);
        imports.put(lastAttempt);

        assertThat(useCase(5, 3).execute(lastAttempt, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.FAILED);

        ImportJob after = imports.get(job.id());
        assertThat(after.status()).isEqualTo(ImportStatus.FAILED);
        assertThat(after.failureReason()).isEqualTo(ErrorCode.PROCESSING_FAILED.name());
    }

    @Test
    void programmingErrorsFailTheImportImmediatelyWithoutRetryOrSensitiveDetails() {
        ImportJob job = claimed(Fixtures.validLines(2));
        writer.failBatch(1, new NullPointerException("at com.example.Internal.secret(Internal.java:42)"));

        assertThat(useCase(5, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.FAILED);

        ImportJob after = imports.get(job.id());
        assertThat(after.status()).isEqualTo(ImportStatus.FAILED);
        assertThat(after.failureMessage()).isEqualTo("NullPointerException");
        assertThat(sleeps.get()).isZero();
    }

    @Test
    void unreadableFileIsAStorageFailureThatIsReleasedForRetry() {
        ImportJob job = claimed(Fixtures.validLines(2));
        storage.failOnOpen = new IOException("volume unmounted");

        assertThat(useCase(5, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.RELEASED);
        assertThat(imports.get(job.id()).status()).isEqualTo(ImportStatus.PENDING);
    }

    @Test
    void cancellationRequestedMidImportStopsAtTheNextBatchBoundaryAndCommitsInFlightWork() {
        ImportJob job = claimed(Fixtures.validLines(10));
        // The cancel arrives while batch 1 is being written.
        writer.failures.put(1, new ArrayList<>(List.of(() -> {
            imports.requestCancel(job.id(), clock.now());
            return null;
        })));
        ProcessImportUseCase useCase = useCase(2, 3);

        ProcessingOutcome outcome = useCase.execute(job, WORKER, new CancellationToken());

        assertThat(outcome).isEqualTo(ProcessingOutcome.CANCELLED);
        ImportJob after = imports.get(job.id());
        assertThat(after.status()).isEqualTo(ImportStatus.CANCELLED);
        assertThat(after.completedAt()).isNotNull();
        assertThat(after.progress().processed()).as("in-flight batches committed, nothing more").isBetween(2L, 6L);
        assertThat(after.lastLineCommitted()).isEqualTo(after.progress().processed());
        assertThat(metrics.events).contains("importFinished:cancelled");
    }

    @Test
    void claimingAnAbandonedCancellingImportJustFinalisesIt() {
        ImportJob job = claimed(Fixtures.validLines(4));
        imports.requestCancel(job.id(), clock.now());
        ImportJob cancelling = imports.get(job.id());
        assertThat(cancelling.status()).isEqualTo(ImportStatus.CANCELLING);

        assertThat(useCase(2, 3).execute(cancelling, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.CANCELLED);

        assertThat(imports.get(job.id()).status()).isEqualTo(ImportStatus.CANCELLED);
        assertThat(writer.writtenBatches).as("nothing processed").isEmpty();
    }

    @Test
    void shutdownSignalReleasesTheImportSoAnotherWorkerResumesFromTheCheckpoint() {
        ImportJob job = claimed(Fixtures.validLines(10));
        CancellationToken token = new CancellationToken();
        writer.failures.put(2, new ArrayList<>(List.of(() -> {
            token.cancel(CancellationToken.Reason.SHUTDOWN);
            return null;
        })));

        ProcessingOutcome outcome = useCase(2, 3).execute(job, WORKER, token);

        assertThat(outcome).isEqualTo(ProcessingOutcome.RELEASED);
        ImportJob after = imports.get(job.id());
        assertThat(after.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(after.leaseOwner()).isNull();
        assertThat(after.lastLineCommitted()).isBetween(2L, 8L);
        assertThat(after.progress().processed()).isEqualTo(after.lastLineCommitted());
    }

    @Test
    void lostLeaseAbandonsTheAttemptWithoutChangingState() {
        ImportJob job = claimed(Fixtures.validLines(6));
        // Another worker takes over: the lease expired and was re-claimed.
        clock.advance(LEASE.plusSeconds(1));
        ImportJob takenOver = queue.claim(job.id(), "worker-2").orElseThrow();
        assertThat(takenOver.leaseOwner()).isEqualTo("worker-2");

        ProcessingOutcome outcome = useCase(2, 3).execute(job, WORKER, new CancellationToken());

        assertThat(outcome).isEqualTo(ProcessingOutcome.ABANDONED);
        ImportJob after = imports.get(job.id());
        assertThat(after.leaseOwner()).isEqualTo("worker-2");
        assertThat(after.status()).isEqualTo(ImportStatus.PROCESSING);
        assertThat(after.progress()).isEqualTo(ImportProgress.ZERO);
        assertThat(sleeps.get()).as("lease loss is never retried").isZero();
    }

    @Test
    void heartbeatDetectedLeaseLossStopsTheLoop() {
        ImportJob job = claimed(Fixtures.validLines(20));
        CancellationToken token = new CancellationToken();
        token.cancel(CancellationToken.Reason.LEASE_LOST);

        assertThat(useCase(5, 3).execute(job, WORKER, token)).isEqualTo(ProcessingOutcome.ABANDONED);
        assertThat(writer.writtenBatches).isEmpty();
    }

    @Test
    void redeliveredBatchIsANoOpThanksToTheCheckpointGuard() {
        ImportJob job = claimed(Fixtures.validLines(3));
        useCase(10, 3).execute(job, WORKER, new CancellationToken());
        ImportJob after = imports.get(job.id());

        // Pretend the same batch is written again by a straggling attempt that still holds the lease.
        imports.put(new ImportJob(after.id(), after.idempotencyKey(), after.providerId(), ImportStatus.PROCESSING,
                after.originalFilename(), after.storageKey(), after.fileSizeBytes(), after.contentSha256(),
                after.progress(), after.lastLineCommitted(), false, 1, WORKER, clock.now().plus(LEASE), null, null,
                after.createdAt(), after.updatedAt(), after.startedAt(), null));
        BatchWriter.BatchOutcome outcome = writer.write(new BatchWriter.BatchWrite(job.id(), WORKER, 1, 3,
                writer.accepted.subList(0, 3), List.of()));

        assertThat(outcome.alreadyCommitted()).isTrue();
        assertThat(outcome.progressAfter()).isEqualTo(new ImportProgress(3, 3, 0, 0));
        assertThat(writer.accepted).hasSize(3);
    }

    @Test
    void writerWithoutALeaseThrowsLeaseLost() {
        ImportJob job = claimed(Fixtures.validLines(1));
        assertThatThrownBy(() -> writer.write(new BatchWriter.BatchWrite(job.id(), "someone-else", 1, 1, List.of(), List.of())))
                .isInstanceOf(LeaseLostException.class);
    }

    @Test
    void appExceptionsThatAreNotRetryableFailTheImportWithTheirCode() {
        ImportJob job = claimed(Fixtures.validLines(1));
        writer.failBatch(1, new AppException(ErrorCode.IMPORT_STATE_CONFLICT, "row vanished"));

        assertThat(useCase(5, 3).execute(job, WORKER, new CancellationToken())).isEqualTo(ProcessingOutcome.FAILED);
        assertThat(imports.get(job.id()).failureMessage()).isEqualTo("IMPORT_STATE_CONFLICT: row vanished");
    }
}
