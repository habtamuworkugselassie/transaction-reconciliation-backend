package com.reconciliation.infrastructure.persistence;

import com.reconciliation.application.port.BatchWriter.BatchOutcome;
import com.reconciliation.application.port.BatchWriter.BatchWrite;
import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.domain.model.RiskLevel;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.jooq.exception.DataAccessException;

import java.util.List;
import java.util.Map;

import static com.reconciliation.infrastructure.persistence.jooq.Tables.REJECTED_RECORDS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * One transaction per batch: accepted rows, rejections, duplicate classification,
 * counters and checkpoint all commit together or not at all.
 */
class JooqBatchWriterIT {

    private final PostgresSupport pg = PostgresSupport.get();
    private final FixedClock clock = new FixedClock();
    private final JooqImportRepository repo = new JooqImportRepository(pg.db());
    private final JooqImportJobStore store = new JooqImportJobStore(pg.db());
    private final JooqBatchWriter writer = new JooqBatchWriter(pg.db(), pg.transactionTemplate(), clock);

    @BeforeEach
    void clean() {
        pg.truncateAll();
    }

    private ImportJob processing(String key, String worker) {
        return ItFixtures.processing(repo, store, key, Fixtures.PROVIDER, worker, clock.now());
    }

    @Test
    void persistsAcceptedRowsRejectionsCountersAndCheckpointTogether() {
        ImportJob job = processing("k1", "w1");
        List<ScoredTransaction> candidates = List.of(ItFixtures.scored(1, "txn-1"), ItFixtures.scored(3, "txn-2"));
        List<Rejection> rejections = List.of(new Rejection(2, RejectionReason.INVALID_JSON, "Line is not valid JSON",
                Map.of("raw", "{oops")));

        BatchOutcome outcome = writer.write(new BatchWrite(job.id(), "w1", 1, 3, candidates, rejections));

        assertThat(outcome.accepted()).isEqualTo(2);
        assertThat(outcome.rejected()).isEqualTo(1);
        assertThat(outcome.duplicates()).isZero();
        assertThat(outcome.alreadyCommitted()).isFalse();
        assertThat(outcome.progressAfter()).isEqualTo(new ImportProgress(3, 2, 1, 0));

        ImportJob after = repo.findById(job.id()).orElseThrow();
        assertThat(after.progress()).isEqualTo(new ImportProgress(3, 2, 1, 0));
        assertThat(after.lastLineCommitted()).isEqualTo(3);

        var rows = pg.db().selectFrom(TRANSACTIONS).where(TRANSACTIONS.IMPORT_ID.eq(job.id())).orderBy(TRANSACTIONS.LINE_NUMBER).fetch();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getTransactionId()).isEqualTo("txn-1");
        assertThat(rows.get(0).getProviderId()).isEqualTo(Fixtures.PROVIDER);
        assertThat(rows.get(0).getAmount()).isEqualByComparingTo("145.7500");
        assertThat(rows.get(0).getCurrency()).isEqualTo("USD");
        assertThat(rows.get(0).getFingerprint()).isEqualTo(candidates.get(0).fingerprint());
        assertThat(rows.get(0).getFingerprintVersion()).isEqualTo((short) 1);
        assertThat(rows.get(0).getRiskScore()).isEqualTo((short) candidates.get(0).riskScore());
        assertThat(rows.get(0).getRiskLevel()).isEqualTo(candidates.get(0).riskLevel().wireName());
        assertThat(rows.get(0).getOccurredAt().toInstant()).isEqualTo(candidates.get(0).transaction().timestamp());

        var rejected = pg.db().selectFrom(REJECTED_RECORDS).where(REJECTED_RECORDS.IMPORT_ID.eq(job.id())).fetch();
        assertThat(rejected).hasSize(1);
        assertThat(rejected.get(0).getLineNumber()).isEqualTo(2);
        assertThat(rejected.get(0).getReason()).isEqualTo("INVALID_JSON");
        assertThat(rejected.get(0).getRawValue().data()).contains("\"raw\"").contains("{oops");
    }

    @Test
    void duplicatesAreDetectedWithinABatchAcrossBatchesAndAcrossImportsAndClassified() {
        ImportJob first = processing("k1", "w1");
        writer.write(new BatchWrite(first.id(), "w1", 1, 2,
                List.of(ItFixtures.scored(1, "txn-1"), ItFixtures.scored(2, "txn-2")), List.of()));

        ImportJob second = processing("k2", "w1");
        List<ScoredTransaction> candidates = List.of(
                ItFixtures.scored(1, "txn-1"),                                                        // identical, other import
                ItFixtures.scored(2, Fixtures.PROVIDER, "txn-2", "999.0000", "USD", "m", "a", "changed"), // conflict, other import
                ItFixtures.scored(3, "txn-3"),
                ItFixtures.scored(4, "txn-3"));                                                       // identical, same batch
        BatchOutcome outcome = writer.write(new BatchWrite(second.id(), "w1", 1, 4, candidates, List.of()));

        assertThat(outcome.accepted()).isEqualTo(1);
        assertThat(outcome.duplicates()).isEqualTo(3);
        assertThat(outcome.progressAfter()).isEqualTo(new ImportProgress(4, 1, 0, 3));

        // Across batches of the same import.
        BatchOutcome next = writer.write(new BatchWrite(second.id(), "w1", 2, 5, List.of(ItFixtures.scored(5, "txn-3")), List.of()));
        assertThat(next.duplicates()).isEqualTo(1);

        var rejected = pg.db().selectFrom(REJECTED_RECORDS).where(REJECTED_RECORDS.IMPORT_ID.eq(second.id()))
                .orderBy(REJECTED_RECORDS.LINE_NUMBER).fetch();
        assertThat(rejected).extracting(r -> r.getLineNumber() + ":" + r.getReason()).containsExactly(
                "1:DUPLICATE_TRANSACTION", "2:DUPLICATE_TRANSACTION_CONFLICT", "4:DUPLICATE_TRANSACTION", "5:DUPLICATE_TRANSACTION");
        assertThat(rejected.get(1).getRawValue().data()).contains("txn-2");

        // First write wins: txn-2 still carries its original content.
        var kept = pg.db().selectFrom(TRANSACTIONS).where(TRANSACTIONS.TRANSACTION_ID.eq("txn-2")).fetchSingle();
        assertThat(kept.getImportId()).isEqualTo(first.id());
        assertThat(kept.getAmount()).isEqualByComparingTo("145.7500");
        assertThat(pg.db().fetchCount(TRANSACTIONS)).isEqualTo(3);
    }

    @Test
    void sameTransactionIdFromAnotherProviderIsNotADuplicate() {
        ImportJob a = ItFixtures.processing(repo, store, "ka", "provider-a", "w1", clock.now());
        ImportJob b = ItFixtures.processing(repo, store, "kb", "provider-b", "w1", clock.now());

        writer.write(new BatchWrite(a.id(), "w1", 1, 1, List.of(ItFixtures.scored(1, "provider-a", "txn-1", "1.0000", "USD", "m", "a", null)), List.of()));
        BatchOutcome outcome = writer.write(new BatchWrite(b.id(), "w1", 1, 1,
                List.of(ItFixtures.scored(1, "provider-b", "txn-1", "1.0000", "USD", "m", "a", null)), List.of()));

        assertThat(outcome.accepted()).isEqualTo(1);
        assertThat(outcome.duplicates()).isZero();
        assertThat(pg.db().fetchCount(TRANSACTIONS)).isEqualTo(2);
    }

    @Test
    void aBatchAlreadyCoveredByTheCheckpointIsANoOp() {
        ImportJob job = processing("k1", "w1");
        BatchWrite batch = new BatchWrite(job.id(), "w1", 1, 2, List.of(ItFixtures.scored(1, "txn-1")),
                List.of(new Rejection(2, RejectionReason.MISSING_FIELD, "amount is required", Map.of())));
        writer.write(batch);

        BatchOutcome redelivered = writer.write(batch);

        assertThat(redelivered.alreadyCommitted()).isTrue();
        assertThat(redelivered.accepted()).isZero();
        assertThat(redelivered.progressAfter()).isEqualTo(new ImportProgress(2, 1, 1, 0));
        assertThat(repo.findById(job.id()).orElseThrow().progress()).isEqualTo(new ImportProgress(2, 1, 1, 0));
        assertThat(pg.db().fetchCount(TRANSACTIONS)).isEqualTo(1);
        assertThat(pg.db().fetchCount(REJECTED_RECORDS)).isEqualTo(1);
    }

    @Test
    void writerWithoutTheLeaseIsRefusedAndWritesNothing() {
        ImportJob job = processing("k1", "w1");
        BatchWrite batch = new BatchWrite(job.id(), "w2", 1, 1, List.of(ItFixtures.scored(1, "txn-1")), List.of());

        assertThatThrownBy(() -> writer.write(batch)).isInstanceOf(LeaseLostException.class);

        assertThat(pg.db().fetchCount(TRANSACTIONS)).isZero();
        assertThat(repo.findById(job.id()).orElseThrow().progress()).isEqualTo(ImportProgress.ZERO);
    }

    @Test
    void lostLeaseAfterCompletionIsAlsoRefused() {
        ImportJob job = processing("k1", "w1");
        repo.complete(job.id(), "w1", clock.now());

        assertThatThrownBy(() -> writer.write(new BatchWrite(job.id(), "w1", 1, 1, List.of(ItFixtures.scored(1, "txn-1")), List.of())))
                .isInstanceOf(LeaseLostException.class);
    }

    @Test
    void aConstraintViolationRollsBackTheWholeBatch() {
        ImportJob job = processing("k1", "w1");
        writer.write(new BatchWrite(job.id(), "w1", 1, 1, List.of(ItFixtures.scored(1, "txn-0")), List.of()));

        // risk_score 101 violates transactions_risk_score_range; everything in the batch must vanish.
        ScoredTransaction good = ItFixtures.scored(2, "txn-1");
        NormalizedTransaction t = ItFixtures.scored(3, "txn-2").transaction();
        ScoredTransaction bad = new ScoredTransaction(t, "f".repeat(64), 1, 101, RiskLevel.HIGH);
        BatchWrite batch = new BatchWrite(job.id(), "w1", 2, 4, List.of(good, bad),
                List.of(new Rejection(4, RejectionReason.INVALID_JSON, "x", Map.of())));

        assertThatThrownBy(() -> writer.write(batch)).isInstanceOf(DataAccessException.class);

        ImportJob after = repo.findById(job.id()).orElseThrow();
        assertThat(after.progress()).isEqualTo(new ImportProgress(1, 1, 0, 0));
        assertThat(after.lastLineCommitted()).isEqualTo(1);
        assertThat(pg.db().fetchCount(TRANSACTIONS)).isEqualTo(1);
        assertThat(pg.db().fetchCount(REJECTED_RECORDS)).isZero();

        // The same batch, corrected, then goes through: nothing from the failed attempt lingers.
        BatchOutcome retry = writer.write(new BatchWrite(job.id(), "w1", 2, 4, List.of(good, ItFixtures.scored(3, "txn-2")),
                List.of(new Rejection(4, RejectionReason.INVALID_JSON, "x", Map.of()))));
        assertThat(retry.progressAfter()).isEqualTo(new ImportProgress(4, 3, 1, 0));
    }

    @Test
    void rejectionsOfARedeliveredBatchAreNotDoubleRecorded() {
        // Same import, same line, written by two batches (possible when a release re-reads a partial batch
        // with a different batch boundary): the unique (import_id, line_number) constraint absorbs it.
        ImportJob job = processing("k1", "w1");
        Rejection r = new Rejection(5, RejectionReason.INVALID_JSON, "x", Map.of());
        writer.write(new BatchWrite(job.id(), "w1", 1, 5, List.of(), List.of(r)));
        writer.write(new BatchWrite(job.id(), "w1", 2, 6, List.of(ItFixtures.scored(6, "txn-6")), List.of(r)));

        assertThat(pg.db().fetchCount(REJECTED_RECORDS)).isEqualTo(1);
    }

    @Test
    void overLongRejectionMessagesAreTruncatedToTheColumnWidth() {
        ImportJob job = processing("k1", "w1");
        writer.write(new BatchWrite(job.id(), "w1", 1, 1, List.of(),
                List.of(new Rejection(1, RejectionReason.INVALID_JSON, "m".repeat(2000), Map.of()))));

        assertThat(pg.db().selectFrom(REJECTED_RECORDS).fetchSingle().getMessage()).hasSize(512);
    }

    @Test
    void largeBatchesGoThroughInOneRoundTripPerStatement() {
        ImportJob job = processing("k1", "w1");
        List<ScoredTransaction> candidates = new java.util.ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            candidates.add(ItFixtures.scored(i, "txn-" + i));
        }

        BatchOutcome outcome = writer.write(new BatchWrite(job.id(), "w1", 1, 1000, candidates, List.of()));

        assertThat(outcome.accepted()).isEqualTo(1000);
        assertThat(pg.db().fetchCount(TRANSACTIONS)).isEqualTo(1000);
    }
}
