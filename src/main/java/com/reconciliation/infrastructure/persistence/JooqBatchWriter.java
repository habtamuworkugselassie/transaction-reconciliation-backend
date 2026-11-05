package com.reconciliation.infrastructure.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.infrastructure.persistence.jooq.tables.records.ImportsRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.reconciliation.infrastructure.persistence.Timestamps.odt;
import static com.reconciliation.infrastructure.persistence.Timestamps.truncate;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.REJECTED_RECORDS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.TRANSACTIONS;

/**
 * One database transaction per batch:
 * <ol>
 *   <li>lock the import row and verify the lease and that the checkpoint is behind this
 *       batch (a batch already committed by an earlier attempt is a no-op);</li>
 *   <li>bulk-insert candidates with {@code ON CONFLICT DO NOTHING RETURNING line_number};
 *       lines not returned are duplicates (within the batch, the file, or history);</li>
 *   <li>classify duplicates as identical or conflicting by comparing fingerprints with the
 *       accepted row, and record them as rejections;</li>
 *   <li>bulk-insert validation rejections;</li>
 *   <li>advance counters and the checkpoint.</li>
 * </ol>
 * Rows travel as PostgreSQL arrays through {@code unnest}, so a batch costs a handful of
 * round trips and a fixed number of bind parameters regardless of its size.
 */
public class JooqBatchWriter implements BatchWriter {

    private final DSLContext db;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final ObjectMapper json = new ObjectMapper();

    public JooqBatchWriter(DSLContext db, TransactionTemplate tx, Clock clock) {
        this.db = db;
        this.tx = tx;
        this.clock = clock;
    }

    @Override
    public BatchOutcome write(BatchWrite batch) {
        return tx.execute(status -> doWrite(batch));
    }

    private BatchOutcome doWrite(BatchWrite batch) {
        OffsetDateTime now = odt(clock.now());

        // 1. lease + checkpoint guard, under row lock
        ImportsRecord row = db.selectFrom(IMPORTS).where(IMPORTS.ID.eq(batch.importId())).forUpdate().fetchOne();
        if (row == null || !batch.workerId().equals(row.getLeaseOwner())
                || !(ImportStatus.PROCESSING.wireName().equals(row.getStatus())
                     || ImportStatus.CANCELLING.wireName().equals(row.getStatus()))) {
            throw new LeaseLostException(batch.importId(), batch.workerId());
        }
        ImportProgress before = new ImportProgress(row.getProcessedCount(), row.getAcceptedCount(),
                row.getRejectedCount(), row.getDuplicateCount());
        if (row.getLastLineCommitted() >= batch.lastLineNumber()) {
            return new BatchOutcome(0, 0, 0, before, true);
        }

        // 2. insert candidates; RETURNING tells us which ones were new
        Set<Long> acceptedLines = insertTransactions(batch, now);
        List<Rejection> rejections = new ArrayList<>(batch.rejections());

        // 3. classify duplicates
        int duplicates = 0;
        if (acceptedLines.size() < batch.candidates().size()) {
            String providerId = batch.candidates().get(0).transaction().providerId();
            List<ScoredTransaction> dupes = batch.candidates().stream()
                    .filter(c -> !acceptedLines.contains(c.transaction().lineNumber())).toList();
            Map<String, String> existing = db.select(TRANSACTIONS.TRANSACTION_ID, TRANSACTIONS.FINGERPRINT)
                    .from(TRANSACTIONS)
                    .where(TRANSACTIONS.PROVIDER_ID.eq(providerId))
                    .and(TRANSACTIONS.TRANSACTION_ID.in(
                            dupes.stream().map(d -> d.transaction().transactionId()).distinct().toList()))
                    .fetchMap(TRANSACTIONS.TRANSACTION_ID, TRANSACTIONS.FINGERPRINT);
            for (ScoredTransaction d : dupes) {
                boolean identical = d.fingerprint().equals(existing.get(d.transaction().transactionId()));
                rejections.add(new Rejection(d.transaction().lineNumber(),
                        identical ? RejectionReason.DUPLICATE_TRANSACTION : RejectionReason.DUPLICATE_TRANSACTION_CONFLICT,
                        identical ? "Transaction already accepted with identical content"
                                  : "Transaction already accepted with different content; first record kept",
                        Map.of("transactionId", d.transaction().transactionId())));
                duplicates++;
            }
        }

        // 4. rejections (validation + duplicates)
        insertRejections(batch.importId(), rejections, now);

        // 5. counters + checkpoint
        int accepted = acceptedLines.size();
        int rejected = batch.rejections().size();
        Record after = db.update(IMPORTS)
                .set(IMPORTS.PROCESSED_COUNT, IMPORTS.PROCESSED_COUNT.plus(accepted + rejected + duplicates))
                .set(IMPORTS.ACCEPTED_COUNT, IMPORTS.ACCEPTED_COUNT.plus(accepted))
                .set(IMPORTS.REJECTED_COUNT, IMPORTS.REJECTED_COUNT.plus(rejected))
                .set(IMPORTS.DUPLICATE_COUNT, IMPORTS.DUPLICATE_COUNT.plus(duplicates))
                .set(IMPORTS.LAST_LINE_COMMITTED, batch.lastLineNumber())
                .set(IMPORTS.UPDATED_AT, now)
                .where(IMPORTS.ID.eq(batch.importId()))
                .returning(IMPORTS.PROCESSED_COUNT, IMPORTS.ACCEPTED_COUNT, IMPORTS.REJECTED_COUNT, IMPORTS.DUPLICATE_COUNT)
                .fetchSingle();
        ImportProgress progress = new ImportProgress(after.get(IMPORTS.PROCESSED_COUNT), after.get(IMPORTS.ACCEPTED_COUNT),
                after.get(IMPORTS.REJECTED_COUNT), after.get(IMPORTS.DUPLICATE_COUNT));
        return new BatchOutcome(accepted, rejected, duplicates, progress, false);
    }

    private Set<Long> insertTransactions(BatchWrite batch, OffsetDateTime now) {
        List<ScoredTransaction> c = batch.candidates();
        if (c.isEmpty()) {
            return Set.of();
        }
        int n = c.size();
        Long[] lines = new Long[n];
        String[] txIds = new String[n];
        String[] accounts = new String[n];
        String[] merchants = new String[n];
        BigDecimal[] amounts = new BigDecimal[n];
        String[] currencies = new String[n];
        OffsetDateTime[] timestamps = new OffsetDateTime[n];
        String[] descriptions = new String[n];
        String[] fingerprints = new String[n];
        Integer[] scores = new Integer[n];
        String[] levels = new String[n];
        for (int i = 0; i < n; i++) {
            ScoredTransaction s = c.get(i);
            var t = s.transaction();
            lines[i] = t.lineNumber();
            txIds[i] = t.transactionId();
            accounts[i] = t.accountId();
            merchants[i] = t.merchantId();
            amounts[i] = t.amount();
            currencies[i] = t.currency();
            timestamps[i] = odt(t.timestamp());
            descriptions[i] = t.description();
            fingerprints[i] = s.fingerprint();
            scores[i] = s.riskScore();
            levels[i] = s.riskLevel().wireName();
        }
        String providerId = c.get(0).transaction().providerId();

        Table<?> u = DSL.table(
                "unnest({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10})"
                        + " as u(line_number, transaction_id, account_id, merchant_id, amount, currency, occurred_at,"
                        + " description, fingerprint, risk_score, risk_level)",
                DSL.val(lines), DSL.val(txIds), DSL.val(accounts), DSL.val(merchants), DSL.val(amounts),
                DSL.val(currencies), DSL.val(timestamps), DSL.val(descriptions), DSL.val(fingerprints),
                DSL.val(scores), DSL.val(levels));
        Field<Long> uLine = DSL.field(DSL.name("u", "line_number"), SQLDataType.BIGINT);
        Field<String> uTxn = DSL.field(DSL.name("u", "transaction_id"), SQLDataType.VARCHAR);
        Field<String> uAcc = DSL.field(DSL.name("u", "account_id"), SQLDataType.VARCHAR);
        Field<String> uMer = DSL.field(DSL.name("u", "merchant_id"), SQLDataType.VARCHAR);
        Field<BigDecimal> uAmt = DSL.field(DSL.name("u", "amount"), SQLDataType.NUMERIC);
        Field<String> uCur = DSL.field(DSL.name("u", "currency"), SQLDataType.VARCHAR);
        Field<OffsetDateTime> uTs = DSL.field(DSL.name("u", "occurred_at"), SQLDataType.TIMESTAMPWITHTIMEZONE);
        Field<String> uDesc = DSL.field(DSL.name("u", "description"), SQLDataType.VARCHAR);
        Field<String> uFp = DSL.field(DSL.name("u", "fingerprint"), SQLDataType.VARCHAR);
        Field<Short> uScore = DSL.field(DSL.name("u", "risk_score"), SQLDataType.INTEGER).cast(SQLDataType.SMALLINT);
        Field<String> uLevel = DSL.field(DSL.name("u", "risk_level"), SQLDataType.VARCHAR);

        List<Long> accepted = db.insertInto(TRANSACTIONS,
                        TRANSACTIONS.IMPORT_ID, TRANSACTIONS.LINE_NUMBER, TRANSACTIONS.PROVIDER_ID,
                        TRANSACTIONS.TRANSACTION_ID, TRANSACTIONS.ACCOUNT_ID, TRANSACTIONS.MERCHANT_ID,
                        TRANSACTIONS.AMOUNT, TRANSACTIONS.CURRENCY, TRANSACTIONS.OCCURRED_AT, TRANSACTIONS.DESCRIPTION,
                        TRANSACTIONS.FINGERPRINT, TRANSACTIONS.FINGERPRINT_VERSION, TRANSACTIONS.RISK_SCORE,
                        TRANSACTIONS.RISK_LEVEL, TRANSACTIONS.CREATED_AT)
                .select(DSL.select(
                                DSL.val(batch.importId()), uLine, DSL.val(providerId), uTxn, uAcc, uMer, uAmt, uCur, uTs, uDesc,
                                uFp, DSL.val((short) c.get(0).fingerprintVersion()), uScore, uLevel, DSL.val(now))
                        .from(u))
                .onConflict(TRANSACTIONS.PROVIDER_ID, TRANSACTIONS.TRANSACTION_ID).doNothing()
                .returningResult(TRANSACTIONS.LINE_NUMBER)
                .fetch(TRANSACTIONS.LINE_NUMBER);
        return new HashSet<>(accepted);
    }

    private void insertRejections(UUID importId, List<Rejection> rejections, OffsetDateTime now) {
        if (rejections.isEmpty()) {
            return;
        }
        int n = rejections.size();
        Long[] lines = new Long[n];
        String[] reasons = new String[n];
        String[] messages = new String[n];
        String[] raw = new String[n];
        for (int i = 0; i < n; i++) {
            Rejection r = rejections.get(i);
            lines[i] = r.lineNumber();
            reasons[i] = r.reason().name();
            messages[i] = truncate(r.message(), 512);
            raw[i] = toJson(r.rawValue());
        }
        Table<?> u = DSL.table("unnest({0}, {1}, {2}, {3}) as u(line_number, reason, message, raw)",
                DSL.val(lines), DSL.val(reasons), DSL.val(messages), DSL.val(raw));
        Field<Long> uLine = DSL.field(DSL.name("u", "line_number"), SQLDataType.BIGINT);
        Field<String> uReason = DSL.field(DSL.name("u", "reason"), SQLDataType.VARCHAR);
        Field<String> uMessage = DSL.field(DSL.name("u", "message"), SQLDataType.VARCHAR);
        Field<JSON> uRaw = DSL.field("{0}::jsonb", JSON.class, DSL.field(DSL.name("u", "raw"), SQLDataType.VARCHAR));

        db.insertInto(REJECTED_RECORDS,
                        REJECTED_RECORDS.IMPORT_ID, REJECTED_RECORDS.LINE_NUMBER, REJECTED_RECORDS.REASON,
                        REJECTED_RECORDS.MESSAGE, REJECTED_RECORDS.RAW_VALUE, REJECTED_RECORDS.CREATED_AT)
                .select(DSL.select(DSL.val(importId), uLine, uReason, uMessage, uRaw, DSL.val(now)).from(u))
                .onConflict(REJECTED_RECORDS.IMPORT_ID, REJECTED_RECORDS.LINE_NUMBER).doNothing()
                .execute();
    }

    private String toJson(Map<String, Object> value) {
        try {
            return json.writeValueAsString(value == null ? Map.of() : value);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
