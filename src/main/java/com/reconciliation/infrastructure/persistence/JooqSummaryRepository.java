package com.reconciliation.infrastructure.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.SummaryRepository;
import com.reconciliation.domain.model.ImportSummary;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.TableField;
import org.jooq.impl.DSL;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.reconciliation.infrastructure.persistence.Timestamps.odt;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORT_SUMMARIES;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.TRANSACTIONS;

/**
 * Derives summaries with GROUP BY over transactions (indexed by import_id) and stores
 * the final result as JSONB. Rejected/duplicate totals come from the import row's
 * counters, which were committed in the same transactions as the records themselves.
 */
public class JooqSummaryRepository implements SummaryRepository {

    private static final Field<Integer> COUNT = DSL.count().as("c");
    private static final Field<BigDecimal> TOTAL = DSL.sum(TRANSACTIONS.AMOUNT).as("total");

    private final DSLContext db;
    private final ObjectMapper json;

    public JooqSummaryRepository(DSLContext db, ObjectMapper json) {
        this.db = db;
        this.json = json;
    }

    @Override
    public ImportSummary compute(UUID importId) {
        var counters = db.select(IMPORTS.ACCEPTED_COUNT, IMPORTS.REJECTED_COUNT, IMPORTS.DUPLICATE_COUNT)
                .from(IMPORTS).where(IMPORTS.ID.eq(importId)).fetchSingle();
        var totals = new ImportSummary.Totals(counters.value1(), counters.value2(), counters.value3());

        List<ImportSummary.CurrencyTotal> byCurrency = db.select(TRANSACTIONS.CURRENCY, COUNT, TOTAL)
                .from(TRANSACTIONS)
                .where(TRANSACTIONS.IMPORT_ID.eq(importId))
                .groupBy(TRANSACTIONS.CURRENCY)
                .orderBy(TRANSACTIONS.CURRENCY)
                .fetch(r -> new ImportSummary.CurrencyTotal(r.value1().trim(), r.value2(), r.value3()));

        long low = 0;
        long medium = 0;
        long high = 0;
        for (var r : db.select(TRANSACTIONS.RISK_LEVEL, COUNT).from(TRANSACTIONS)
                .where(TRANSACTIONS.IMPORT_ID.eq(importId)).groupBy(TRANSACTIONS.RISK_LEVEL).fetch()) {
            switch (r.value1()) {
                case "low" -> low = r.value2();
                case "medium" -> medium = r.value2();
                case "high" -> high = r.value2();
                default -> { }
            }
        }

        return new ImportSummary(totals, byCurrency, new ImportSummary.RiskTotals(low, medium, high),
                topGroups(importId, TRANSACTIONS.MERCHANT_ID), topGroups(importId, TRANSACTIONS.ACCOUNT_ID));
    }

    private List<ImportSummary.GroupTotal> topGroups(UUID importId, TableField<?, String> key) {
        return db.select(key, COUNT, TOTAL)
                .from(TRANSACTIONS)
                .where(TRANSACTIONS.IMPORT_ID.eq(importId))
                .groupBy(key)
                .orderBy(COUNT.desc(), key)
                .limit(MAX_GROUP_ENTRIES)
                .fetch(r -> new ImportSummary.GroupTotal(r.value1(), r.value2(), r.value3()));
    }

    @Override
    public Optional<ImportSummary> findFinal(UUID importId) {
        return db.select(IMPORT_SUMMARIES.SUMMARY).from(IMPORT_SUMMARIES)
                .where(IMPORT_SUMMARIES.IMPORT_ID.eq(importId))
                .fetchOptional(IMPORT_SUMMARIES.SUMMARY)
                .map(j -> fromJson(j.data()));
    }

    @Override
    public void saveFinal(UUID importId, ImportSummary summary, Instant now) {
        db.insertInto(IMPORT_SUMMARIES)
                .set(IMPORT_SUMMARIES.IMPORT_ID, importId)
                .set(IMPORT_SUMMARIES.SUMMARY, JSON.valueOf(toJson(summary)))
                .set(IMPORT_SUMMARIES.COMPUTED_AT, odt(now))
                // Upsert: a summary stored before a manual retry is replaced by the one computed
                // after the import finishes again.
                .onConflict(IMPORT_SUMMARIES.IMPORT_ID).doUpdate()
                .set(IMPORT_SUMMARIES.SUMMARY, JSON.valueOf(toJson(summary)))
                .set(IMPORT_SUMMARIES.COMPUTED_AT, odt(now))
                .execute();
    }

    private String toJson(ImportSummary summary) {
        try {
            return json.writeValueAsString(summary);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise summary", e);
        }
    }

    private ImportSummary fromJson(String text) {
        try {
            return json.readValue(text, ImportSummary.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialise summary", e);
        }
    }
}
