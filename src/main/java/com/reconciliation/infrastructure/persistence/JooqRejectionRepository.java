package com.reconciliation.infrastructure.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.RejectionRepository;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import org.jooq.DSLContext;
import org.jooq.JSON;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.reconciliation.infrastructure.persistence.jooq.Tables.REJECTED_RECORDS;

/** Keyset pagination over rejected_records using the (import_id, id) index. */
public class JooqRejectionRepository implements RejectionRepository {

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() { };

    private final DSLContext db;
    private final ObjectMapper json;

    public JooqRejectionRepository(DSLContext db, ObjectMapper json) {
        this.db = db;
        this.json = json;
    }

    @Override
    public Page list(UUID importId, long afterId, int limit) {
        record Row(long id, Rejection rejection) {}
        List<Row> rows = db.select(REJECTED_RECORDS.ID, REJECTED_RECORDS.LINE_NUMBER, REJECTED_RECORDS.REASON,
                        REJECTED_RECORDS.MESSAGE, REJECTED_RECORDS.RAW_VALUE)
                .from(REJECTED_RECORDS)
                .where(REJECTED_RECORDS.IMPORT_ID.eq(importId))
                .and(REJECTED_RECORDS.ID.gt(afterId))
                .orderBy(REJECTED_RECORDS.ID)
                .limit(limit + 1)
                .fetch(r -> new Row(r.value1(), new Rejection(r.value2(), RejectionReason.valueOf(r.value3()),
                        r.value4(), parse(r.value5()))));
        boolean more = rows.size() > limit;
        List<Rejection> items = new ArrayList<>(Math.min(rows.size(), limit));
        for (int i = 0; i < Math.min(rows.size(), limit); i++) {
            items.add(rows.get(i).rejection());
        }
        return new Page(items, more ? rows.get(limit - 1).id() : null);
    }

    private Map<String, Object> parse(JSON raw) {
        if (raw == null || raw.data() == null) {
            return Map.of();
        }
        try {
            return json.readValue(raw.data(), MAP);
        } catch (JsonProcessingException e) {
            return Map.of();
        }
    }
}
