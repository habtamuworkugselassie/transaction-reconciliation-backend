package com.reconciliation.infrastructure.persistence;

import com.reconciliation.domain.model.ImportStatus;
import org.jooq.DSLContext;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.reconciliation.infrastructure.persistence.Timestamps.odt;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;

/**
 * Queue-related reads and transitions on the imports table that are not part of the
 * ImportRepository port: enqueueing (uploading -> pending), depth gauges and the
 * sweep for work that needs re-dispatching (lost events, dead workers).
 */
public class JooqImportJobStore {

    private final DSLContext db;

    public JooqImportJobStore(DSLContext db) {
        this.db = db;
    }

    /** uploading -> pending. */
    public boolean markPending(UUID importId, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.PENDING.wireName())
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(importId))
                .and(IMPORTS.STATUS.eq(ImportStatus.UPLOADING.wireName()))
                .execute() == 1;
    }

    public long queuedCount() {
        return db.fetchCount(IMPORTS, IMPORTS.STATUS.eq(ImportStatus.PENDING.wireName()));
    }

    public long inFlightCount(Instant now) {
        return db.fetchCount(IMPORTS, IMPORTS.STATUS.in(ImportStatus.PROCESSING.wireName(), ImportStatus.CANCELLING.wireName())
                .and(IMPORTS.LEASE_EXPIRES_AT.ge(odt(now))));
    }

    /**
     * Imports that should be re-dispatched: pending rows nobody has claimed within
     * {@code pendingOlderThan} (the event was lost or no consumer was alive), and leased
     * rows whose lease expired before {@code now} (the worker died mid-import).
     */
    public List<UUID> findNeedingDispatch(Instant now, Instant pendingOlderThan, int limit) {
        return db.select(IMPORTS.ID).from(IMPORTS)
                .where(IMPORTS.STATUS.eq(ImportStatus.PENDING.wireName()).and(IMPORTS.UPDATED_AT.lt(odt(pendingOlderThan))))
                .or(IMPORTS.STATUS.in(ImportStatus.PROCESSING.wireName(), ImportStatus.CANCELLING.wireName())
                        .and(IMPORTS.LEASE_EXPIRES_AT.lt(odt(now))))
                .orderBy(IMPORTS.CREATED_AT)
                .limit(limit)
                .fetch(IMPORTS.ID);
    }
}
