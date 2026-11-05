package com.reconciliation.infrastructure.persistence;

import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.infrastructure.persistence.jooq.tables.Imports;
import com.reconciliation.infrastructure.persistence.jooq.tables.records.ImportsRecord;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static com.reconciliation.infrastructure.persistence.Timestamps.instant;
import static com.reconciliation.infrastructure.persistence.Timestamps.odt;
import static com.reconciliation.infrastructure.persistence.Timestamps.truncate;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORT_ATTEMPTS;

/**
 * PostgreSQL implementation on jOOQ. Every transition is a single conditional UPDATE so
 * it is atomic without explicit locking; claiming uses {@code FOR UPDATE SKIP LOCKED}
 * so competing workers skip rows another worker is claiming instead of blocking.
 */
public class JooqImportRepository implements ImportRepository {

    private final DSLContext db;

    public JooqImportRepository(DSLContext db) {
        this.db = db;
    }

    @Override
    public Reservation reserve(ImportJob job) {
        Optional<ImportsRecord> inserted = db.insertInto(IMPORTS)
                .set(IMPORTS.ID, job.id())
                .set(IMPORTS.IDEMPOTENCY_KEY, job.idempotencyKey())
                .set(IMPORTS.PROVIDER_ID, job.providerId())
                .set(IMPORTS.STATUS, job.status().wireName())
                .set(IMPORTS.ORIGINAL_FILENAME, job.originalFilename())
                .set(IMPORTS.CREATED_AT, odt(job.createdAt()))
                .set(IMPORTS.UPDATED_AT, odt(job.createdAt()))
                .onConflict(IMPORTS.IDEMPOTENCY_KEY).doNothing()
                .returning()
                .fetchOptional();
        if (inserted.isPresent()) {
            return new Reservation.Created(map(inserted.get()));
        }
        ImportsRecord existing = db.selectFrom(IMPORTS)
                .where(IMPORTS.IDEMPOTENCY_KEY.eq(job.idempotencyKey()))
                .fetchSingle();
        return new Reservation.Existing(map(existing));
    }

    @Override
    public Optional<ImportJob> reopenFailedUpload(UUID id, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.UPLOADING.wireName())
                .setNull(IMPORTS.FAILURE_REASON)
                .setNull(IMPORTS.FAILURE_MESSAGE)
                .setNull(IMPORTS.STORAGE_KEY)
                .setNull(IMPORTS.FILE_SIZE_BYTES)
                .setNull(IMPORTS.CONTENT_SHA256)
                .setNull(IMPORTS.COMPLETED_AT)
                .set(IMPORTS.CREATED_AT, odt(now))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.STATUS.eq(ImportStatus.FAILED.wireName()))
                .and(IMPORTS.STORAGE_KEY.isNull())
                .returning()
                .fetchOptional()
                .map(JooqImportRepository::map);
    }

    @Override
    public Optional<ImportJob> findById(UUID id) {
        return db.selectFrom(IMPORTS).where(IMPORTS.ID.eq(id)).fetchOptional().map(JooqImportRepository::map);
    }

    @Override
    public boolean markUploaded(UUID id, String storageKey, long sizeBytes, String sha256, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STORAGE_KEY, storageKey)
                .set(IMPORTS.FILE_SIZE_BYTES, sizeBytes)
                .set(IMPORTS.CONTENT_SHA256, sha256)
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.STATUS.eq(ImportStatus.UPLOADING.wireName()))
                .execute() == 1;
    }

    @Override
    public boolean markUploadFailed(UUID id, String reason, String message, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.FAILED.wireName())
                .set(IMPORTS.FAILURE_REASON, reason)
                .set(IMPORTS.FAILURE_MESSAGE, truncate(message, 512))
                .set(IMPORTS.COMPLETED_AT, odt(now))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.STATUS.eq(ImportStatus.UPLOADING.wireName()))
                .execute() == 1;
    }

    @Override
    public int failStaleUploads(Instant olderThan, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.FAILED.wireName())
                .set(IMPORTS.FAILURE_REASON, "UPLOAD_INTERRUPTED")
                .set(IMPORTS.FAILURE_MESSAGE, "Upload did not complete")
                .set(IMPORTS.COMPLETED_AT, odt(now))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.STATUS.eq(ImportStatus.UPLOADING.wireName()))
                .and(IMPORTS.CREATED_AT.lt(odt(olderThan)))
                .execute();
    }

    // ---- job control -------------------------------------------------------

    @Override
    public Optional<ImportJob> claim(UUID id, String workerId, Duration leaseDuration, Instant now) {
        Imports c = IMPORTS.as("c");
        return claimWhere(workerId, leaseDuration, now,
                DSL.select(c.ID).from(c).where(c.ID.eq(id)).and(claimable(c, now)).forUpdate().skipLocked());
    }

    @Override
    public Optional<ImportJob> claimNext(String workerId, Duration leaseDuration, Instant now) {
        Imports c = IMPORTS.as("c");
        return claimWhere(workerId, leaseDuration, now,
                DSL.select(c.ID).from(c).where(claimable(c, now)).orderBy(c.CREATED_AT).limit(1).forUpdate().skipLocked());
    }

    private Optional<ImportJob> claimWhere(String workerId, Duration leaseDuration, Instant now,
                                           org.jooq.Select<? extends org.jooq.Record1<UUID>> candidate) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, DSL.when(IMPORTS.STATUS.eq(ImportStatus.CANCELLING.wireName()),
                                DSL.inline(ImportStatus.CANCELLING.wireName()))
                        .otherwise(DSL.inline(ImportStatus.PROCESSING.wireName())))
                .set(IMPORTS.LEASE_OWNER, workerId)
                .set(IMPORTS.LEASE_EXPIRES_AT, odt(now.plus(leaseDuration)))
                .set(IMPORTS.ATTEMPT_COUNT, IMPORTS.ATTEMPT_COUNT.plus(1))
                .set(IMPORTS.STARTED_AT, DSL.coalesce(IMPORTS.STARTED_AT, DSL.val(odt(now))))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(candidate))
                .returning()
                .fetchOptional()
                .map(JooqImportRepository::map);
    }

    /** pending, or leased but the lease has expired (owner died). */
    private static Condition claimable(Imports c, Instant now) {
        return c.STATUS.eq(ImportStatus.PENDING.wireName())
                .or(c.STATUS.in(ImportStatus.PROCESSING.wireName(), ImportStatus.CANCELLING.wireName())
                        .and(c.LEASE_EXPIRES_AT.lt(odt(now))));
    }

    @Override
    public boolean extendLease(UUID id, String workerId, Duration leaseDuration, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.LEASE_EXPIRES_AT, odt(now.plus(leaseDuration)))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.LEASE_OWNER.eq(workerId))
                .and(IMPORTS.STATUS.in(ImportStatus.PROCESSING.wireName(), ImportStatus.CANCELLING.wireName()))
                .execute() == 1;
    }

    @Override
    public Optional<ImportJob> requestCancel(UUID id, Instant now) {
        Condition pending = IMPORTS.STATUS.eq(ImportStatus.PENDING.wireName());
        return db.update(IMPORTS)
                .set(IMPORTS.CANCEL_REQUESTED, true)
                .set(IMPORTS.STATUS, DSL.when(pending, DSL.inline(ImportStatus.CANCELLED.wireName()))
                        .otherwise(DSL.inline(ImportStatus.CANCELLING.wireName())))
                .set(IMPORTS.COMPLETED_AT, DSL.when(pending, DSL.val(odt(now))).otherwise(IMPORTS.COMPLETED_AT))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.STATUS.in(ImportStatus.PENDING.wireName(), ImportStatus.PROCESSING.wireName()))
                .returning()
                .fetchOptional()
                .map(JooqImportRepository::map);
    }

    @Override
    public boolean isCancelRequested(UUID id) {
        return db.select(IMPORTS.CANCEL_REQUESTED).from(IMPORTS).where(IMPORTS.ID.eq(id))
                .fetchOptional(IMPORTS.CANCEL_REQUESTED)
                .orElse(true); // a vanished row is treated as "stop"
    }

    @Override
    public boolean complete(UUID id, String workerId, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.COMPLETED.wireName())
                .setNull(IMPORTS.LEASE_OWNER)
                .setNull(IMPORTS.LEASE_EXPIRES_AT)
                .set(IMPORTS.COMPLETED_AT, odt(now))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.LEASE_OWNER.eq(workerId))
                .and(IMPORTS.STATUS.eq(ImportStatus.PROCESSING.wireName()))
                .execute() == 1;
    }

    @Override
    public boolean fail(UUID id, String workerId, String reason, String message, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.FAILED.wireName())
                .set(IMPORTS.FAILURE_REASON, reason)
                .set(IMPORTS.FAILURE_MESSAGE, truncate(message, 512))
                .setNull(IMPORTS.LEASE_OWNER)
                .setNull(IMPORTS.LEASE_EXPIRES_AT)
                .set(IMPORTS.COMPLETED_AT, odt(now))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.LEASE_OWNER.eq(workerId))
                .and(IMPORTS.STATUS.in(ImportStatus.PROCESSING.wireName(), ImportStatus.CANCELLING.wireName()))
                .execute() == 1;
    }

    @Override
    public boolean markCancelled(UUID id, String workerId, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.CANCELLED.wireName())
                .setNull(IMPORTS.LEASE_OWNER)
                .setNull(IMPORTS.LEASE_EXPIRES_AT)
                .set(IMPORTS.COMPLETED_AT, odt(now))
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.LEASE_OWNER.eq(workerId))
                .and(IMPORTS.STATUS.in(ImportStatus.PROCESSING.wireName(), ImportStatus.CANCELLING.wireName()))
                .execute() == 1;
    }

    @Override
    public boolean release(UUID id, String workerId, Instant now) {
        return db.update(IMPORTS)
                .set(IMPORTS.STATUS, ImportStatus.PENDING.wireName())
                .setNull(IMPORTS.LEASE_OWNER)
                .setNull(IMPORTS.LEASE_EXPIRES_AT)
                .set(IMPORTS.UPDATED_AT, odt(now))
                .where(IMPORTS.ID.eq(id))
                .and(IMPORTS.LEASE_OWNER.eq(workerId))
                .and(IMPORTS.STATUS.eq(ImportStatus.PROCESSING.wireName()))
                .execute() == 1;
    }

    @Override
    public long recordAttemptStart(UUID id, int attempt, String workerId, Instant now) {
        return db.insertInto(IMPORT_ATTEMPTS)
                .set(IMPORT_ATTEMPTS.IMPORT_ID, id)
                .set(IMPORT_ATTEMPTS.ATTEMPT, attempt)
                .set(IMPORT_ATTEMPTS.WORKER_ID, workerId)
                .set(IMPORT_ATTEMPTS.STARTED_AT, odt(now))
                .onConflict(IMPORT_ATTEMPTS.IMPORT_ID, IMPORT_ATTEMPTS.ATTEMPT)
                .doUpdate()
                .set(IMPORT_ATTEMPTS.WORKER_ID, workerId)
                .set(IMPORT_ATTEMPTS.STARTED_AT, odt(now))
                .setNull(IMPORT_ATTEMPTS.ENDED_AT)
                .setNull(IMPORT_ATTEMPTS.OUTCOME)
                .returning(IMPORT_ATTEMPTS.ID)
                .fetchSingle(IMPORT_ATTEMPTS.ID);
    }

    @Override
    public void recordAttemptEnd(long attemptRowId, String outcome, String failureMessage, Instant now) {
        db.update(IMPORT_ATTEMPTS)
                .set(IMPORT_ATTEMPTS.ENDED_AT, odt(now))
                .set(IMPORT_ATTEMPTS.OUTCOME, outcome)
                .set(IMPORT_ATTEMPTS.FAILURE_MESSAGE, truncate(failureMessage, 512))
                .where(IMPORT_ATTEMPTS.ID.eq(attemptRowId))
                .execute();
    }

    static ImportJob map(ImportsRecord r) {
        return new ImportJob(
                r.getId(),
                r.getIdempotencyKey(),
                r.getProviderId(),
                ImportStatus.fromWireName(r.getStatus()),
                r.getOriginalFilename(),
                r.getStorageKey(),
                r.getFileSizeBytes(),
                r.getContentSha256(),
                new ImportProgress(r.getProcessedCount(), r.getAcceptedCount(), r.getRejectedCount(), r.getDuplicateCount()),
                r.getLastLineCommitted(),
                r.getCancelRequested(),
                r.getAttemptCount(),
                r.getLeaseOwner(),
                instant(r.getLeaseExpiresAt()),
                r.getFailureReason(),
                r.getFailureMessage(),
                instant(r.getCreatedAt()),
                instant(r.getUpdatedAt()),
                instant(r.getStartedAt()),
                instant(r.getCompletedAt()));
    }
}
