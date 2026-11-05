package com.reconciliation.application.usecase;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.IdGenerator;
import com.reconciliation.application.port.IdempotencyCache;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.ImportRepository.Reservation;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.UploadLimiter;
import com.reconciliation.application.upload.BoundedHashingInputStream;
import com.reconciliation.application.upload.ContentSniffer;
import com.reconciliation.application.upload.Filenames;
import com.reconciliation.application.upload.UploadSource;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Accepts an upload and turns it into a queued import.
 *
 * <p>Sequence (each step is safe to crash after):
 * <ol>
 *   <li>validate header inputs (no I/O);</li>
 *   <li>acquire an upload slot or fail fast with 429;</li>
 *   <li>reserve the idempotency key: the unique constraint makes concurrent
 *       requests with the same key converge on one row. Exactly one request owns the
 *       upload; every other one (a replay, or a concurrent request that lost the race
 *       while the winner is still streaming) returns that row without reading its
 *       body;</li>
 *   <li>stream the body to storage while counting, hashing and sniffing it;</li>
 *   <li>record file metadata, then enqueue (uploading -> pending).</li>
 * </ol>
 * A crash between 3 and 5 leaves an {@code uploading} row that the stale-upload
 * reaper turns into {@code failed/UPLOAD_INTERRUPTED}; the client may then retry
 * with the same key and the row is re-opened.
 */
public class CreateImportUseCase {

    private static final Logger log = LoggerFactory.getLogger(CreateImportUseCase.class);
    private static final Pattern IDEMPOTENCY_KEY = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
    private static final Pattern PROVIDER_ID = Pattern.compile("[A-Za-z0-9_.-]{1,64}");

    /** What the caller asked for. */
    public record Command(String idempotencyKey, String providerId, String originalFilename, UploadSource body) {}

    /** The import, and whether this call created it or replayed an earlier one. */
    public record Result(ImportJob job, boolean replayed) {}

    private final ImportRepository imports;
    private final FileStorage storage;
    private final ImportJobQueue queue;
    private final UploadLimiter uploadLimiter;
    private final Clock clock;
    private final IdGenerator ids;
    private final MetricsRecorder metrics;
    private final IdempotencyCache idempotencyCache;
    private final Duration idempotencyTtl;
    private final long maxFileSizeBytes;

    public CreateImportUseCase(ImportRepository imports, FileStorage storage, ImportJobQueue queue,
                               UploadLimiter uploadLimiter, Clock clock, IdGenerator ids,
                               MetricsRecorder metrics, IdempotencyCache idempotencyCache, Duration idempotencyTtl,
                               long maxFileSizeBytes) {
        this.imports = imports;
        this.storage = storage;
        this.queue = queue;
        this.uploadLimiter = uploadLimiter;
        this.clock = clock;
        this.ids = ids;
        this.metrics = metrics;
        this.idempotencyCache = idempotencyCache;
        this.idempotencyTtl = idempotencyTtl;
        this.maxFileSizeBytes = maxFileSizeBytes;
    }

    public Result execute(Command command) {
        String key = requireIdempotencyKey(command.idempotencyKey());
        String provider = requireProvider(command.providerId());
        String filename = Filenames.sanitize(command.originalFilename());

        // Fast path for a replay: the cache knows which import this key produced. The
        // state still comes from the database; the cache only skips the reservation.
        Optional<ImportJob> cached = idempotencyCache.find(key).flatMap(imports::findById);
        if (cached.isPresent() && cached.get().status() != ImportStatus.UPLOADING && cached.get().storageKey() != null) {
            metrics.importReplayed();
            return new Result(cached.get(), true);
        }

        if (!uploadLimiter.tryAcquire()) {
            metrics.uploadRejected("concurrency");
            throw new AppException(ErrorCode.TOO_MANY_CONCURRENT_UPLOADS,
                    "Too many uploads in progress; retry shortly",
                    null, Map.of("capacity", uploadLimiter.capacity()));
        }
        try {
            Instant now = clock.now();
            Owned owned = resolveReservation(ImportJob.reserved(ids.newId(), key, provider, filename, now));
            if (!owned.byThisRequest()) {
                // Any state, including an upload still in flight on another request: the
                // key is taken, so this request neither reads its body nor touches the row.
                metrics.importReplayed();
                return new Result(owned.job(), true);
            }
            return new Result(storeAndEnqueue(owned.job(), command.body()), false);
        } finally {
            uploadLimiter.release();
        }
    }

    /** The import row for this key, and whether this request owns the upload of its body. */
    private record Owned(ImportJob job, boolean byThisRequest) {}

    private Owned resolveReservation(ImportJob candidate) {
        Reservation reservation = imports.reserve(candidate);
        if (reservation instanceof Reservation.Created created) {
            return new Owned(created.job(), true);
        }
        ImportJob existing = reservation.job();
        if (existing.status() == ImportStatus.FAILED && existing.storageKey() == null) {
            // Previous attempt with this key never got a complete file: let the client retry.
            // The conditional update means concurrent retries hand ownership to exactly one.
            Optional<ImportJob> reopened = imports.reopenFailedUpload(existing.id(), clock.now());
            if (reopened.isPresent()) {
                return new Owned(reopened.get(), true);
            }
            existing = imports.findById(existing.id()).orElse(existing);
        }
        return new Owned(existing, false);
    }

    private ImportJob storeAndEnqueue(ImportJob job, UploadSource body) {
        String storageKey = "imports/" + job.id() + ".ndjson";
        long size;
        String sha256;
        try (InputStream raw = body.open();
             PushbackInputStream sniffable = ContentSniffer.wrap(raw)) {
            ContentSniffer.Verdict verdict = ContentSniffer.sniff(sniffable);
            switch (verdict) {
                case EMPTY -> throw uploadRejected(job, ErrorCode.IMPORT_FILE_EMPTY, "The uploaded file is empty");
                case BINARY, NOT_JSON -> throw uploadRejected(job, ErrorCode.IMPORT_FILE_TYPE_INVALID,
                        "The uploaded file is not NDJSON");
                case NDJSON -> { }
            }
            BoundedHashingInputStream bounded = new BoundedHashingInputStream(sniffable, maxFileSizeBytes);
            size = storage.store(storageKey, bounded);
            sha256 = bounded.sha256Hex();
        } catch (BoundedHashingInputStream.LimitExceededException e) {
            discard(storageKey);
            throw uploadRejected(job, ErrorCode.IMPORT_FILE_TOO_LARGE, "The uploaded file exceeds the allowed size");
        } catch (AppException e) {
            discard(storageKey);
            throw e;
        } catch (IOException e) {
            // Client went away or storage failed mid-stream. Nothing partial survives.
            discard(storageKey);
            boolean marked = imports.markUploadFailed(job.id(), ErrorCode.UPLOAD_INTERRUPTED.name(),
                    "Upload did not complete", clock.now());
            log.warn("Upload interrupted importId={} marked={} cause={}", job.id(), marked, e.getClass().getSimpleName());
            metrics.uploadRejected("interrupted");
            throw new AppException(ErrorCode.UPLOAD_INTERRUPTED, "Upload did not complete", e,
                    Map.of("importId", job.id().toString()));
        }

        Instant now = clock.now();
        if (!imports.markUploaded(job.id(), storageKey, size, sha256, now)) {
            // Only possible if the reaper failed this row mid-upload (very slow upload).
            discard(storageKey);
            throw new AppException(ErrorCode.IMPORT_STATE_CONFLICT, "Upload took too long and was discarded; retry",
                    null, Map.of("importId", job.id().toString()));
        }
        idempotencyCache.remember(job.idempotencyKey(), job.id(), idempotencyTtl);
        metrics.importCreated(size);
        queue.enqueue(job.id()); // may throw QUEUE_UNAVAILABLE: the import is persisted and will be re-dispatched
        log.info("Import created importId={} provider={} bytes={}", job.id(), job.providerId(), size);
        return imports.findById(job.id()).orElseThrow();
    }

    private AppException uploadRejected(ImportJob job, ErrorCode code, String message) {
        imports.markUploadFailed(job.id(), code.name(), message, clock.now());
        metrics.uploadRejected(code.name().toLowerCase());
        return new AppException(code, message, null, Map.of("importId", job.id().toString()));
    }

    private void discard(String storageKey) {
        try {
            storage.delete(storageKey);
        } catch (IOException e) {
            log.warn("Could not delete partial upload key={}", storageKey, e);
        }
    }

    private static String requireIdempotencyKey(String key) {
        if (key == null || key.isBlank()) {
            throw new AppException(ErrorCode.IDEMPOTENCY_KEY_REQUIRED, "Idempotency-Key header is required");
        }
        if (!IDEMPOTENCY_KEY.matcher(key).matches()) {
            throw new AppException(ErrorCode.IDEMPOTENCY_KEY_INVALID,
                    "Idempotency-Key must be 1-128 characters from [A-Za-z0-9_.:-]");
        }
        return key;
    }

    private static String requireProvider(String provider) {
        if (provider == null || !PROVIDER_ID.matcher(provider).matches()) {
            throw new AppException(ErrorCode.INVALID_PROVIDER,
                    "Provider id must be 1-64 characters from [A-Za-z0-9_.-]");
        }
        return provider;
    }
}
