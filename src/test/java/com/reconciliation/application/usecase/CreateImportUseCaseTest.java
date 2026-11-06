package com.reconciliation.application.usecase;

import com.reconciliation.application.usecase.CreateImportUseCase.Command;
import com.reconciliation.application.usecase.CreateImportUseCase.Result;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.infrastructure.concurrency.SemaphoreUploadLimiter;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryFileStorage;
import com.reconciliation.testing.InMemoryImportJobQueue;
import com.reconciliation.testing.InMemoryImportRepository;
import com.reconciliation.testing.RecordingMetrics;
import com.reconciliation.testing.SequentialIdGenerator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The create-import use case wired entirely with in-memory fakes: no database, no file
 * system, no broker. Every production port is replaced.
 */
class CreateImportUseCaseTest {

    private static final long MAX_BYTES = 1024;

    private final FixedClock clock = new FixedClock();
    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final InMemoryFileStorage storage = new InMemoryFileStorage();
    private final InMemoryImportJobQueue queue = new InMemoryImportJobQueue(imports, clock, Duration.ofSeconds(30));
    private final RecordingMetrics metrics = new RecordingMetrics();
    private final SemaphoreUploadLimiter limiter = new SemaphoreUploadLimiter(2);
    private final Map<String, UUID> idempotencyCache = new java.util.concurrent.ConcurrentHashMap<>();
    private final CreateImportUseCase useCase = new CreateImportUseCase(imports, storage, queue, limiter, clock,
            new SequentialIdGenerator(), metrics, new com.reconciliation.application.port.IdempotencyCache() {
                @Override public Optional<UUID> find(String key) { return Optional.ofNullable(idempotencyCache.get(key)); }
                @Override public void remember(String key, UUID importId, Duration ttl) { idempotencyCache.put(key, importId); }
            }, Duration.ofHours(1), MAX_BYTES);

    private static Command command(String key, String body) {
        return new Command(key, Fixtures.PROVIDER, "transactions.ndjson",
                () -> new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void createsAnImportStoresTheFileAndEnqueuesIt() {
        Result result = useCase.execute(command("key-1", Fixtures.validLines(3)));

        assertThat(result.replayed()).isFalse();
        ImportJob job = result.job();
        assertThat(job.id()).isEqualTo(SequentialIdGenerator.id(1));
        assertThat(job.status()).isEqualTo(ImportStatus.PENDING);
        assertThat(job.providerId()).isEqualTo(Fixtures.PROVIDER);
        assertThat(job.originalFilename()).isEqualTo("transactions.ndjson");
        assertThat(job.storageKey()).isEqualTo("imports/" + job.id() + ".ndjson");
        assertThat(job.fileSizeBytes()).isEqualTo(Fixtures.validLines(3).length());
        assertThat(job.contentSha256()).hasSize(64);
        assertThat(job.createdAt()).isEqualTo(FixedClock.START);
        assertThat(storage.text(job.storageKey())).isEqualTo(Fixtures.validLines(3));
        assertThat(queue.published).containsExactly(job.id());
        assertThat(metrics.events).contains("importCreated:" + job.fileSizeBytes());
        assertThat(limiter.available()).as("upload slot released").isEqualTo(2);
    }

    @Test
    void sameKeyReplaysTheOriginalImportWithoutReadingTheBodyAgain() {
        Result first = useCase.execute(command("key-1", Fixtures.validLines(2)));
        AtomicInteger opened = new AtomicInteger();
        Command replay = new Command("key-1", Fixtures.PROVIDER, "other.ndjson", () -> {
            opened.incrementAndGet();
            return InputStream.nullInputStream();
        });

        Result second = useCase.execute(replay);

        assertThat(second.replayed()).isTrue();
        assertThat(second.job().id()).isEqualTo(first.job().id());
        assertThat(opened.get()).as("body of a replayed request is never opened").isZero();
        assertThat(imports.size()).isEqualTo(1);
        assertThat(storage.size()).isEqualTo(1);
        assertThat(queue.published).hasSize(1);
        assertThat(metrics.count("importReplayed")).isEqualTo(1);
    }

    @Test
    void replayWorksWithoutTheCacheBecauseTheRepositoryReservationIsTheGuarantee() {
        Result first = useCase.execute(command("key-1", Fixtures.validLines(2)));
        idempotencyCache.clear();

        Result second = useCase.execute(command("key-1", Fixtures.validLines(9)));

        assertThat(second.replayed()).isTrue();
        assertThat(second.job().id()).isEqualTo(first.job().id());
        assertThat(imports.size()).isEqualTo(1);
    }

    @Test
    void concurrentRequestsWithTheSameKeyConvergeOnOneImport() throws Exception {
        int threads = 8;
        SemaphoreUploadLimiter wide = new SemaphoreUploadLimiter(threads);
        CreateImportUseCase concurrent = new CreateImportUseCase(imports, storage, queue, wide, clock,
                new SequentialIdGenerator(), metrics, com.reconciliation.application.port.IdempotencyCache.NONE,
                Duration.ofHours(1), MAX_BYTES);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Result>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                go.await();
                return concurrent.execute(command("same-key", Fixtures.validLines(1)));
            }));
        }
        go.countDown();
        List<Result> results = new ArrayList<>();
        for (Future<Result> f : futures) {
            results.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertThat(results).extracting(r -> r.job().id()).containsOnly(results.get(0).job().id());
        assertThat(results.stream().filter(r -> !r.replayed()).count()).isEqualTo(1);
        assertThat(imports.size()).isEqualTo(1);
        assertThat(storage.size()).isEqualTo(1);
        assertThat(queue.published).hasSize(1);
    }

    @Test
    void missingOrMalformedIdempotencyKeyIsRejectedBeforeAnyIo() {
        assertThatThrownBy(() -> useCase.execute(command(null, "{}")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IDEMPOTENCY_KEY_REQUIRED);
        assertThatThrownBy(() -> useCase.execute(command("  ", "{}")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IDEMPOTENCY_KEY_REQUIRED);
        assertThatThrownBy(() -> useCase.execute(command("has space", "{}")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IDEMPOTENCY_KEY_INVALID);
        assertThatThrownBy(() -> useCase.execute(command("k".repeat(129), "{}")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IDEMPOTENCY_KEY_INVALID);
        assertThat(imports.size()).isZero();
        assertThat(storage.size()).isZero();
    }

    @Test
    void invalidProviderIsRejected() {
        Command bad = new Command("key-1", "bad provider!", "f", () -> InputStream.nullInputStream());
        assertThatThrownBy(() -> useCase.execute(bad))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.INVALID_PROVIDER);
    }

    @Test
    void emptyFileIsRejectedAndTheImportIsMarkedFailed() {
        assertThatThrownBy(() -> useCase.execute(command("key-1", "")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IMPORT_FILE_EMPTY);

        ImportJob job = imports.get(SequentialIdGenerator.id(1));
        assertThat(job.status()).isEqualTo(ImportStatus.FAILED);
        assertThat(job.failureReason()).isEqualTo("IMPORT_FILE_EMPTY");
        assertThat(job.storageKey()).isNull();
        assertThat(storage.size()).isZero();
        assertThat(queue.published).isEmpty();
    }

    @Test
    void nonNdjsonContentIsRejectedRegardlessOfFilename() {
        assertThatThrownBy(() -> useCase.execute(command("csv", "id,amount\n1,2\n")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IMPORT_FILE_TYPE_INVALID);
        assertThatThrownBy(() -> useCase.execute(command("zip", "PK\u0003\u0004binary")))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IMPORT_FILE_TYPE_INVALID);
        assertThat(storage.size()).isZero();
        assertThat(metrics.count("uploadRejected:import_file_type_invalid")).isEqualTo(2);
    }

    @Test
    void oversizedFileIsCutOffMidStreamAndNothingPartialSurvives() {
        String big = Fixtures.validLines(50); // well over 1024 bytes
        assertThat(big.length()).isGreaterThan((int) MAX_BYTES);

        assertThatThrownBy(() -> useCase.execute(command("key-1", big)))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IMPORT_FILE_TOO_LARGE);

        assertThat(storage.size()).isZero();
        assertThat(imports.get(SequentialIdGenerator.id(1)).status()).isEqualTo(ImportStatus.FAILED);
        assertThat(queue.published).isEmpty();
    }

    @Test
    void interruptedUploadIsMarkedFailedAndCanBeRetriedWithTheSameKey() {
        InputStream broken = new InputStream() {
            int n;
            @Override public int read() throws IOException {
                if (n++ < 5) return '{';
                throw new IOException("connection reset");
            }
        };
        Command interrupted = new Command("key-1", Fixtures.PROVIDER, "f.ndjson", () -> broken);

        assertThatThrownBy(() -> useCase.execute(interrupted))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.UPLOAD_INTERRUPTED);
        ImportJob failed = imports.get(SequentialIdGenerator.id(1));
        assertThat(failed.status()).isEqualTo(ImportStatus.FAILED);
        assertThat(failed.failureReason()).isEqualTo("UPLOAD_INTERRUPTED");
        assertThat(storage.size()).isZero();
        assertThat(metrics.count("uploadRejected:interrupted")).isEqualTo(1);

        // Retry with the same key: the failed row is re-opened, not duplicated.
        Result retry = useCase.execute(command("key-1", Fixtures.validLines(1)));

        assertThat(retry.replayed()).isFalse();
        assertThat(retry.job().id()).isEqualTo(failed.id());
        assertThat(retry.job().status()).isEqualTo(ImportStatus.PENDING);
        assertThat(imports.size()).isEqualTo(1);
        assertThat(queue.published).containsExactly(failed.id());
    }

    @Test
    void tooManyConcurrentUploadsAreRefusedImmediatelyWith429() {
        limiter.tryAcquire();
        limiter.tryAcquire(); // both slots busy

        assertThatThrownBy(() -> useCase.execute(command("key-1", Fixtures.validLines(1))))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.TOO_MANY_CONCURRENT_UPLOADS);

        assertThat(imports.size()).as("rejected before reserving anything").isZero();
        assertThat(metrics.count("uploadRejected:concurrency")).isEqualTo(1);
    }

    @Test
    void providerOverItsRateLimitIsRefusedWith429BeforeAnythingIsReservedOrRead() {
        List<String> asked = new java.util.ArrayList<>();
        CreateImportUseCase limited = new CreateImportUseCase(imports, storage, queue, limiter, clock,
                new SequentialIdGenerator(), metrics, com.reconciliation.application.port.IdempotencyCache.NONE,
                Duration.ofHours(1), MAX_BYTES, provider -> {
                    asked.add(provider);
                    return new com.reconciliation.application.port.UploadRateLimiter.Decision(false, 42);
                });
        java.util.concurrent.atomic.AtomicBoolean bodyRead = new java.util.concurrent.atomic.AtomicBoolean();
        Command cmd = new Command("rate-1", Fixtures.PROVIDER, "f.ndjson", () -> {
            bodyRead.set(true);
            return new ByteArrayInputStream(Fixtures.validLines(1).getBytes(StandardCharsets.UTF_8));
        });

        assertThatThrownBy(() -> limited.execute(cmd))
                .isInstanceOfSatisfying(AppException.class, e -> {
                    assertThat(e.code()).isEqualTo(ErrorCode.PROVIDER_RATE_LIMITED);
                    assertThat(e.diagnostics()).containsEntry("retryAfterSeconds", 42L);
                });
        assertThat(asked).containsExactly(Fixtures.PROVIDER);
        assertThat(imports.size()).isZero();
        assertThat(bodyRead).isFalse();
        assertThat(limiter.available()).isEqualTo(2);
        assertThat(metrics.events).contains("uploadRejected:provider_rate_limit");
    }

    @Test
    void uploadSlotIsReleasedEvenWhenTheUploadFails() {
        assertThatThrownBy(() -> useCase.execute(command("key-1", ""))).isInstanceOf(AppException.class);
        assertThat(limiter.available()).isEqualTo(2);
    }

    @Test
    void brokerOutageAfterPersistingSurfacesAsQueueUnavailableButKeepsTheImport() {
        queue.failEnqueueWith = new AppException(ErrorCode.QUEUE_UNAVAILABLE, "broker down");

        assertThatThrownBy(() -> useCase.execute(command("key-1", Fixtures.validLines(1))))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.QUEUE_UNAVAILABLE);

        ImportJob job = imports.get(SequentialIdGenerator.id(1));
        assertThat(job.status()).as("durably pending; the sweep re-dispatches it").isEqualTo(ImportStatus.PENDING);
        assertThat(storage.contains(job.storageKey())).isTrue();
    }

    @Test
    void filenameIsSanitisedBeforeBeingPersisted() {
        Command cmd = new Command("key-1", Fixtures.PROVIDER, "../../etc/passwd",
                () -> new ByteArrayInputStream(Fixtures.validLines(1).getBytes(StandardCharsets.UTF_8)));

        assertThat(useCase.execute(cmd).job().originalFilename()).isEqualTo("passwd");
    }

    @Test
    void secondRequestArrivingWhileTheFirstIsStillUploadingGetsTheSameImportAndTouchesNothing() throws Exception {
        CountDownLatch firstIsStreaming = new CountDownLatch(1);
        CountDownLatch letFirstFinish = new CountDownLatch(1);
        byte[] bytes = Fixtures.validLines(2).getBytes(StandardCharsets.UTF_8);
        InputStream slow = new InputStream() {
            int i;
            @Override public int read() throws IOException {
                if (i == 0) {
                    firstIsStreaming.countDown();
                    try { letFirstFinish.await(); } catch (InterruptedException e) { throw new IOException(e); }
                }
                return i < bytes.length ? bytes[i++] & 0xFF : -1;
            }
        };
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<Result> first = pool.submit(() -> useCase.execute(new Command("key-1", Fixtures.PROVIDER, "slow.ndjson", () -> slow)));
        assertThat(firstIsStreaming.await(5, TimeUnit.SECONDS)).isTrue();

        AtomicInteger opened = new AtomicInteger();
        Result second = useCase.execute(new Command("key-1", Fixtures.PROVIDER, "fast.ndjson", () -> {
            opened.incrementAndGet();
            return InputStream.nullInputStream();
        }));

        assertThat(second.replayed()).isTrue();
        assertThat(second.job().status()).isEqualTo(ImportStatus.UPLOADING);
        assertThat(opened.get()).isZero();

        letFirstFinish.countDown();
        Result winner = first.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(winner.replayed()).isFalse();
        assertThat(winner.job().id()).isEqualTo(second.job().id());
        assertThat(winner.job().status()).isEqualTo(ImportStatus.PENDING);
        assertThat(storage.text(winner.job().storageKey())).as("the loser must not delete the winner's file")
                .isEqualTo(Fixtures.validLines(2));
        assertThat(imports.size()).isEqualTo(1);
        assertThat(queue.published).containsExactly(winner.job().id());
    }
}
