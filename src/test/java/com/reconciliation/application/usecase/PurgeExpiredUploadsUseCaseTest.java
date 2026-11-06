package com.reconciliation.application.usecase;

import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryFileStorage;
import com.reconciliation.testing.InMemoryImportJobQueue;
import com.reconciliation.testing.InMemoryImportRepository;
import com.reconciliation.testing.RecordingMetrics;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PurgeExpiredUploadsUseCaseTest {

    private static final Duration RETAIN = Duration.ofDays(7);

    private final FixedClock clock = new FixedClock();
    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final InMemoryFileStorage storage = new InMemoryFileStorage();
    private final RecordingMetrics metrics = new RecordingMetrics();

    private PurgeExpiredUploadsUseCase useCase(int batchSize) {
        return new PurgeExpiredUploadsUseCase(imports, storage, metrics, clock, RETAIN, batchSize);
    }

    private UUID seed(ImportStatus status, Instant completedAt) {
        UUID id = UUID.randomUUID();
        String key = "imports/" + id + ".ndjson";
        storage.put(key, Fixtures.validLines(1));
        imports.put(new ImportJob(id, "key-" + id, Fixtures.PROVIDER, status, "f.ndjson", key, 10L, "sha",
                ImportProgress.ZERO, 0, false, 1, null, null, null, null, clock.now(), clock.now(), clock.now(),
                completedAt));
        return id;
    }

    @Test
    void deletesFilesOfTerminalImportsOlderThanTheRetentionAndKeepsTheImport() {
        Instant old = clock.now().minus(RETAIN).minusSeconds(1);
        UUID completed = seed(ImportStatus.COMPLETED, old);
        UUID failed = seed(ImportStatus.FAILED, old);
        UUID cancelled = seed(ImportStatus.CANCELLED, old);

        assertThat(useCase(100).execute()).isEqualTo(3);

        for (UUID id : new UUID[] {completed, failed, cancelled}) {
            ImportJob job = imports.get(id);
            assertThat(job.filePurgedAt()).isEqualTo(clock.now());
            assertThat(job.storageKey()).isNotNull(); // the record of what was uploaded stays
            assertThat(storage.contains(job.storageKey())).isFalse();
        }
        assertThat(metrics.events).contains("uploadFilesPurged:3");
    }

    @Test
    void keepsRecentAndUnfinishedImports() {
        UUID recent = seed(ImportStatus.COMPLETED, clock.now().minus(RETAIN).plusSeconds(60));
        UUID running = seed(ImportStatus.PROCESSING, null);
        UUID pending = seed(ImportStatus.PENDING, null);

        assertThat(useCase(100).execute()).isZero();

        for (UUID id : new UUID[] {recent, running, pending}) {
            assertThat(storage.contains(imports.get(id).storageKey())).isTrue();
            assertThat(imports.get(id).filePurgedAt()).isNull();
        }
    }

    @Test
    void eachRunIsBoundedAndTheNextRunContinues() {
        Instant old = clock.now().minus(RETAIN).minusSeconds(1);
        for (int i = 0; i < 5; i++) {
            seed(ImportStatus.COMPLETED, old.minusSeconds(i));
        }

        assertThat(useCase(2).execute()).isEqualTo(2);
        assertThat(useCase(2).execute()).isEqualTo(2);
        assertThat(useCase(2).execute()).isEqualTo(1);
        assertThat(useCase(2).execute()).isZero();
        assertThat(storage.size()).isZero();
    }

    @Test
    void anImportRetriedBeforeThePurgeClaimIsLeftAlone() {
        UUID id = seed(ImportStatus.FAILED, clock.now().minus(RETAIN).minusSeconds(1));
        // A client retry wins the race: the import is pending again when retention reaches it.
        new RetryImportUseCase(imports, new InMemoryImportJobQueue(imports, clock, Duration.ofSeconds(30)),
                com.reconciliation.application.port.SummaryCache.NONE, metrics, clock, 3).execute(id);

        assertThat(useCase(100).execute()).isZero();
        assertThat(storage.contains(imports.get(id).storageKey())).isTrue();
    }
}
