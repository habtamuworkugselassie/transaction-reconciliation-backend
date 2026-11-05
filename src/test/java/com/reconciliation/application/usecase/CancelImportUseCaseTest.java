package com.reconciliation.application.usecase;

import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryImportRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CancelImportUseCaseTest {

    private final FixedClock clock = new FixedClock();
    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final CancelImportUseCase useCase = new CancelImportUseCase(imports, clock);

    private UUID seed(ImportStatus status) {
        UUID id = UUID.randomUUID();
        imports.put(InMemoryImportRepository.job(id, status, Fixtures.PROVIDER, "k", clock.now()));
        return id;
    }

    @Test
    void pendingImportIsCancelledImmediately() {
        UUID id = seed(ImportStatus.PENDING);
        clock.advance(Duration.ofMinutes(1));

        ImportJob job = useCase.execute(id);

        assertThat(job.status()).isEqualTo(ImportStatus.CANCELLED);
        assertThat(job.cancelRequested()).isTrue();
        assertThat(job.completedAt()).isEqualTo(clock.now());
    }

    @Test
    void processingImportBecomesCancellingForTheWorkerToObserve() {
        UUID id = seed(ImportStatus.PENDING);
        imports.claim(id, "worker-1", Duration.ofSeconds(30), clock.now());

        ImportJob job = useCase.execute(id);

        assertThat(job.status()).isEqualTo(ImportStatus.CANCELLING);
        assertThat(job.cancelRequested()).isTrue();
        assertThat(job.completedAt()).as("not finished until the worker stops").isNull();
        assertThat(imports.isCancelRequested(id)).isTrue();
    }

    @Test
    void cancellingTwiceIsIdempotent() {
        UUID id = seed(ImportStatus.PENDING);
        imports.claim(id, "worker-1", Duration.ofSeconds(30), clock.now());
        useCase.execute(id);

        assertThat(useCase.execute(id).status()).isEqualTo(ImportStatus.CANCELLING);

        imports.markCancelled(id, "worker-1", clock.now());
        assertThat(useCase.execute(id).status()).isEqualTo(ImportStatus.CANCELLED);
    }

    @ParameterizedTest
    @EnumSource(value = ImportStatus.class, names = {"COMPLETED", "FAILED", "UPLOADING"})
    void finishedOrNotYetQueuedImportsCannotBeCancelled(ImportStatus status) {
        UUID id = seed(status);

        assertThatThrownBy(() -> useCase.execute(id))
                .isInstanceOf(AppException.class)
                .extracting("code").isEqualTo(ErrorCode.IMPORT_NOT_CANCELLABLE);
        assertThat(imports.get(id).status()).isEqualTo(status);
    }

    @Test
    void unknownImportIs404() {
        assertThatThrownBy(() -> useCase.execute(UUID.randomUUID()))
                .isInstanceOf(AppException.class)
                .extracting("code").isEqualTo(ErrorCode.IMPORT_NOT_FOUND);
    }
}
