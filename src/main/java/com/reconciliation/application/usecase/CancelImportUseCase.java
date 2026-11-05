package com.reconciliation.application.usecase;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Requests cancellation. A {@code pending} import is cancelled immediately (no worker
 * holds it). A {@code processing} import becomes {@code cancelling}; the worker observes
 * the flag between batches, lets in-flight batches commit, and marks it {@code cancelled}.
 * If the worker died, the next worker to claim the expired lease finalises it.
 * Cancelling an import that is already cancelling/cancelled is idempotent.
 */
public class CancelImportUseCase {

    private final ImportRepository imports;
    private final Clock clock;

    public CancelImportUseCase(ImportRepository imports, Clock clock) {
        this.imports = imports;
        this.clock = clock;
    }

    public ImportJob execute(UUID id) {
        Optional<ImportJob> transitioned = imports.requestCancel(id, clock.now());
        if (transitioned.isPresent()) {
            return transitioned.get();
        }
        ImportJob current = imports.findById(id).orElseThrow(() -> AppException.notFound("Import", id));
        if (current.status() == ImportStatus.CANCELLING || current.status() == ImportStatus.CANCELLED) {
            return current;
        }
        throw new AppException(ErrorCode.IMPORT_NOT_CANCELLABLE,
                "Import cannot be cancelled in status '" + current.status().wireName() + "'",
                null, Map.of("importId", id.toString(), "status", current.status().name()));
    }
}
