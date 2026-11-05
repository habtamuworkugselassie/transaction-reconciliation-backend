package com.reconciliation.application.usecase;

import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.model.ImportJob;

import java.util.UUID;

/** Reads the persisted state of an import. */
public class GetImportUseCase {

    private final ImportRepository imports;

    public GetImportUseCase(ImportRepository imports) {
        this.imports = imports;
    }

    public ImportJob execute(UUID id) {
        return imports.findById(id).orElseThrow(() -> AppException.notFound("Import", id));
    }
}
