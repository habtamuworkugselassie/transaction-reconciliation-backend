package com.reconciliation.infrastructure.tracing;

import com.reconciliation.application.port.BatchWriter;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * Decorates a {@link BatchWriter} with one observation per write attempt, which becomes a
 * span ({@code import.batch.write}) when tracing is exported. The import id and batch
 * number are high-cardinality span attributes only; they never become metric tags.
 *
 * <p>A decorator keeps tracing out of the use case: {@code ProcessImportUseCase} depends on
 * the {@code BatchWriter} port and does not know whether it is observed.
 */
public final class ObservedBatchWriter implements BatchWriter {

    private final BatchWriter delegate;
    private final ObservationRegistry registry;

    public ObservedBatchWriter(BatchWriter delegate, ObservationRegistry registry) {
        this.delegate = delegate;
        this.registry = registry;
    }

    @Override
    public BatchOutcome write(BatchWrite batch) {
        return Observation.createNotStarted("import.batch.write", registry)
                .contextualName("import batch write")
                .highCardinalityKeyValue("import.id", batch.importId().toString())
                .highCardinalityKeyValue("import.batch.number", String.valueOf(batch.batchNumber()))
                .highCardinalityKeyValue("import.batch.records",
                        String.valueOf(batch.candidates().size() + batch.rejections().size()))
                .observe(() -> delegate.write(batch));
    }
}
