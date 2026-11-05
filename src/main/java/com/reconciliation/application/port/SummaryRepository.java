package com.reconciliation.application.port;

import com.reconciliation.domain.model.ImportSummary;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Aggregation over persisted results (ADR-005). {@link #compute} derives the summary from
 * the transactions table; {@link #saveFinal}/{@link #findFinal} cache it once an import is
 * terminal so repeated reads of a finished import cost one row.
 */
public interface SummaryRepository {

    /** Groups larger than this are truncated to the top entries by count. */
    int MAX_GROUP_ENTRIES = 100;

    ImportSummary compute(UUID importId);

    Optional<ImportSummary> findFinal(UUID importId);

    void saveFinal(UUID importId, ImportSummary summary, Instant now);
}
