package com.reconciliation.application.port;

import com.reconciliation.domain.model.ImportSummary;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-through cache for summaries. Implementations must fail soft: a cache outage
 * degrades to database reads, it never fails a request.
 */
public interface SummaryCache {

    Optional<ImportSummary> get(UUID importId);

    void put(UUID importId, ImportSummary summary, Duration ttl);

    void evict(UUID importId);

    SummaryCache NONE = new SummaryCache() {
        @Override public Optional<ImportSummary> get(UUID importId) { return Optional.empty(); }
        @Override public void put(UUID importId, ImportSummary summary, Duration ttl) { }
        @Override public void evict(UUID importId) { }
    };
}
