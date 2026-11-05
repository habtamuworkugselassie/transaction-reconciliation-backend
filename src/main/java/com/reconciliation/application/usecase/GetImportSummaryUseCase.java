package com.reconciliation.application.usecase;

import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.application.port.SummaryRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportSummary;

import java.time.Duration;
import java.util.UUID;

/**
 * Serves the reconciliation summary.
 *
 * <p>Three tiers: Redis (hot reads), the import_summaries table (durable final result)
 * and a live GROUP BY over transactions. While an import is running the live summary
 * is cached for a short TTL so a dashboard polling every second does not re-aggregate
 * every call; once terminal the summary is immutable, stored in the database and cached
 * for a long TTL. The database is always the source of truth; the cache only shortens
 * the path to it.
 */
public class GetImportSummaryUseCase {

    public record Result(ImportJob job, ImportSummary summary, boolean isFinal) {}

    private final ImportRepository imports;
    private final SummaryRepository summaries;
    private final SummaryCache cache;
    private final Duration liveTtl;
    private final Duration finalTtl;
    private final Clock clock;

    public GetImportSummaryUseCase(ImportRepository imports, SummaryRepository summaries, SummaryCache cache,
                                   Duration liveTtl, Duration finalTtl, Clock clock) {
        this.imports = imports;
        this.summaries = summaries;
        this.cache = cache;
        this.liveTtl = liveTtl;
        this.finalTtl = finalTtl;
        this.clock = clock;
    }

    public Result execute(UUID id) {
        ImportJob job = imports.findById(id).orElseThrow(() -> AppException.notFound("Import", id));
        boolean isFinal = job.status().isTerminal();
        ImportSummary cached = cache.get(id).orElse(null);
        if (cached != null && (isFinal || cached.totals().accepted() + cached.totals().rejected()
                + cached.totals().duplicates() == job.progress().processed())) {
            return new Result(job, cached, isFinal);
        }
        if (!isFinal) {
            ImportSummary live = summaries.compute(id);
            cache.put(id, live, liveTtl);
            return new Result(job, live, false);
        }
        ImportSummary stored = summaries.findFinal(id).orElse(null);
        if (stored == null) {
            stored = summaries.compute(id);
            summaries.saveFinal(id, stored, clock.now()); // idempotent; a race writes the same value twice
        }
        cache.put(id, stored, finalTtl);
        return new Result(job, stored, true);
    }
}
