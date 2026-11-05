package com.reconciliation.infrastructure.persistence;

import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.domain.processing.RiskScoreCalculator;
import com.reconciliation.testing.Fixtures;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/** Shortcuts to put an import row into a given state through the production adapters. */
final class ItFixtures {

    static final Duration LEASE = Duration.ofSeconds(30);
    private static final RiskScoreCalculator CALCULATOR = new RiskScoreCalculator(1);

    private ItFixtures() {
    }

    /** A row in {@code uploading}: key reserved, nothing stored yet. */
    static ImportJob reserved(ImportRepository repo, String key, String provider, Instant now) {
        ImportRepository.Reservation r = repo.reserve(ImportJob.reserved(UUID.randomUUID(), key, provider, "f.ndjson", now));
        if (!(r instanceof ImportRepository.Reservation.Created c)) {
            throw new IllegalStateException("key already used: " + key);
        }
        return c.job();
    }

    /** A row in {@code pending}: uploaded and enqueued. */
    static ImportJob pending(ImportRepository repo, JooqImportJobStore store, String key, String provider, Instant now) {
        ImportJob job = reserved(repo, key, provider, now);
        if (!repo.markUploaded(job.id(), "imports/" + job.id() + ".ndjson", 123, "a".repeat(64), now)) {
            throw new IllegalStateException();
        }
        if (!store.markPending(job.id(), now)) {
            throw new IllegalStateException();
        }
        return repo.findById(job.id()).orElseThrow();
    }

    /** A row in {@code processing}, leased by {@code worker}. */
    static ImportJob processing(ImportRepository repo, JooqImportJobStore store, String key, String provider,
                                String worker, Instant now) {
        ImportJob job = pending(repo, store, key, provider, now);
        return repo.claim(job.id(), worker, LEASE, now).orElseThrow();
    }

    static ScoredTransaction scored(long line, String provider, String transactionId, String amount, String currency,
                                    String merchant, String account, String description) {
        return CALCULATOR.score(new com.reconciliation.domain.model.NormalizedTransaction(line, provider, transactionId,
                account, merchant, new java.math.BigDecimal(amount), currency, Instant.parse("2026-07-20T10:25:00Z"),
                description));
    }

    static ScoredTransaction scored(long line, String transactionId) {
        return scored(line, Fixtures.PROVIDER, transactionId, "145.7500", "USD", "merchant-18", "acc-201", "Subscription payment");
    }

    static boolean is(ImportJob job, ImportStatus status) {
        return job.status() == status;
    }
}
