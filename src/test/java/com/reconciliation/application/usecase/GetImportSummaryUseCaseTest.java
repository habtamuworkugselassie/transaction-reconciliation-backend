package com.reconciliation.application.usecase;

import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.application.port.SummaryRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportProgress;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.ImportSummary;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryImportRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GetImportSummaryUseCaseTest {

    private static final class FakeSummaries implements SummaryRepository {
        final Map<UUID, ImportSummary> finals = new HashMap<>();
        int computeCalls;
        long acceptedInDb;

        @Override
        public ImportSummary compute(UUID importId) {
            computeCalls++;
            return summary(acceptedInDb);
        }

        @Override
        public Optional<ImportSummary> findFinal(UUID importId) {
            return Optional.ofNullable(finals.get(importId));
        }

        @Override
        public void saveFinal(UUID importId, ImportSummary summary, Instant now) {
            finals.putIfAbsent(importId, summary);
        }
    }

    private static final class FakeCache implements SummaryCache {
        final Map<UUID, ImportSummary> entries = new HashMap<>();
        final Map<UUID, Duration> ttls = new HashMap<>();

        @Override public Optional<ImportSummary> get(UUID importId) { return Optional.ofNullable(entries.get(importId)); }
        @Override public void put(UUID importId, ImportSummary summary, Duration ttl) { entries.put(importId, summary); ttls.put(importId, ttl); }
        @Override public void evict(UUID importId) { entries.remove(importId); }
    }

    private static ImportSummary summary(long accepted) {
        return new ImportSummary(new ImportSummary.Totals(accepted, 0, 0),
                List.of(new ImportSummary.CurrencyTotal("USD", accepted, BigDecimal.TEN)),
                new ImportSummary.RiskTotals(accepted, 0, 0), List.of(), List.of());
    }

    private final FixedClock clock = new FixedClock();
    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final FakeSummaries summaries = new FakeSummaries();
    private final FakeCache cache = new FakeCache();
    private final GetImportSummaryUseCase useCase = new GetImportSummaryUseCase(imports, summaries, cache,
            Duration.ofSeconds(2), Duration.ofHours(1), clock);
    private final UUID id = UUID.randomUUID();

    private void seed(ImportStatus status, long processed) {
        ImportJob base = InMemoryImportRepository.job(id, status, Fixtures.PROVIDER, "k", clock.now());
        imports.put(new ImportJob(base.id(), base.idempotencyKey(), base.providerId(), status, base.originalFilename(),
                base.storageKey(), base.fileSizeBytes(), base.contentSha256(), new ImportProgress(processed, processed, 0, 0),
                processed, false, 1, null, null, null, null, base.createdAt(), base.updatedAt(), null, null));
    }

    @Test
    void liveSummaryIsComputedAndCachedBriefly() {
        seed(ImportStatus.PROCESSING, 5);
        summaries.acceptedInDb = 5;

        GetImportSummaryUseCase.Result r = useCase.execute(id);

        assertThat(r.isFinal()).isFalse();
        assertThat(r.summary().totals().accepted()).isEqualTo(5);
        assertThat(summaries.computeCalls).isEqualTo(1);
        assertThat(cache.ttls.get(id)).isEqualTo(Duration.ofSeconds(2));
        assertThat(summaries.finals).as("nothing materialised while running").isEmpty();
    }

    @Test
    void staleLiveCacheEntryIsIgnoredOnceProgressMovedOn() {
        seed(ImportStatus.PROCESSING, 5);
        cache.put(id, summary(3), Duration.ofSeconds(2));
        summaries.acceptedInDb = 5;

        GetImportSummaryUseCase.Result r = useCase.execute(id);

        assertThat(r.summary().totals().accepted()).isEqualTo(5);
        assertThat(summaries.computeCalls).isEqualTo(1);
    }

    @Test
    void liveCacheEntryMatchingProgressIsServedWithoutRecomputing() {
        seed(ImportStatus.PROCESSING, 3);
        cache.put(id, summary(3), Duration.ofSeconds(2));

        useCase.execute(id);

        assertThat(summaries.computeCalls).isZero();
    }

    @Test
    void terminalImportIsMaterialisedOnceThenServedFromTheTable() {
        seed(ImportStatus.COMPLETED, 7);
        summaries.acceptedInDb = 7;

        GetImportSummaryUseCase.Result first = useCase.execute(id);
        cache.entries.clear();
        GetImportSummaryUseCase.Result second = useCase.execute(id);

        assertThat(first.isFinal()).isTrue();
        assertThat(second.summary()).isEqualTo(first.summary());
        assertThat(summaries.computeCalls).as("computed once, then read from import_summaries").isEqualTo(1);
        assertThat(summaries.finals).containsKey(id);
        assertThat(cache.ttls.get(id)).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void cacheOutageIsInvisible() {
        seed(ImportStatus.COMPLETED, 2);
        summaries.acceptedInDb = 2;
        GetImportSummaryUseCase failSoft = new GetImportSummaryUseCase(imports, summaries, SummaryCache.NONE,
                Duration.ofSeconds(2), Duration.ofHours(1), clock);

        assertThat(failSoft.execute(id).summary().totals().accepted()).isEqualTo(2);
    }

    @Test
    void unknownImportIs404() {
        assertThatThrownBy(() -> useCase.execute(UUID.randomUUID())).isInstanceOf(AppException.class);
    }
}
