package com.reconciliation.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.BatchWriter.BatchWrite;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportSummary;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JooqSummaryRepositoryIT {

    private final PostgresSupport pg = PostgresSupport.get();
    private final FixedClock clock = new FixedClock();
    private final JooqImportRepository repo = new JooqImportRepository(pg.db());
    private final JooqImportJobStore store = new JooqImportJobStore(pg.db());
    private final JooqBatchWriter writer = new JooqBatchWriter(pg.db(), pg.transactionTemplate(), clock);
    private final JooqSummaryRepository summaries = new JooqSummaryRepository(pg.db(), new ObjectMapper());

    @BeforeEach
    void clean() {
        pg.truncateAll();
    }

    @Test
    void aggregatesPersistedRowsByCurrencyRiskMerchantAndAccount() {
        ImportJob job = ItFixtures.processing(repo, store, "k", Fixtures.PROVIDER, "w1", clock.now());
        ImportJob other = ItFixtures.processing(repo, store, "o", Fixtures.PROVIDER, "w1", clock.now());
        List<ScoredTransaction> rows = List.of(
                ItFixtures.scored(1, Fixtures.PROVIDER, "t1", "10.0000", "USD", "m1", "a1", "d"),
                ItFixtures.scored(2, Fixtures.PROVIDER, "t2", "20.5000", "USD", "m1", "a2", "d"),
                ItFixtures.scored(3, Fixtures.PROVIDER, "t3", "5.2500", "EUR", "m2", "a1", "d"),
                ItFixtures.scored(4, Fixtures.PROVIDER, "t1", "10.0000", "USD", "m1", "a1", "d")); // duplicate
        writer.write(new BatchWrite(job.id(), "w1", 1, 5, rows,
                List.of(new Rejection(5, RejectionReason.INVALID_JSON, "x", Map.of()))));
        writer.write(new BatchWrite(other.id(), "w1", 1, 1,
                List.of(ItFixtures.scored(1, Fixtures.PROVIDER, "t9", "1000.0000", "GBP", "m9", "a9", "d")), List.of()));

        ImportSummary s = summaries.compute(job.id());

        assertThat(s.totals()).isEqualTo(new ImportSummary.Totals(3, 1, 1));
        assertThat(s.byCurrency()).containsExactly(
                new ImportSummary.CurrencyTotal("EUR", 1, new BigDecimal("5.2500")),
                new ImportSummary.CurrencyTotal("USD", 2, new BigDecimal("30.5000")));
        assertThat(s.byRiskLevel().low() + s.byRiskLevel().medium() + s.byRiskLevel().high()).isEqualTo(3);
        assertThat(s.byMerchant()).containsExactly(
                new ImportSummary.GroupTotal("m1", 2, new BigDecimal("30.5000")),
                new ImportSummary.GroupTotal("m2", 1, new BigDecimal("5.2500")));
        assertThat(s.byAccount()).containsExactly(
                new ImportSummary.GroupTotal("a1", 2, new BigDecimal("15.2500")),
                new ImportSummary.GroupTotal("a2", 1, new BigDecimal("20.5000")));
    }

    @Test
    void groupsAreCappedAtTheTopHundredByCount() {
        ImportJob job = ItFixtures.processing(repo, store, "k", Fixtures.PROVIDER, "w1", clock.now());
        List<ScoredTransaction> rows = new ArrayList<>();
        int line = 0;
        for (int m = 0; m < 150; m++) {
            int copies = m == 7 ? 3 : 1; // one merchant with more rows than the rest
            for (int c = 0; c < copies; c++) {
                line++;
                rows.add(ItFixtures.scored(line, Fixtures.PROVIDER, "t" + line, "1.0000", "USD", "m" + m, "a" + (line % 3), "d"));
            }
        }
        writer.write(new BatchWrite(job.id(), "w1", 1, line, rows, List.of()));

        ImportSummary s = summaries.compute(job.id());

        assertThat(s.byMerchant()).hasSize(100);
        assertThat(s.byMerchant().get(0)).isEqualTo(new ImportSummary.GroupTotal("m7", 3, new BigDecimal("3.0000")));
        assertThat(s.byAccount()).hasSize(3);
        assertThat(s.totals().accepted()).isEqualTo(line);
    }

    @Test
    void emptyImportSummarisesToZeros() {
        ImportJob job = ItFixtures.processing(repo, store, "k", Fixtures.PROVIDER, "w1", clock.now());

        ImportSummary s = summaries.compute(job.id());

        assertThat(s.totals()).isEqualTo(new ImportSummary.Totals(0, 0, 0));
        assertThat(s.byCurrency()).isEmpty();
        assertThat(s.byRiskLevel()).isEqualTo(new ImportSummary.RiskTotals(0, 0, 0));
    }

    @Test
    void finalSummaryIsStoredOnceAndReadBackIdentically() {
        ImportJob job = ItFixtures.processing(repo, store, "k", Fixtures.PROVIDER, "w1", clock.now());
        writer.write(new BatchWrite(job.id(), "w1", 1, 1, List.of(ItFixtures.scored(1, "t1")), List.of()));
        ImportSummary computed = summaries.compute(job.id());

        assertThat(summaries.findFinal(job.id())).isEmpty();
        summaries.saveFinal(job.id(), computed, clock.now());
        summaries.saveFinal(job.id(), summaries.compute(job.id()), clock.now()); // idempotent race

        assertThat(summaries.findFinal(job.id())).contains(computed);
    }
}
