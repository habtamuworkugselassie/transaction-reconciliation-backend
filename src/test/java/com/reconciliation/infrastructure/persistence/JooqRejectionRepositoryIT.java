package com.reconciliation.infrastructure.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.BatchWriter.BatchWrite;
import com.reconciliation.application.port.RejectionRepository.Page;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class JooqRejectionRepositoryIT {

    private final PostgresSupport pg = PostgresSupport.get();
    private final FixedClock clock = new FixedClock();
    private final JooqImportRepository repo = new JooqImportRepository(pg.db());
    private final JooqImportJobStore store = new JooqImportJobStore(pg.db());
    private final JooqBatchWriter writer = new JooqBatchWriter(pg.db(), pg.transactionTemplate(), clock);
    private final JooqRejectionRepository rejections = new JooqRejectionRepository(pg.db(), new ObjectMapper());

    @BeforeEach
    void clean() {
        pg.truncateAll();
    }

    private ImportJob withRejections(String key, int count) {
        ImportJob job = ItFixtures.processing(repo, store, key, Fixtures.PROVIDER, "w1", clock.now());
        List<Rejection> rows = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            rows.add(new Rejection(i, RejectionReason.INVALID_CURRENCY, "Currency must be a supported three-letter code",
                    Map.of("transactionId", "txn-" + i)));
        }
        writer.write(new BatchWrite(job.id(), "w1", 1, count, List.of(), rows));
        return job;
    }

    @Test
    void pagesInLineOrderAndEachRowAppearsExactlyOnce() {
        ImportJob other = withRejections("other", 4); // noise from another import
        ImportJob job = withRejections("mine", 7);

        List<Rejection> seen = new ArrayList<>();
        long after = 0;
        int pages = 0;
        while (true) {
            Page page = rejections.list(job.id(), after, 3);
            seen.addAll(page.items());
            pages++;
            if (page.nextAfterId() == null) {
                break;
            }
            after = page.nextAfterId();
        }

        assertThat(pages).isEqualTo(3);
        assertThat(seen).extracting(Rejection::lineNumber).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        assertThat(seen.get(0).reason()).isEqualTo(RejectionReason.INVALID_CURRENCY);
        assertThat(seen.get(0).message()).isEqualTo("Currency must be a supported three-letter code");
        assertThat(seen.get(0).rawValue()).containsEntry("transactionId", "txn-1");
        assertThat(rejections.list(other.id(), 0, 50).items()).hasSize(4);
    }

    @Test
    void aFullLastPageHasNoNextCursor() {
        ImportJob job = withRejections("k", 6);

        Page first = rejections.list(job.id(), 0, 3);
        Page second = rejections.list(job.id(), first.nextAfterId(), 3);

        assertThat(first.items()).hasSize(3);
        assertThat(first.nextAfterId()).isNotNull();
        assertThat(second.items()).hasSize(3);
        assertThat(second.nextAfterId()).isNull();
    }

    @Test
    void unknownImportOrExhaustedCursorYieldsAnEmptyPage() {
        ImportJob job = withRejections("k", 2);

        assertThat(rejections.list(UUID.randomUUID(), 0, 10).items()).isEmpty();
        assertThat(rejections.list(job.id(), Long.MAX_VALUE, 10).items()).isEmpty();
        assertThat(rejections.list(job.id(), Long.MAX_VALUE, 10).nextAfterId()).isNull();
    }
}
