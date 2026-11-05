package com.reconciliation.application.usecase;

import com.reconciliation.application.port.RejectionRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportStatus;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import com.reconciliation.testing.InMemoryImportRepository;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ListRejectionsUseCaseTest {

    /** Rejections keyed by a monotonically increasing row id, like the database table. */
    private static final class InMemoryRejections implements RejectionRepository {
        final List<Rejection> rows = new ArrayList<>();
        final List<String> calls = new ArrayList<>();

        @Override
        public Page list(UUID importId, long afterId, int limit) {
            calls.add(afterId + "/" + limit);
            List<Rejection> items = new ArrayList<>();
            Long next = null;
            for (int i = (int) afterId; i < rows.size(); i++) {
                if (items.size() == limit) {
                    next = (long) i; // id of the last returned row (ids are 1-based)
                    break;
                }
                items.add(rows.get(i));
            }
            return new Page(items, next);
        }
    }

    private final InMemoryImportRepository imports = new InMemoryImportRepository();
    private final InMemoryRejections rejections = new InMemoryRejections();
    private final ListRejectionsUseCase useCase = new ListRejectionsUseCase(imports, rejections);
    private final UUID importId = UUID.randomUUID();

    ListRejectionsUseCaseTest() {
        imports.put(InMemoryImportRepository.job(importId, ImportStatus.COMPLETED, Fixtures.PROVIDER, "k", FixedClock.START));
        for (int i = 1; i <= 7; i++) {
            rejections.rows.add(new Rejection(i * 10L, RejectionReason.INVALID_AMOUNT, "m" + i, Map.of("transactionId", "t" + i)));
        }
    }

    @Test
    void pagesThroughEveryRowExactlyOnceUsingOpaqueCursors() {
        List<Rejection> seen = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            ListRejectionsUseCase.Page page = useCase.execute(importId, 3, cursor);
            seen.addAll(page.items());
            cursor = page.nextCursor();
            pages++;
            if (cursor != null) {
                assertThat(cursor).matches("[A-Za-z0-9_-]+");
            }
        } while (cursor != null);

        assertThat(pages).isEqualTo(3);
        assertThat(seen).containsExactlyElementsOf(rejections.rows);
        assertThat(rejections.calls).containsExactly("0/3", "3/3", "6/3");
    }

    @Test
    void lastPageHasNoCursorEvenWhenFull() {
        ListRejectionsUseCase.Page page = useCase.execute(importId, 7, null);
        assertThat(page.items()).hasSize(7);
        assertThat(page.nextCursor()).isNull();
    }

    @Test
    void defaultsTo50AndCapsAt200() {
        useCase.execute(importId, null, null);
        assertThat(rejections.calls).containsExactly("0/50");

        assertThatThrownBy(() -> useCase.execute(importId, 201, null))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThatThrownBy(() -> useCase.execute(importId, 0, null))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.VALIDATION_FAILED);
    }

    @Test
    void tamperedCursorsAreRejected() {
        for (String bad : List.of("garbage!", "cjotMQ", "cjotLTE", ListRejectionsUseCase.encode(5) + "x",
                java.util.Base64.getUrlEncoder().withoutPadding().encodeToString("r:abc".getBytes()))) {
            assertThatThrownBy(() -> useCase.execute(importId, 10, bad))
                    .as("cursor " + bad)
                    .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.INVALID_CURSOR);
        }
    }

    @Test
    void cursorRoundTrips() {
        assertThat(ListRejectionsUseCase.decode(ListRejectionsUseCase.encode(0))).isZero();
        assertThat(ListRejectionsUseCase.decode(ListRejectionsUseCase.encode(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
        assertThat(ListRejectionsUseCase.decode(null)).isZero();
        assertThat(ListRejectionsUseCase.decode("")).isZero();
    }

    @Test
    void unknownImportIs404() {
        assertThatThrownBy(() -> useCase.execute(UUID.randomUUID(), 10, null))
                .isInstanceOf(AppException.class).extracting("code").isEqualTo(ErrorCode.IMPORT_NOT_FOUND);
    }
}
