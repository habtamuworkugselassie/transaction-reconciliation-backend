package com.reconciliation.application.port;

import com.reconciliation.domain.model.Rejection;

import java.util.List;
import java.util.UUID;

/** Read side of rejected records, keyset-paginated by the row id. */
public interface RejectionRepository {

    record Page(List<Rejection> items, Long nextAfterId) {}

    /** Rows with id greater than {@code afterId} (0 for the first page), at most {@code limit}. */
    Page list(UUID importId, long afterId, int limit);
}
