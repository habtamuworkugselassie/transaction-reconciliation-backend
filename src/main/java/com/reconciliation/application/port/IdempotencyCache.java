package com.reconciliation.application.port;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Fast path for replayed create requests: maps an idempotency key to the import it
 * produced. The database unique constraint remains the guarantee; the cache only
 * saves the insert attempt for the common "client retried" case. Fail-soft.
 */
public interface IdempotencyCache {

    Optional<UUID> find(String idempotencyKey);

    void remember(String idempotencyKey, UUID importId, Duration ttl);

    IdempotencyCache NONE = new IdempotencyCache() {
        @Override public Optional<UUID> find(String idempotencyKey) { return Optional.empty(); }
        @Override public void remember(String idempotencyKey, UUID importId, Duration ttl) { }
    };
}
