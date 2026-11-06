package com.reconciliation.domain.model;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Lifecycle of an import. Transitions are explicit so that every state change in the
 * persistence layer is a conditional update ({@code WHERE status = <from>}), which is
 * what makes concurrent cancel / claim / complete requests safe.
 *
 * <pre>
 * uploading -> pending -> processing -> completed
 *     |          |            |   \--> failed
 *     v          v            v
 *   failed    cancelled   cancelling -> cancelled
 *                             ^
 *                processing --+ (lease lost / transient failure: back to pending)
 * </pre>
 *
 * {@code uploading} is an addition to the statuses listed in the assignment: it is
 * the window between the idempotency key being reserved and the body being fully
 * stored, and it is what makes interrupted uploads detectable.
 */
public enum ImportStatus {
    UPLOADING,
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    CANCELLING,
    CANCELLED;

    private static final Set<ImportStatus> TERMINAL = EnumSet.of(COMPLETED, FAILED, CANCELLED);

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }

    public boolean canTransitionTo(ImportStatus target) {
        return switch (this) {
            case UPLOADING -> target == PENDING || target == FAILED;
            case PENDING -> target == PROCESSING || target == CANCELLED || target == FAILED;
            case PROCESSING -> target == COMPLETED || target == FAILED || target == CANCELLING
                    || target == PENDING || target == CANCELLED;
            case CANCELLING -> target == CANCELLED || target == FAILED;
            // A failed import whose file was stored can be re-queued by an explicit client retry
            // (POST /v1/imports/{id}/retry). It is still terminal: nothing moves it automatically.
            case FAILED -> target == PENDING;
            case COMPLETED, CANCELLED -> false;
        };
    }

    /** Whether a cancel request is meaningful in this state. */
    public boolean isCancellable() {
        return this == PENDING || this == PROCESSING;
    }

    /** Lower-case form used in the database and the API. */
    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ImportStatus fromWireName(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }
}
