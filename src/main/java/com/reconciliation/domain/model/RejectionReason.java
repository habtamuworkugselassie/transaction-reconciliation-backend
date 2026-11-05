package com.reconciliation.domain.model;

/**
 * Machine-readable reasons a record is not accepted. Part of the public contract of
 * {@code GET /v1/imports/{id}/rejections}.
 */
public enum RejectionReason {
    INVALID_JSON,
    NOT_AN_OBJECT,
    LINE_TOO_LONG,
    MISSING_FIELD,
    INVALID_TYPE,
    INVALID_VALUE,
    INVALID_AMOUNT,
    INVALID_CURRENCY,
    INVALID_TIMESTAMP,
    DESCRIPTION_TOO_LONG,
    FIELD_TOO_LONG,
    /** Same provider + transactionId already accepted with identical normalised content. */
    DUPLICATE_TRANSACTION,
    /** Same provider + transactionId already accepted with different content. First write wins. */
    DUPLICATE_TRANSACTION_CONFLICT;

    /** Duplicates are counted separately from validation rejections in progress and summaries. */
    public boolean isDuplicate() {
        return this == DUPLICATE_TRANSACTION || this == DUPLICATE_TRANSACTION_CONFLICT;
    }
}
