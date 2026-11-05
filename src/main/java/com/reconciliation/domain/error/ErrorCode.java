package com.reconciliation.domain.error;

/**
 * Stable, machine-readable error codes. The string form is part of the public API
 * contract and must not be renamed without a versioning decision.
 */
public enum ErrorCode {
    // --- request validation (4xx, deterministic) ---
    IDEMPOTENCY_KEY_REQUIRED(400, ErrorCategory.REQUEST_VALIDATION, false),
    IDEMPOTENCY_KEY_INVALID(400, ErrorCategory.REQUEST_VALIDATION, false),
    IMPORT_FILE_REQUIRED(400, ErrorCategory.REQUEST_VALIDATION, false),
    IMPORT_FILE_TOO_LARGE(413, ErrorCategory.REQUEST_VALIDATION, false),
    IMPORT_FILE_TYPE_INVALID(415, ErrorCategory.REQUEST_VALIDATION, false),
    IMPORT_FILE_EMPTY(400, ErrorCategory.REQUEST_VALIDATION, false),
    INVALID_CURSOR(400, ErrorCategory.REQUEST_VALIDATION, false),
    INVALID_PROVIDER(400, ErrorCategory.REQUEST_VALIDATION, false),
    VALIDATION_FAILED(400, ErrorCategory.REQUEST_VALIDATION, false),

    // --- business rules ---
    IMPORT_NOT_FOUND(404, ErrorCategory.BUSINESS_RULE, false),
    IDEMPOTENCY_KEY_CONFLICT(409, ErrorCategory.BUSINESS_RULE, false),
    IMPORT_NOT_CANCELLABLE(409, ErrorCategory.BUSINESS_RULE, false),
    IMPORT_STATE_CONFLICT(409, ErrorCategory.BUSINESS_RULE, false),
    TOO_MANY_CONCURRENT_UPLOADS(429, ErrorCategory.BUSINESS_RULE, false),

    // --- infrastructure ---
    UPLOAD_INTERRUPTED(400, ErrorCategory.INFRASTRUCTURE, false),
    STORAGE_FAILURE(503, ErrorCategory.INFRASTRUCTURE, true),
    DATABASE_UNAVAILABLE(503, ErrorCategory.INFRASTRUCTURE, true),
    /** The import was persisted but the broker did not accept the event yet; it will be re-dispatched. */
    QUEUE_UNAVAILABLE(503, ErrorCategory.INFRASTRUCTURE, true),
    SERVICE_SHUTTING_DOWN(503, ErrorCategory.INFRASTRUCTURE, true),
    PROCESSING_FAILED(500, ErrorCategory.INFRASTRUCTURE, false),

    // --- programming ---
    INTERNAL_ERROR(500, ErrorCategory.PROGRAMMING, false),

    // --- cancellation ---
    CANCELLED(409, ErrorCategory.CANCELLATION, false);

    private final int httpStatus;
    private final ErrorCategory category;
    private final boolean retryable;

    ErrorCode(int httpStatus, ErrorCategory category, boolean retryable) {
        this.httpStatus = httpStatus;
        this.category = category;
        this.retryable = retryable;
    }

    public int httpStatus() {
        return httpStatus;
    }

    public ErrorCategory category() {
        return category;
    }

    /** Whether a client (or an internal retry policy) may reasonably retry the same operation. */
    public boolean retryable() {
        return retryable;
    }
}
