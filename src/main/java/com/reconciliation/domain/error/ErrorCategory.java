package com.reconciliation.domain.error;

/**
 * Coarse classification used to decide retryability and HTTP mapping.
 * The distinction matters operationally: only {@link #INFRASTRUCTURE} failures are
 * ever retried; the rest are deterministic and retrying them would only burn capacity.
 */
public enum ErrorCategory {
    /** Malformed request (bad header, wrong content type, file too large). Never retried. */
    REQUEST_VALIDATION,
    /** A single record failed validation; recorded as a rejection, processing continues. */
    RECORD_VALIDATION,
    /** The request was well-formed but violates a business rule (e.g. cancelling a completed import). */
    BUSINESS_RULE,
    /** Database, file system or network failure. Possibly transient, candidate for retry. */
    INFRASTRUCTURE,
    /** A bug. Surfaces as 500, is logged with full context, and is never retried or hidden. */
    PROGRAMMING,
    /** Work was interrupted by a cancellation or shutdown request. Not an error for the client. */
    CANCELLATION
}
