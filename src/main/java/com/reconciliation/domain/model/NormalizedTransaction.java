package com.reconciliation.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A transaction after validation and normalisation: every field is in its canonical
 * representation, so two logically identical inputs produce an identical record
 * (and therefore an identical fingerprint).
 */
public record NormalizedTransaction(
        long lineNumber,
        String providerId,
        String transactionId,
        String accountId,
        String merchantId,
        /** Scale is always 4 (e.g. 145.7500). */
        BigDecimal amount,
        /** Upper-case ISO 4217 code. */
        String currency,
        /** UTC, millisecond precision. */
        Instant timestamp,
        /** Trimmed, whitespace-collapsed, control characters removed; null when absent. */
        String description
) {}
