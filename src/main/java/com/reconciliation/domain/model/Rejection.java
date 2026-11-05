package com.reconciliation.domain.model;

import java.util.Map;

/**
 * A record that was not accepted. {@code rawValue} is a small, sanitised excerpt of the
 * input (typically just the transactionId, or a truncated prefix when the line could not
 * be parsed); it never contains the full line.
 */
public record Rejection(long lineNumber, RejectionReason reason, String message, Map<String, Object> rawValue) {}
