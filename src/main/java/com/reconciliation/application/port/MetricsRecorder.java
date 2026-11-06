package com.reconciliation.application.port;

/**
 * Business-level metrics emitted by use cases. Kept as a port so use cases stay free
 * of Micrometer, and so a test can assert on what was recorded. Labels are
 * low-cardinality by construction: no ids ever become tags.
 */
public interface MetricsRecorder {

    void importCreated(long bytes);

    void importReplayed();

    /** reason: concurrency | interrupted | import_file_too_large | ... */
    void uploadRejected(String reason);

    void recordsParsed(int count);

    void batchWritten(int accepted, int rejected, int duplicates, long durationNanos);

    /** outcome: completed | cancelled | failed | released | abandoned */
    void importFinished(String outcome, long durationNanos);

    /** operation: batch-write | ... ; attempt number starting at 1 */
    void retryAttempt(String operation, int attempt);

    void retryExhausted(String operation);

    /** A client re-queued a failed import (POST /v1/imports/{id}/retry). */
    default void importRetried() { }

    /** The retention job deleted uploaded files. */
    default void uploadFilesPurged(int count) { }

    /** No-op implementation for tests and for wiring before Micrometer is configured. */
    MetricsRecorder NOOP = new MetricsRecorder() {
        @Override public void importCreated(long bytes) { }
        @Override public void importReplayed() { }
        @Override public void uploadRejected(String reason) { }
        @Override public void recordsParsed(int count) { }
        @Override public void batchWritten(int accepted, int rejected, int duplicates, long durationNanos) { }
        @Override public void importFinished(String outcome, long durationNanos) { }
        @Override public void retryAttempt(String operation, int attempt) { }
        @Override public void retryExhausted(String operation) { }
    };
}
