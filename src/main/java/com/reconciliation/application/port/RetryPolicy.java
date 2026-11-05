package com.reconciliation.application.port;

import com.reconciliation.application.processing.CancellationToken;

import java.util.concurrent.Callable;

/**
 * Executes an operation with retries for transient failures. The implementation
 * decides what is transient; the contract here is that non-transient failures
 * propagate unchanged after the first attempt, and that a cancelled token stops
 * further attempts.
 */
public interface RetryPolicy {

    <T> T execute(String operation, CancellationToken token, Callable<T> action) throws Exception;

    /** Single attempt, no retry. */
    RetryPolicy NONE = new RetryPolicy() {
        @Override
        public <T> T execute(String operation, CancellationToken token, Callable<T> action) throws Exception {
            return action.call();
        }
    };
}
