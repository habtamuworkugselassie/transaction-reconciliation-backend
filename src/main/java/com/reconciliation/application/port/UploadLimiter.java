package com.reconciliation.application.port;

/**
 * Bounds the number of uploads streaming at once. An upload that cannot get a slot
 * is rejected immediately with 429 rather than queued: holding a half-open HTTP
 * connection in a queue would consume the very resource we are protecting.
 */
public interface UploadLimiter {

    boolean tryAcquire();

    void release();

    int available();

    int capacity();
}
