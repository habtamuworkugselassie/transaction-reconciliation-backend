package com.reconciliation.infrastructure.concurrency;

import com.reconciliation.application.port.UploadLimiter;

import java.util.concurrent.Semaphore;

/** Fixed-size permit pool; {@code tryAcquire} never blocks. */
public final class SemaphoreUploadLimiter implements UploadLimiter {

    private final Semaphore permits;
    private final int capacity;

    public SemaphoreUploadLimiter(int capacity) {
        this.capacity = capacity;
        this.permits = new Semaphore(capacity, true);
    }

    @Override
    public boolean tryAcquire() {
        return permits.tryAcquire();
    }

    @Override
    public void release() {
        permits.release();
    }

    @Override
    public int available() {
        return permits.availablePermits();
    }

    @Override
    public int capacity() {
        return capacity;
    }
}
