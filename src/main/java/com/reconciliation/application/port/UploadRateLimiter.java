package com.reconciliation.application.port;

/**
 * Per-provider admission control for uploads: at most N imports per provider per time
 * window. Separate from {@link UploadLimiter}, which bounds how many uploads this instance
 * streams at once; this one keeps a single provider from monopolising the service across
 * all instances.
 *
 * <p>Implementations must fail open: if the shared counter store is unavailable, the
 * upload is admitted (the concurrency limit and size limit still apply), because a cache
 * outage must not turn into an outage of the upload API.
 */
public interface UploadRateLimiter {

    /** Whether the upload may proceed and, if not, how long until the window resets. */
    record Decision(boolean allowed, long retryAfterSeconds) {
        public static final Decision ALLOW = new Decision(true, 0);
    }

    Decision tryAcquire(String providerId);

    /** No limit (the default; also used by tests). */
    UploadRateLimiter UNLIMITED = providerId -> Decision.ALLOW;
}
