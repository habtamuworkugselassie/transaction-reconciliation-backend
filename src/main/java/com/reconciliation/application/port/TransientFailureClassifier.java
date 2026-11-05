package com.reconciliation.application.port;

/** Decides whether a failure is worth retrying. Infrastructure knows the SQL states; the use case does not. */
@FunctionalInterface
public interface TransientFailureClassifier {
    boolean isTransient(Throwable failure);
}
