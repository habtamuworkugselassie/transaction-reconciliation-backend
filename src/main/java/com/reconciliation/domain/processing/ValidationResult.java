package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.Rejection;

/** Either a normalised transaction or the reason it was rejected. */
public sealed interface ValidationResult {
    record Valid(NormalizedTransaction transaction) implements ValidationResult {}
    record Invalid(Rejection rejection) implements ValidationResult {}
}
