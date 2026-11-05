package com.reconciliation.domain.model;

/** Counters persisted per committed batch. Invariant: processed = accepted + rejected + duplicates. */
public record ImportProgress(long processed, long accepted, long rejected, long duplicates) {

    public static final ImportProgress ZERO = new ImportProgress(0, 0, 0, 0);

    public ImportProgress {
        if (processed != accepted + rejected + duplicates) {
            throw new IllegalArgumentException("processed must equal accepted + rejected + duplicates");
        }
    }

    public ImportProgress plus(long accepted, long rejected, long duplicates) {
        return new ImportProgress(
                this.processed + accepted + rejected + duplicates,
                this.accepted + accepted,
                this.rejected + rejected,
                this.duplicates + duplicates);
    }
}
