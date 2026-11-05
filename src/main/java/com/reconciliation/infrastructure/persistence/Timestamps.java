package com.reconciliation.infrastructure.persistence;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** jOOQ maps timestamptz to OffsetDateTime; the domain speaks Instant. */
final class Timestamps {

    private Timestamps() {
    }

    static OffsetDateTime odt(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    static Instant instant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }

    static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }
}
