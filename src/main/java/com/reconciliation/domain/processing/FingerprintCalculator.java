package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Deterministic content fingerprint.
 *
 * <p>Version 1 = SHA-256 over the UTF-8 bytes of the canonical string
 * <pre>"1" NUL provider NUL transactionId NUL accountId NUL merchantId NUL amount NUL currency NUL timestamp NUL description</pre>
 * where every field is in its normalised form ({@link TransactionValidator}): amount with
 * scale 4 in plain notation, timestamp as ISO-8601 UTC with milliseconds, description
 * normalised (empty string when absent). NUL separators cannot occur inside fields (control
 * characters are rejected/stripped), so the encoding is unambiguous.
 *
 * <p>The version number is both the first field of the hashed string and persisted next to the
 * hash, so a future v2 (different fields or algorithm) can coexist with v1 rows and
 * comparisons are only ever made between fingerprints of the same version.
 */
public final class FingerprintCalculator {

    public static final int VERSION = 1;
    private static final char SEP = '\u0000';

    private FingerprintCalculator() {
    }

    public static String canonicalString(NormalizedTransaction t) {
        return new StringBuilder(256)
                .append(VERSION).append(SEP)
                .append(t.providerId()).append(SEP)
                .append(t.transactionId()).append(SEP)
                .append(t.accountId()).append(SEP)
                .append(t.merchantId()).append(SEP)
                .append(t.amount().toPlainString()).append(SEP)
                .append(t.currency()).append(SEP)
                .append(t.timestamp().toString()).append(SEP)
                .append(t.description() == null ? "" : t.description())
                .toString();
    }

    public static String fingerprint(NormalizedTransaction t) {
        return HexFormat.of().formatHex(sha256().digest(canonicalString(t).getBytes(StandardCharsets.UTF_8)));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory in every JVM", e);
        }
    }
}
