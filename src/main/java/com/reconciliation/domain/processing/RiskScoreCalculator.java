package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.RiskLevel;
import com.reconciliation.domain.model.ScoredTransaction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.util.regex.Pattern;

/**
 * Deterministic risk score in [0, 100]. Five additive components, each bounded, summed and
 * capped at 100. The same input always yields the same score; there is no randomness,
 * time-of-evaluation dependency or external lookup.
 *
 * <table>
 *   <tr><th>Component</th><th>Max</th><th>Rule</th></tr>
 *   <tr><td>amount</td><td>35</td><td>35 * log10(1 + amount) / log10(1 + 1,000,000), capped (a 1M+ transaction scores the full 35; 100 scores about 12)</td></tr>
 *   <tr><td>hour</td><td>20</td><td>UTC hour 00-05: 20, 06-07: 10, 22-23: 15, otherwise 0</td></tr>
 *   <tr><td>description</td><td>15</td><td>absent: 10; longer than 200 chars: 15; contains 8+ consecutive digits: 10; otherwise 0</td></tr>
 *   <tr><td>merchant</td><td>15</td><td>stable bucket of the merchant id (SHA-256 first byte mod 16), standing in for a merchant risk table</td></tr>
 *   <tr><td>fingerprint</td><td>15</td><td>SHA-256 iterated N times over the fingerprint; last byte mod 16. The iteration is the CPU-intensive step.</td></tr>
 * </table>
 *
 * <p>The fingerprint component is intentionally expensive (N configurable iterations) to model
 * a real scoring routine; it is why scoring runs on a dedicated bounded pool rather than on
 * request-handling threads.
 */
public final class RiskScoreCalculator {

    private static final double AMOUNT_LOG_CEILING = Math.log10(1 + 1_000_000d);
    private static final Pattern LONG_DIGIT_RUN = Pattern.compile("\\d{8,}");

    private final int hashIterations;

    public RiskScoreCalculator(int hashIterations) {
        if (hashIterations < 1) {
            throw new IllegalArgumentException("hashIterations must be >= 1");
        }
        this.hashIterations = hashIterations;
    }

    public ScoredTransaction score(NormalizedTransaction t) {
        String fingerprint = FingerprintCalculator.fingerprint(t);
        int score = amountComponent(t) + hourComponent(t) + descriptionComponent(t)
                + merchantComponent(t) + fingerprintComponent(fingerprint);
        int capped = Math.min(100, score);
        return new ScoredTransaction(t, fingerprint, FingerprintCalculator.VERSION, capped, RiskLevel.ofScore(capped));
    }

    int amountComponent(NormalizedTransaction t) {
        double v = 35 * Math.log10(1 + t.amount().doubleValue()) / AMOUNT_LOG_CEILING;
        return (int) Math.min(35, Math.round(v));
    }

    int hourComponent(NormalizedTransaction t) {
        int hour = t.timestamp().atOffset(ZoneOffset.UTC).getHour();
        if (hour <= 5) {
            return 20;
        }
        if (hour <= 7) {
            return 10;
        }
        return hour >= 22 ? 15 : 0;
    }

    int descriptionComponent(NormalizedTransaction t) {
        String d = t.description();
        if (d == null) {
            return 10;
        }
        if (d.length() > 200) {
            return 15;
        }
        return LONG_DIGIT_RUN.matcher(d).find() ? 10 : 0;
    }

    int merchantComponent(NormalizedTransaction t) {
        byte[] digest = FingerprintCalculator.sha256().digest(t.merchantId().getBytes(StandardCharsets.UTF_8));
        return (digest[0] & 0xFF) % 16;
    }

    int fingerprintComponent(String fingerprint) {
        MessageDigest md = FingerprintCalculator.sha256();
        byte[] current = fingerprint.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < hashIterations; i++) {
            current = md.digest(current);
        }
        return (current[current.length - 1] & 0xFF) % 16;
    }
}
