package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class FingerprintCalculatorTest {

    @Test
    void isSha256HexOfTheCanonicalString() {
        NormalizedTransaction t = Fixtures.normalized("txn-1");

        String fp = FingerprintCalculator.fingerprint(t);

        assertThat(fp).hasSize(64).matches("[0-9a-f]{64}");
        assertThat(FingerprintCalculator.fingerprint(t)).isEqualTo(fp);
    }

    @Test
    void canonicalStringStartsWithVersionAndUsesNulSeparators() {
        NormalizedTransaction t = Fixtures.normalized("txn-1");

        String canonical = FingerprintCalculator.canonicalString(t);

        assertThat(canonical).startsWith(FingerprintCalculator.VERSION + "\u0000" + Fixtures.PROVIDER + "\u0000txn-1\u0000");
        assertThat(canonical.split("\u0000", -1)).hasSize(9);
        assertThat(canonical).contains("\u0000145.7500\u0000USD\u00002026-07-20T10:25:00Z\u0000Subscription payment");
    }

    @Test
    void logicallyIdenticalTransactionsShareAFingerprintRegardlessOfLineNumber() {
        NormalizedTransaction a = Fixtures.normalized("txn-1");
        NormalizedTransaction b = new NormalizedTransaction(999, a.providerId(), a.transactionId(), a.accountId(),
                a.merchantId(), a.amount(), a.currency(), a.timestamp(), a.description());

        assertThat(FingerprintCalculator.fingerprint(b)).isEqualTo(FingerprintCalculator.fingerprint(a));
    }

    @Test
    void anyContentDifferenceChangesTheFingerprint() {
        NormalizedTransaction base = Fixtures.normalized("txn-1");
        String fp = FingerprintCalculator.fingerprint(base);

        assertThat(FingerprintCalculator.fingerprint(Fixtures.normalized("txn-1", "145.7501", "Subscription payment"))).isNotEqualTo(fp);
        assertThat(FingerprintCalculator.fingerprint(Fixtures.normalized("txn-1", "145.7500", "Other"))).isNotEqualTo(fp);
        assertThat(FingerprintCalculator.fingerprint(Fixtures.normalized("txn-2", "145.7500", "Subscription payment"))).isNotEqualTo(fp);
        assertThat(FingerprintCalculator.fingerprint(new NormalizedTransaction(1, "other-provider", "txn-1", "acc-201",
                "merchant-18", new BigDecimal("145.7500"), "USD", Instant.parse("2026-07-20T10:25:00Z"), "Subscription payment")))
                .isNotEqualTo(fp);
    }

    @Test
    void absentDescriptionIsEncodedAsEmptyFieldNotOmitted() {
        NormalizedTransaction noDescription = Fixtures.normalized("txn-1", "145.7500", null);
        NormalizedTransaction emptyDescription = Fixtures.normalized("txn-1", "145.7500", "");

        assertThat(FingerprintCalculator.canonicalString(noDescription)).endsWith("\u0000");
        assertThat(FingerprintCalculator.fingerprint(noDescription)).isEqualTo(FingerprintCalculator.fingerprint(emptyDescription));
    }
}
