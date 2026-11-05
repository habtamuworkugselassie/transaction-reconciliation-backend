package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.RiskLevel;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The documented algorithm: five bounded components, deterministic, capped at 100. */
class RiskScoreCalculatorTest {

    private final RiskScoreCalculator calculator = new RiskScoreCalculator(3);

    private static NormalizedTransaction tx(String amount, String timestamp, String description) {
        return new NormalizedTransaction(1, Fixtures.PROVIDER, "txn-1", "acc-1", "merchant-1",
                new BigDecimal(amount), "USD", Instant.parse(timestamp), description);
    }

    @Test
    void scoreIsDeterministicAcrossCallsAndInstances() {
        NormalizedTransaction t = Fixtures.normalized("txn-1");

        ScoredTransaction a = calculator.score(t);
        ScoredTransaction b = new RiskScoreCalculator(3).score(t);

        assertThat(a).isEqualTo(b);
        assertThat(a.fingerprint()).isEqualTo(FingerprintCalculator.fingerprint(t));
        assertThat(a.fingerprintVersion()).isEqualTo(FingerprintCalculator.VERSION);
    }

    @Test
    void scoreIsAlwaysWithinZeroAndHundredAndLevelMatchesBand() {
        for (int i = 0; i < 500; i++) {
            NormalizedTransaction t = new NormalizedTransaction(i, Fixtures.PROVIDER, "txn-" + i, "acc-" + (i % 7),
                    "merchant-" + (i % 13), new BigDecimal(1 + (i * 7919L) % 2_000_000).setScale(4), "USD",
                    Instant.parse("2026-07-20T00:00:00Z").plusSeconds(i * 3571L),
                    i % 3 == 0 ? null : "desc " + "x".repeat(i % 250) + (i % 5 == 0 ? " 123456789" : ""));
            ScoredTransaction s = calculator.score(t);
            assertThat(s.riskScore()).isBetween(0, 100);
            assertThat(s.riskLevel()).isEqualTo(RiskLevel.ofScore(s.riskScore()));
        }
    }

    @ParameterizedTest
    @CsvSource({"0.0100, 0", "1.0000, 2", "100.0000, 12", "10000.0000, 23", "1000000.0000, 35", "50000000.0000, 35"})
    void amountComponentGrowsLogarithmicallyAndCapsAt35(String amount, int expected) {
        assertThat(calculator.amountComponent(tx(amount, "2026-07-20T12:00:00Z", "d"))).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"2026-07-20T00:00:00Z, 20", "2026-07-20T05:59:59Z, 20", "2026-07-20T06:00:00Z, 10",
            "2026-07-20T07:59:59Z, 10", "2026-07-20T08:00:00Z, 0", "2026-07-20T21:59:59Z, 0",
            "2026-07-20T22:00:00Z, 15", "2026-07-20T23:59:59Z, 15"})
    void hourComponentUsesUtcBands(String timestamp, int expected) {
        assertThat(calculator.hourComponent(tx("10.0000", timestamp, "d"))).isEqualTo(expected);
    }

    @Test
    void hourComponentIsEvaluatedInUtcNotLocalTime() {
        // 23:30 in UTC+2 is 21:30 UTC -> daytime band.
        NormalizedTransaction t = tx("10.0000", "2026-07-20T21:30:00Z", "d");
        assertThat(calculator.hourComponent(t)).isZero();
    }

    @Test
    void descriptionComponent() {
        assertThat(calculator.descriptionComponent(tx("1", "2026-07-20T12:00:00Z", null))).isEqualTo(10);
        assertThat(calculator.descriptionComponent(tx("1", "2026-07-20T12:00:00Z", "x".repeat(201)))).isEqualTo(15);
        assertThat(calculator.descriptionComponent(tx("1", "2026-07-20T12:00:00Z", "card 12345678"))).isEqualTo(10);
        assertThat(calculator.descriptionComponent(tx("1", "2026-07-20T12:00:00Z", "card 1234567"))).isZero();
        assertThat(calculator.descriptionComponent(tx("1", "2026-07-20T12:00:00Z", "Coffee"))).isZero();
    }

    @Test
    void merchantAndFingerprintComponentsAreStableBucketsBelow16() {
        NormalizedTransaction t = Fixtures.normalized("txn-1");
        int merchant = calculator.merchantComponent(t);
        int fingerprint = calculator.fingerprintComponent(FingerprintCalculator.fingerprint(t));

        assertThat(merchant).isBetween(0, 15);
        assertThat(fingerprint).isBetween(0, 15);
        assertThat(calculator.merchantComponent(t)).isEqualTo(merchant);
        assertThat(calculator.fingerprintComponent(FingerprintCalculator.fingerprint(t))).isEqualTo(fingerprint);
    }

    @Test
    void hashIterationCountChangesTheFingerprintComponentButNotTheRangeOrDeterminism() {
        String fp = FingerprintCalculator.fingerprint(Fixtures.normalized("txn-1"));
        int a = new RiskScoreCalculator(1).fingerprintComponent(fp);
        int b = new RiskScoreCalculator(1).fingerprintComponent(fp);
        assertThat(a).isEqualTo(b).isBetween(0, 15);
        assertThat(new RiskScoreCalculator(50).fingerprintComponent(fp)).isBetween(0, 15);
    }

    @Test
    void worstCaseInputIsCappedAtHundredAndRatedHigh() {
        // amount 35 + hour 20 + description 15 = 70 before the two hash buckets (0-30): 70..100.
        NormalizedTransaction t = tx("5000000.0000", "2026-07-20T03:00:00Z", "y".repeat(300));
        ScoredTransaction s = calculator.score(t);
        assertThat(s.riskScore()).isBetween(70, 100);
        assertThat(s.riskLevel()).isEqualTo(RiskLevel.HIGH);
    }

    @Test
    void benignInputIsLow() {
        // amount 1 (~2) + daytime 0 + normal description 0 + buckets <= 30 -> at most 32.
        NormalizedTransaction t = tx("1.0000", "2026-07-20T12:00:00Z", "Coffee");
        ScoredTransaction s = calculator.score(t);
        assertThat(s.riskScore()).isLessThanOrEqualTo(32);
        assertThat(s.riskLevel()).isEqualTo(RiskLevel.LOW);
    }

    @Test
    void rejectsNonPositiveIterationCount() {
        assertThatThrownBy(() -> new RiskScoreCalculator(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
