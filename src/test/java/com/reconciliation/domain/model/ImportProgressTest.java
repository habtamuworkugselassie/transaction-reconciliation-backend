package com.reconciliation.domain.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ImportProgressTest {

    @Test
    void processedIsAlwaysTheSumOfTheOtherCounters() {
        ImportProgress p = ImportProgress.ZERO.plus(10, 2, 1).plus(5, 0, 0);

        assertThat(p).isEqualTo(new ImportProgress(18, 15, 2, 1));
    }

    @Test
    void inconsistentCountersAreRejected() {
        assertThatThrownBy(() -> new ImportProgress(10, 5, 2, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void riskLevelBands() {
        assertThat(RiskLevel.ofScore(0)).isEqualTo(RiskLevel.LOW);
        assertThat(RiskLevel.ofScore(39)).isEqualTo(RiskLevel.LOW);
        assertThat(RiskLevel.ofScore(40)).isEqualTo(RiskLevel.MEDIUM);
        assertThat(RiskLevel.ofScore(69)).isEqualTo(RiskLevel.MEDIUM);
        assertThat(RiskLevel.ofScore(70)).isEqualTo(RiskLevel.HIGH);
        assertThat(RiskLevel.ofScore(100)).isEqualTo(RiskLevel.HIGH);
        assertThatThrownBy(() -> RiskLevel.ofScore(101)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RiskLevel.ofScore(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
