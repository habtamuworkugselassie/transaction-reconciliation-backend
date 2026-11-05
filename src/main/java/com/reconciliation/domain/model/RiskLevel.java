package com.reconciliation.domain.model;

import java.util.Locale;

/** Risk bands as defined by the assignment: 0-39 low, 40-69 medium, 70-100 high. */
public enum RiskLevel {
    LOW, MEDIUM, HIGH;

    public static RiskLevel ofScore(int score) {
        if (score < 0 || score > 100) {
            throw new IllegalArgumentException("score out of range: " + score);
        }
        if (score <= 39) {
            return LOW;
        }
        return score <= 69 ? MEDIUM : HIGH;
    }

    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
