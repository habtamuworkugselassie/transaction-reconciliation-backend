package com.reconciliation.domain.model;

/** A normalised transaction with its fingerprint and risk assessment, ready to persist. */
public record ScoredTransaction(
        NormalizedTransaction transaction,
        String fingerprint,
        int fingerprintVersion,
        int riskScore,
        RiskLevel riskLevel
) {}
