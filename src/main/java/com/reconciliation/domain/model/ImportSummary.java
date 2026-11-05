package com.reconciliation.domain.model;

import java.math.BigDecimal;
import java.util.List;

/** Aggregates over the persisted results of one import. */
public record ImportSummary(
        Totals totals,
        List<CurrencyTotal> byCurrency,
        RiskTotals byRiskLevel,
        List<GroupTotal> byMerchant,
        List<GroupTotal> byAccount
) {
    public record Totals(long accepted, long rejected, long duplicates) {}
    public record CurrencyTotal(String currency, long transactionCount, BigDecimal totalAmount) {}
    public record RiskTotals(long low, long medium, long high) {}
    /** Count and amount per group key; amount is only meaningful within a single currency. */
    public record GroupTotal(String key, long transactionCount, BigDecimal totalAmount) {}
}
