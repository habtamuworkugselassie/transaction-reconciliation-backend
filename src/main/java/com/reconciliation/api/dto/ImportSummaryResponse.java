package com.reconciliation.api.dto;

import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.ImportSummary;
import io.swagger.v3.oas.annotations.media.Schema;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Schema(name = "ImportSummary")
public record ImportSummaryResponse(
        UUID importId,
        @Schema(description = "Import status at the time of the read") String status,
        @Schema(description = "True once the import is terminal and the summary can no longer change") boolean isFinal,
        Totals totals,
        List<CurrencyTotal> byCurrency,
        RiskTotals byRiskLevel,
        @Schema(description = "Top 100 merchants by transaction count") List<GroupTotal> byMerchant,
        @Schema(description = "Top 100 accounts by transaction count") List<GroupTotal> byAccount) {

    public record Totals(long accepted, long rejected, long duplicates) {}
    public record CurrencyTotal(String currency, long transactionCount, BigDecimal totalAmount) {}
    public record RiskTotals(long low, long medium, long high) {}
    public record GroupTotal(String id, long transactionCount, BigDecimal totalAmount) {}

    public static ImportSummaryResponse from(ImportJob job, ImportSummary s, boolean isFinal) {
        return new ImportSummaryResponse(
                job.id(), job.status().wireName(), isFinal,
                new Totals(s.totals().accepted(), s.totals().rejected(), s.totals().duplicates()),
                s.byCurrency().stream().map(c -> new CurrencyTotal(c.currency(), c.transactionCount(), c.totalAmount())).toList(),
                new RiskTotals(s.byRiskLevel().low(), s.byRiskLevel().medium(), s.byRiskLevel().high()),
                s.byMerchant().stream().map(g -> new GroupTotal(g.key(), g.transactionCount(), g.totalAmount())).toList(),
                s.byAccount().stream().map(g -> new GroupTotal(g.key(), g.transactionCount(), g.totalAmount())).toList());
    }
}
