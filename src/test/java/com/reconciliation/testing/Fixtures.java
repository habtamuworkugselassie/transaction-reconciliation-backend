package com.reconciliation.testing;

import com.reconciliation.domain.model.NormalizedTransaction;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builders for records and NDJSON lines used across tests. */
public final class Fixtures {

    public static final String PROVIDER = "provider-a";

    private Fixtures() {
    }

    /** A fully valid raw record as the parser would produce it. */
    public static Map<String, Object> rawRecord(String transactionId) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("transactionId", transactionId);
        m.put("accountId", "acc-201");
        m.put("merchantId", "merchant-18");
        m.put("amount", new BigDecimal("145.75"));
        m.put("currency", "USD");
        m.put("timestamp", "2026-07-20T10:25:00.000Z");
        m.put("description", "Subscription payment");
        return m;
    }

    /** One valid NDJSON line. */
    public static String line(String transactionId) {
        return line(transactionId, "145.75", "USD", "2026-07-20T10:25:00.000Z", "Subscription payment");
    }

    public static String line(String transactionId, String amount, String currency, String timestamp, String description) {
        return "{\"transactionId\":\"" + transactionId + "\",\"accountId\":\"acc-201\",\"merchantId\":\"merchant-18\","
                + "\"amount\":" + amount + ",\"currency\":\"" + currency + "\",\"timestamp\":\"" + timestamp + "\""
                + (description == null ? "" : ",\"description\":\"" + description + "\"") + "}";
    }

    /** n valid lines txn-1 .. txn-n, newline-terminated. */
    public static String validLines(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            sb.append(line("txn-" + i)).append('\n');
        }
        return sb.toString();
    }

    public static NormalizedTransaction normalized(String transactionId) {
        return normalized(transactionId, "145.7500", "Subscription payment");
    }

    public static NormalizedTransaction normalized(String transactionId, String amount, String description) {
        return new NormalizedTransaction(1, PROVIDER, transactionId, "acc-201", "merchant-18", new BigDecimal(amount),
                "USD", Instant.parse("2026-07-20T10:25:00Z"), description);
    }
}
