# ADR-005: Summaries are derived from persisted rows, then materialised, then cached

**Status:** accepted

## Context

Summaries group by currency, risk level, merchant and account. They could be maintained
incrementally during processing or derived on read.

## Decision

Derive on read with indexed GROUP BY over `transactions` while an import runs; when the
import is terminal, compute once and store in `import_summaries` (JSONB). Redis caches
the result: 2 s TTL while live, 1 h once final. Merchant and account groups are capped at
the top 100 by count.

## Consequences

- Correct by construction: the summary is whatever has been committed, consistent with
  the progress counters from the same transactions.
- No write amplification on the hot path (incremental aggregates would mean up to one
  upsert per distinct merchant/account per batch).
- A live summary of a very large running import costs a scan of its rows; the short cache
  bounds how often that happens under polling. The final summary costs one row read.
