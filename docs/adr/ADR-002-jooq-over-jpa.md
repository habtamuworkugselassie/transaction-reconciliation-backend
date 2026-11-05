# ADR-002: jOOQ instead of JPA

**Status:** accepted

## Context

The hot path is bulk inserts with `ON CONFLICT DO NOTHING RETURNING`, array-based
`unnest`, conditional `UPDATE ... RETURNING`, `FOR UPDATE SKIP LOCKED` and GROUP BY
aggregations. These are the correctness mechanism, not incidental queries.

## Decision

jOOQ with code generated at build time from the Flyway migrations (DDLDatabase), so the
generated schema can never drift from what Flyway applies and no database is needed to
compile. Transactions are Spring-managed (`TransactionTemplate`) around the batch writer.

## Consequences

- Every statement is explicit and type-checked; the SQL in the design document is the SQL
  that runs.
- No persistence context, no lazy loading, no flush-ordering surprises in batch writes.
- Developers must know SQL; there is no entity graph. Mapping to domain records is done by
  hand in the adapters (small, and it keeps the domain free of persistence annotations).
