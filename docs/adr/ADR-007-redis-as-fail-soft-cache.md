# ADR-007: Redis as a fail-soft cache, not a dependency for correctness

**Status:** accepted

## Context

Clients poll status and summaries while imports run; create requests are retried. The
stack includes Redis.

## Decision

Redis caches (a) final and live summaries and (b) idempotency key → import id, with TTLs
and an LRU memory ceiling. Every cache call is wrapped: a Redis failure is logged and the
request proceeds against the database. Redis is reported on `/actuator/health` but does
not gate readiness.

## Consequences

- Hot reads of finished imports cost one Redis GET; summary polling of running imports is
  rate-limited to one aggregation per TTL.
- A Redis outage degrades latency, never correctness or availability.
- Not used for job dispatch or locking: those need durability and conditional updates,
  which the database already provides (ADR-001).
