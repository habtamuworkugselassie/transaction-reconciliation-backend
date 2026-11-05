# ADR-001: Kafka for dispatch, database leases for ownership

**Status:** accepted

## Context

Imports must be processed asynchronously by one of several workers, survive worker
crashes, never run twice concurrently, and be recoverable after restarts.

## Decision

Kafka carries a small `ImportRequested{importId}` event per import; workers are a consumer
group with `max.poll.records=1` and manual acknowledgement. Ownership is decided by the
`imports` row: every event triggers a conditional claim (`pending`, or an expired lease)
that sets `lease_owner` and `lease_expires_at`, and every later transition checks the owner.
Offsets are acknowledged only after a terminal outcome; a released attempt is negatively
acknowledged with a delay. A periodic sweep re-publishes imports still `pending` after two
sweep intervals and leases that expired.

## Consequences

- Low dispatch latency and fan-out across workers without polling the database.
- At-least-once delivery is safe: duplicate events become no-op claims.
- The database remains the single source of truth; Kafka can be wiped without losing work.
- Costs a broker (~700 MB on the droplet) and a long `max.poll.interval.ms` (1 h) because
  an import is a long unit of work; an import longer than that triggers a rebalance that
  the lease makes harmless but noisy. Alternative considered: a database-only queue with
  polling, which is simpler but adds poll latency and database load per worker.
