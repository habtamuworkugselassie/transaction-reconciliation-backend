# ADR-006: Idempotency by reservation; one transaction per batch with checkpoint

**Status:** accepted

## Context

Repeating a create request must not create a second import, including under concurrency.
Processing must survive crashes without double counting.

## Decision

The idempotency key is reserved with `INSERT ... ON CONFLICT DO NOTHING RETURNING` before
the body is read; the existing row is returned on replay and the body is not consumed.
Redis caches key → id to skip even the reservation on replays (fail-soft).

Each batch is one database transaction: lock the import row, verify lease and checkpoint,
insert transactions (`ON CONFLICT DO NOTHING RETURNING`), classify duplicates, insert
rejections (`ON CONFLICT DO NOTHING`), update counters and `last_line_committed`.

## Consequences

- Concurrent same-key requests converge on one import without application locks.
- A crash at any point loses at most one uncommitted batch, which is re-read from the
  checkpoint; a replayed batch whose commit succeeded is detected by the checkpoint and
  skipped. Delivery is effectively once.
- Batch size trades commit overhead against the amount of work redone after a crash
  (default 1000 records).
