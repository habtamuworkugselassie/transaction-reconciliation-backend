# ADR-003: Risk scoring on a fixed platform-thread pool

**Status:** accepted

## Context

Scoring is CPU-bound (iterated hashing). It must not delay latency-sensitive requests.
Options: run it inline on the processing thread, on virtual threads, on a thread pool, in
a separate process, or in a separate service.

## Decision

A `ThreadPoolExecutor` with `cores` platform threads and a bounded queue whose rejection
handler blocks the submitter. Batches are split into 100-record chunks so all cores share
one batch. Request handling uses virtual threads; the worker's reader and writer threads
are separate from the pool.

## Consequences

- Scoring can never occupy more than the machine's CPUs, leaving scheduler capacity for
  request threads; on the droplet the API is a separate process anyway.
- Back-pressure flows from CPU saturation to file reading through the bounded queue.
- Virtual threads were rejected for this work: they add nothing for CPU-bound tasks and
  would pin carriers. A separate process would isolate better but doubles deployment
  complexity for a 4 GB host; the role split (api/worker) already gives process isolation
  where it matters.
