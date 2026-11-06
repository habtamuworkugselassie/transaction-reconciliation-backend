# Submission

**Repository:** https://github.com/habtamuworkugselassie/transaction-reconciliation-backend
**Deployment:** http://165.245.247.93:9001 (single DigitalOcean droplet behind Caddy; plain
HTTP on port 9001 because the deployment has no DNS name and ports 80/443 are used by
another service on the droplet, see `deploy/README.md` §1).
Swagger UI: http://165.245.247.93:9001/swagger-ui.html · Readiness: http://165.245.247.93:9001/health/ready ·
Metrics: http://165.245.247.93:9001/metrics. Runbook: `deploy/README.md`.

## Summary

A Java 21 / Spring Boot 3.5 service that imports NDJSON transaction files of any size with
bounded memory, processes them asynchronously through Kafka-dispatched workers, and exposes
progress, reconciliation summaries and rejected records over HTTP. PostgreSQL (via jOOQ) is
the correctness model: idempotent creation, duplicate detection, job ownership and
checkpointed batches are all database constraints and conditional updates. Redis caches
hot reads and fails soft.

## Architecture overview

Hexagonal layering (`domain` ← `application` ← `infrastructure`/`api`, wired in `config`).
Upload: streamed multipart → sniff/count/hash → atomic file → idempotency reservation →
`pending` → Kafka event. Worker: claim lease → stream file from checkpoint → parse,
validate, normalise → score on a fixed CPU pool → one transaction per batch (insert with
`ON CONFLICT DO NOTHING RETURNING`, rejections, counters, checkpoint) → terminal
transition → acknowledge offset. Details: `ARCHITECTURE.md`; decisions: `docs/adr/`.

## Important decisions and trade-offs

1. **Kafka for dispatch, database leases for ownership** (ADR-001): at-least-once
   delivery made harmless by conditional claims; costs a broker and a long poll interval.
2. **jOOQ, code generated from the Flyway migrations** (ADR-002): the exact SQL of the
   design runs; no ORM semantics in the batch path.
3. **Scoring on a fixed platform-thread pool sized to cores** (ADR-003): CPU isolation
   without a second service; back-pressure through a bounded queue.
4. **One transaction per batch, checkpoint inside it** (ADR-006): effectively-once
   processing; batch size trades commit overhead against redo after a crash.
5. **Summaries derived from rows, materialised when final, cached in Redis**
   (ADR-005, ADR-007): correct by construction; no write amplification; Redis is never a
   correctness dependency.
6. **Local shared file storage behind a port** (ADR-004): single write, works on one host
   or a shared volume; S3 adapter is the obvious next step for multi-host.
7. **Progress streaming polls persisted state** (ADR-008): server-sent events report only
   committed progress, with no push channel between worker and API.

## Bonus features

All of these leave the core guarantees unchanged, and the optional ones are off by
default. Details: ARCHITECTURE → Optional features.

| Feature | What it adds | Default |
|---|---|---|
| Import retry endpoint | `POST /v1/imports/{id}/retry`: a failed import resumes from its checkpoint with a fresh, bounded attempt budget; idempotent; capped per import | on |
| Dead-letter topic | unreadable Kafka records go to `imports.requested.DLT` with exception headers, counted by reason | on |
| Server-sent progress events | `GET /v1/imports/{id}/events`, bounded streams, one database read per watched import | on |
| OpenTelemetry tracing | one trace from upload through Kafka to every batch write; trace ids in logs; OTLP export + Jaeger overlay | in-process on, export opt-in |
| Per-provider rate limits | Redis fixed window, atomic Lua script, 429 with `Retry-After`, fails open | off |
| API-key authentication | hashed keys mapped to providers; ownership enforced on every endpoint | off |
| Data retention | worker job deletes uploaded files of old terminal imports; exclusive with retry | off |
| Kubernetes manifests | `deploy/k8s` (kustomize), validated with kubeconform | – |
| Also | OpenAPI/Swagger, Testcontainers, processing checkpoints, storage port, process RSS metric | – |

Verified on the running compose stack, beyond the automated tests:
- **Tracing:** one trace in Jaeger (HTTP → Kafka send → worker receive → 3 batch-write
  spans, correctly parented).
- **Dead-lettering:** a non-JSON record and an event without an id both landed in the DLT
  with their exception headers, and the worker kept processing.
- **SSE:** a progress stream followed a real worker until completion.

## Tests

`./mvnw test` runs 235 unit tests (domain rules, upload helpers, reader and parser, retry
policy, scoring pool, the API-key filter, and every use case against in-memory fakes).
`./mvnw -Pintegration verify` adds 75 integration tests on PostgreSQL and Redis:
- the jOOQ adapters: idempotency under concurrency, leases, atomic checkpointed batches,
  duplicate detection, pagination, summaries, retry conditions, and retry versus purge
  exclusivity;
- the Redis rate limiter;
- `ApiKeyAuthIT`;
- `ImportApiIT`, which runs the full application over real HTTP. That covers upload,
  replay, concurrent same-key POSTs, 400/413/415, rejection pagination, cancel, summary,
  retry, the SSE stream, health, metrics, and a crash-and-resume run.

CI (`.github/workflows/ci.yml`) runs both on every push against Postgres 16 and Redis 7
service containers. See README → Running tests.

## Benchmark summary

500,000 records (108 MB) processed in 17.5 s (~28.5k records/s) on 10 cores with the
compose stack, status p99 of 13 ms during the import, and 76 MB peak heap. On 4 vCPU it
reached ~8–9k records/s (200k records in ~21 s, including a SIGTERM and resume). Counts
match the generator exactly in every run. See `BENCHMARK.md`.

## What I would improve with more time

- Automated end-to-end tests with a real Kafka (Testcontainers) for the listener,
  redelivery and the dead-letter path. Today these are verified by hand on the compose
  stack; `ImportApiIT` uses a database-only queue.
- Resumable (chunked) uploads. Processing already resumes from checkpoints; an interrupted
  upload is retried in full with the same `Idempotency-Key`.
- Parallel batch writers per import and `COPY`-based inserts.
- S3-compatible storage adapter (also removes the `ReadWriteMany` requirement on Kubernetes).
- JWT/OIDC in place of API keys, and roles beyond "a key is a provider".

## Known risks

- Single-node Kafka and Postgres in the provided compose files (operations concern).
- `max.poll.interval.ms` of 1 h: an import slower than that causes a rebalance; the lease
  keeps it correct but it is noisy. Raise it or split very large files.
- Local file storage ties API and worker to one host or a shared volume.

## Incomplete requirements

- The 500,000-record benchmark was measured on a development machine (10 cores), not on
  the droplet. `scripts/benchmark.sh 500000 https://<host>` reproduces it there.
- Load testing beyond the k6 script (`scripts/load-test.js`) is not automated in CI.
