# Transaction Reconciliation Service

High-throughput NDJSON transaction import and reconciliation service.

**Stack:** Java 21 · Spring Boot 3.5 · jOOQ · PostgreSQL 16 · Kafka (KRaft) · Redis · Docker Compose · Caddy.

A file of hundreds of thousands of transactions is uploaded once, streamed to disk,
and processed asynchronously by a worker: parsed line by line, validated and
normalised, de-duplicated by a database constraint, risk-scored on a bounded CPU
pool, and persisted in atomic batches with a checkpoint. Progress, summaries and
rejected records are served from what has been committed.

- [ARCHITECTURE.md](ARCHITECTURE.md): components, flows, consistency, failure recovery, back-pressure.
- [docs/adr/](docs/adr/): the decisions and their trade-offs.
- [BENCHMARK.md](BENCHMARK.md): what was measured and what it means.
- [SUBMISSION.md](SUBMISSION.md): summary for reviewers, known gaps.
- [deploy/README.md](deploy/README.md): droplet deployment runbook; [deploy/k8s/](deploy/k8s/README.md): Kubernetes manifests.

## Quick start (Docker)

```bash
docker compose up --build
```

Brings up Postgres, Redis, Kafka, the API (`:8080`) and one worker (`:8081` for probes
and metrics). Flyway applies the schema on first start. Then:

```bash
scripts/generate-data.sh --records=100000            # writes data/transactions-100000.ndjson
curl -s -X POST http://localhost:8080/v1/imports \
     -H 'Idempotency-Key: demo-1' -H 'X-Provider-Id: acme' \
     -F file=@data/transactions-100000.ndjson         # 202 {"id":"...","status":"pending",...}
curl -s http://localhost:8080/v1/imports/<id>                      # progress
curl -s http://localhost:8080/v1/imports/<id>/summary              # reconciliation summary
curl -s 'http://localhost:8080/v1/imports/<id>/rejections?limit=50' # rejected records (cursor paginated)
curl -s -N http://localhost:8080/v1/imports/<id>/events            # live progress (server-sent events)
curl -s -X POST http://localhost:8080/v1/imports/<id>/cancel
curl -s -X POST http://localhost:8080/v1/imports/<id>/retry          # re-queue a failed import from its checkpoint
```

Swagger UI: <http://localhost:8080/swagger-ui.html> · OpenAPI: `/v3/api-docs` ·
Metrics: `/metrics` (Prometheus) · Probes: `/health/live`, `/health/ready`.

Tracing: `docker compose -f docker-compose.yml -f docker-compose.tracing.yml up --build`
adds Jaeger (<http://localhost:16686>); one trace follows an upload through HTTP, Kafka and
every batch the worker writes.

Scale workers: `docker compose up --scale worker=2`. Kafka partitions (8 by default)
bound total parallelism; each worker processes `PROCESSING_MAX_CONCURRENT_IMPORTS`
imports at once.

## Running without Docker

Requires Java 21, PostgreSQL 16, Kafka and Redis reachable on their default ports
(or set `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_HOST`).

```bash
createdb reconciliation
./mvnw spring-boot:run                       # API + worker in one process (PROCESSING_ENABLED=true)
# or
./mvnw -DskipTests package                 # skip tests for a quick local build; see "Running tests"
PROCESSING_ENABLED=false java -jar target/transaction-reconciliation-service-0.1.0-SNAPSHOT.jar   # API only
PORT=8081 java -jar target/transaction-reconciliation-service-0.1.0-SNAPSHOT.jar                 # worker
```

Migrations run automatically at startup (Flyway, `src/main/resources/db/migration`). The
jOOQ classes are generated at build time from those same migration files
(`./mvnw generate-sources`), so no database is needed to compile.

## Running tests

```bash
./mvnw test                      # unit tests only (no Docker, no database)
./mvnw -Pintegration verify      # unit + integration tests (*IT) on PostgreSQL and Redis
```

235 unit tests and 75 integration tests.

**Unit tests** (`src/test/java`, `*Test`) cover the domain rules (validation,
normalisation, fingerprint, risk score, status transitions), the upload helpers (content
sniffing, bounded hashing, filename sanitising), the NDJSON reader and parser, the retry
policy and failure classifier, the scoring pool, the API-key filter, the dead-letter
classification, the RSS reader and every use case (including retry and retention) against
in-memory fakes (`com.reconciliation.testing`).

**Integration tests** (`*IT`, run by Failsafe only with `-Pintegration`) need PostgreSQL
and, for one class, Redis:

- the jOOQ adapters (`JooqImportRepositoryIT`, `JooqBatchWriterIT`,
  `JooqRejectionRepositoryIT`, `JooqSummaryRepositoryIT`, `JooqRetryAndRetentionIT`):
  idempotent reservation under concurrency, leases and `SKIP LOCKED` claims, atomic batches
  and checkpoints, duplicate detection, keyset pagination, summary aggregation, retry
  conditions, and retry versus retention purge never both applying;
- `RedisUploadRateLimiterIT`: the per-provider limit on a real Redis, covering window
  arithmetic, key TTLs, no overshoot under 40 concurrent requests, and failing open;
- `ApiKeyAuthIT`: with API keys enabled, 401 without a key, the key decides the provider,
  and one provider cannot see or act on another provider's import on any endpoint;
- `ImportApiIT`: the full Spring context on a random port over real HTTP. It covers 202
  on upload, replay with the same key, concurrent same-key POSTs converging on one import,
  400/413/415 rejections, rejection pagination, cancel (pending, processing, completed),
  summary, retry of a failed import (resumed from its checkpoint, stale final summary
  replaced), the server-sent progress stream until completion, `/health/*` and
  `/metrics`, and a crash-and-resume run where one worker
  abandons an import mid-file and a second resumes it from the checkpoint after the lease
  expires, with every record counted exactly once. Kafka and Redis are not needed: the
  test swaps in a database-only `ImportJobQueue`, no-op caches and a controllable clock,
  and drives the processing use case itself (`app.processing.enabled=false`).

Where the database comes from:

| Setting | Effect |
|---|---|
| `TEST_DATABASE_URL` set (plus `TEST_DATABASE_USERNAME` / `TEST_DATABASE_PASSWORD`, default `postgres`/`postgres`) | uses that server. **The schema is dropped and re-created**, so point it at a throwaway database only |
| not set | starts a `postgres:16-alpine` container through Testcontainers (Docker must be running) |
| `TEST_REDIS_HOST` (+ `TEST_REDIS_PORT`, default 6379) set | `RedisUploadRateLimiterIT` uses that Redis and **flushes it** |
| not set | starts `redis:7-alpine` through Testcontainers |

```bash
# against a local throwaway database
createdb reconciliation_test
TEST_DATABASE_URL=jdbc:postgresql://localhost:5432/reconciliation_test ./mvnw -Pintegration verify
```

CI (`.github/workflows/ci.yml`) runs `./mvnw -Pintegration verify` on every push and
pull request against Postgres 16 and Redis 7 service containers.

## Generating test data

```bash
scripts/generate-data.sh --records=500000 [--out=path] [--invalid-rate=0.02] [--duplicate-rate=0.005] [--seed=42] [--prefix=txn]
```

Produces a realistic mix: valid records (with lower-case currencies, offsets, unicode,
unexpected fields), invalid JSON, non-objects, missing fields, wrong types, bad
amounts/currencies/timestamps, over-long descriptions and lines, control characters,
blank lines, and duplicates (half identical, half with changed content). The same seed
and options always produce the same file.

## Benchmark and load test

```bash
scripts/benchmark.sh 500000                   # generate (if needed), upload, measure; writes benchmark-results/*.md
k6 run -e IMPORT_ID=<running import> scripts/load-test.js   # API latency thresholds during an import
```

See [BENCHMARK.md](BENCHMARK.md) for results and interpretation.

## Configuration

Everything is an environment variable with a default in `application.yml`; the typed,
validated structure is `AppProperties`. The ones that matter for capacity:

| Variable | Default | Bounds |
|---|---|---|
| `UPLOAD_MAX_FILE_SIZE_BYTES` | 209715200 (200 MB) | enforced mid-stream |
| `UPLOAD_MAX_CONCURRENT` | 4 | uploads admitted at once; the rest get 429 |
| `PROCESSING_ENABLED` | true | false = API-only instance |
| `PROCESSING_MAX_CONCURRENT_IMPORTS` | 2 | imports per worker instance (= Kafka consumer threads) |
| `PROCESSING_BATCH_SIZE` | 1000 | records per database transaction and checkpoint |
| `PROCESSING_MAX_IN_FLIGHT_BATCHES` | 2 | batches materialised per import (memory bound) |
| `PROCESSING_MAX_LINE_LENGTH` | 16384 | longer lines are rejected without being buffered |
| `PROCESSING_LEASE_DURATION` / `HEARTBEAT_INTERVAL` | 30s / 10s | abandoned-job detection |
| `PROCESSING_MAX_ATTEMPTS` | 3 | processing attempts before an import is failed |
| `RISK_WORKER_THREADS` | 0 (= CPUs) | CPU scoring pool |
| `RISK_QUEUE_CAPACITY` | 4000 | scoring chunks queued before producers block |
| `RISK_HASH_ITERATIONS` | 200 | CPU cost per record of the scoring algorithm |
| `DB_POOL_SIZE` | 10 | Hikari connections |
| `RETRY_MAX_ATTEMPTS` / `RETRY_INITIAL_BACKOFF` / `RETRY_MAX_BACKOFF` | 5 / 200ms / 5s | batch-write retry |
| `SHUTDOWN_GRACE_PERIOD` | 30s | time given to in-flight batches on SIGTERM |
| `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_IMPORTS_TOPIC` / `KAFKA_IMPORTS_PARTITIONS` | localhost:9092 / imports.requested / 8 | dispatch |
| `REDIS_HOST` / `REDIS_PORT` | localhost / 6379 | caches (fail-soft) |
| `CACHE_SUMMARY_LIVE_TTL` / `CACHE_SUMMARY_FINAL_TTL` / `CACHE_IDEMPOTENCY_TTL` | 2s / 1h / 24h | Redis TTLs |
| `SPRING_PROFILES_ACTIVE=json` | – | JSON structured logs |
| `PROCESSING_MAX_MANUAL_RETRIES` | 3 | `POST .../retry` calls allowed per import |
| `UPLOAD_PROVIDER_RATE_LIMIT` / `UPLOAD_PROVIDER_RATE_WINDOW` | 0 (off) / 1m | uploads per provider per window, in Redis; over the limit: 429 with `Retry-After`; fails open |
| `API_KEYS_ENABLED` / `API_KEYS` | false / – | opt-in auth on `/v1`; `API_KEYS` is `<sha256-hex-of-key>:<providerId>,...` (hashes only) |
| `RETENTION_ENABLED` / `RETENTION_UPLOADED_FILES_AFTER` / `RETENTION_INTERVAL` / `RETENTION_BATCH_SIZE` | false / 7d / 1h / 100 | delete uploaded files of terminal imports (worker role) |
| `EVENTS_MAX_CONNECTIONS` / `EVENTS_POLL_INTERVAL` / `EVENTS_HEARTBEAT_INTERVAL` / `EVENTS_TIMEOUT` | 100 / 1s / 15s / 30m | progress streams per instance; over the limit: 429 |
| `MANAGEMENT_OTLP_TRACING_ENDPOINT` / `TRACING_SAMPLING_PROBABILITY` | – / 1.0 | export spans over OTLP (unset: traced in-process only, ids in logs) |

**API keys.** Generate a key and configure only its hash:

```bash
KEY=$(openssl rand -hex 32); echo "give this to the provider: $KEY"
echo "API_KEYS=$(printf %s "$KEY" | shasum -a 256 | cut -d' ' -f1):acme"
```

Clients send `Authorization: Bearer <key>` (or `X-API-Key`). The key's provider becomes
the import's provider, a contradicting `X-Provider-Id` gets 403, and other providers'
imports answer 404. Health, metrics and Swagger stay open.

## API

| Method | Path | Notes |
|---|---|---|
| POST | `/v1/imports` | multipart `file`, header `Idempotency-Key` (required), `X-Provider-Id` (optional). 202 |
| GET | `/v1/imports/{id}` | status + progress |
| POST | `/v1/imports/{id}/cancel` | 202; stops at the next batch boundary |
| POST | `/v1/imports/{id}/retry` | 202; failed import back to `pending`, resumes from its checkpoint; 409 if not retryable |
| GET | `/v1/imports/{id}/events` | `text/event-stream` of `progress` events until the import is terminal |
| GET | `/v1/imports/{id}/summary` | totals, by currency, by risk level, top merchants/accounts |
| GET | `/v1/imports/{id}/rejections?limit=50&cursor=` | keyset pagination, max 200 |
| GET | `/health/live`, `/health/ready` | liveness / readiness |
| GET | `/metrics` | Prometheus |

Errors: `{"error":{"code":"IMPORT_FILE_TOO_LARGE","message":"...","requestId":"..."}}`.
Codes are stable (`ErrorCode`); `X-Request-Id` is echoed and logged.

## Monitoring runtime health

How saturation shows up, and where to look (all on `/metrics`):

| Symptom | Metric | Threshold that means trouble |
|---|---|---|
| CPU saturation (scoring) | `risk_scorer_active` = `risk_scorer_pool_size` and `risk_scorer_queue_depth` growing; `process_cpu_usage` ≈ 1 | queue depth > 0 for minutes: parsing is outrunning scoring; throughput is CPU-bound |
| Downstream I/O latency (database) | `import_batch_duration_seconds` p95 rising while `risk_scorer_queue_depth` is 0; `hikaricp_connections_pending` > 0 | batch p95 > 1 s: Postgres is the bottleneck (disk, locks, vacuum) |
| Queue backlog | `import_queue_depth` (pending imports), `import_queue_inflight`; Kafka consumer lag | depth growing while `import_worker_active` = capacity: add workers or partitions |
| API latency | `http_server_requests_seconds` p99 by uri; `tomcat_threads_busy_threads` | p99 > 250 ms on `/v1/imports/{id}` during an import: check CPU first |
| Memory | `jvm_memory_used_bytes{area="heap"}`, `jvm_gc_pause_seconds`; `process_memory_rss_bytes` (Linux only: whole-process resident memory including native memory, which is what a container limit counts) | heap climbing across batches: a bound is not being respected; RSS above ~85 % of the container limit: risk of an OOM kill, even with a healthy heap |
| Retries | `retries_total`, `retries_exhausted_total` | any `exhausted`: look at the import's `failureReason` |
| Uploads | `upload_slots_available`, `uploads_rejected_total{reason}` | sustained `concurrency` rejections: raise the slot count or add API replicas |
| Dead letters | `kafka_dead_letter_records_total{reason}` | any increase: a producer is sending records the worker cannot read; inspect `imports.requested.DLT` |
| Progress streams | `sse_streams_active` vs `sse_streams_capacity` | at capacity: clients get 429; raise `EVENTS_MAX_CONNECTIONS` or add API replicas |
| Retries / retention | `imports_retried_total`, `retention_files_purged_total` | retries climbing: imports fail for a recurring reason; check `failureReason` |

CPU saturation and downstream latency look alike from the outside (throughput drops)
but differ in `risk_scorer_queue_depth` (grows under CPU pressure, stays 0 under DB
pressure) and `import_batch_duration` (grows under DB pressure). Request handling runs on
virtual threads and scoring on a separate fixed platform-thread pool sized to the CPU
count, so the JVM scheduler always has capacity for a request; the risk of blocking the
API is a pinned carrier thread, visible as `jvm_threads_states` and rising request p99.

Likely bottlenecks in order: the scoring pool (CPU), then the single writer per import
(sequential batch commits), then Postgres index maintenance on `transactions`.

## Known limitations

- File storage is a local directory shared between API and worker (one host or a
  shared volume). An S3-compatible adapter behind the same port is the next step.
- Summaries group merchants and accounts to the top 100 by count; full group lists would
  need their own paginated endpoint.
- Authentication is opt-in API keys (off by default so the public deployment stays testable);
  JWT/OIDC would replace the key lookup in the same filter. There are no roles: a key is a
  provider.
- Tracing covers HTTP, Kafka and batch writes; JDBC statements are not individual spans.
- Uploads are not resumable: an interrupted upload is retried in full with the same
  `Idempotency-Key`. Processing, by contrast, resumes from its checkpoint.
- Kafka and Redis run single-node in the compose files; production replication is an
  operations decision outside this repository.
