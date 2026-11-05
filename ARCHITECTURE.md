# Architecture

## Components

```mermaid
flowchart LR
    Client -->|multipart stream| API
    subgraph API["API instance (PROCESSING_ENABLED=false)"]
        Ctl[ImportController] --> Create[CreateImportUseCase]
        Ctl --> Read[Get / Summary / Rejections / Cancel use cases]
    end
    Create -->|stream, hash, sniff| Storage[(Upload storage)]
    Create -->|reserve key, mark pending| PG[(PostgreSQL)]
    Create -->|ImportRequested| Kafka[(Kafka)]
    Create -.->|idem key, summaries| Redis[(Redis)]
    Read --> PG
    Read -.-> Redis
    subgraph Worker["Worker instance (PROCESSING_ENABLED=true)"]
        Listener[ImportWorker<br/>Kafka listener, N threads] -->|claim lease| PG
        Listener --> Proc[ProcessImportUseCase]
        Proc --> Reader[NdjsonLineReader<br/>bounded line buffer]
        Proc --> Val[TransactionValidator<br/>normalise]
        Proc --> Pool[WorkerPoolRiskScorer<br/>CPU threads = cores]
        Proc --> Writer[JooqBatchWriter<br/>1 tx per batch]
    end
    Kafka --> Listener
    Storage --> Reader
    Writer --> PG
```

### Module boundaries and dependency direction

```
api  ──►  application  ──►  domain
infrastructure  ──►  application, domain
config (composition root)  ──►  everything
```

- `domain`: pure rules. `TransactionValidator`, `FingerprintCalculator`,
  `RiskScoreCalculator`, `ImportStatus` transitions, the error model. No framework imports.
- `application`: use cases and ports. `CreateImportUseCase`, `ProcessImportUseCase`,
  `CancelImportUseCase`, `GetImportSummaryUseCase`, `ListRejectionsUseCase`; ports
  `ImportRepository`, `BatchWriter`, `FileStorage`, `ImportJobQueue`, `RiskScorer`,
  `RetryPolicy`, `Clock`, `IdGenerator`, `MetricsRecorder`, `SummaryCache`,
  `IdempotencyCache`. Use cases receive every dependency through the constructor and can
  run with in-memory fakes.
- `infrastructure`: adapters. jOOQ repositories, local file storage, Kafka queue, Redis
  caches, the scoring thread pool, retry policy and failure classifier, Micrometer.
- `api`: controllers, DTOs, error mapping, request-id filter, streaming multipart reader.
- `config`: `ApplicationAssembly`, `PersistenceConfig`, `MessagingConfig`, `CacheConfig`,
  `ProcessingConfig`, `ObservabilityConfig`. The only place that instantiates adapters.

**Dependency injection** is constructor injection throughout. There is no service
locator, no static registry and no `@Autowired` field; Spring's container is used only
in `config` to assemble the graph. Database clients appear only in
`infrastructure.persistence`.

**Logging is the one deliberate exception.** Classes get their logger from a
`private static final Logger` (SLF4J) rather than through the constructor. SLF4J is
already an abstraction: code depends only on the `org.slf4j` API, and the backend
(Logback, with the JSON encoder under the `json` profile) is chosen by configuration,
not by the code. Injecting a logger into every constructor would add a parameter to
every class without making anything easier to replace. Tests can capture logs through
the backend. Per-request and per-import context (`requestId`, `importId`, `workerId`,
`batch`, ...) travels in the MDC instead of logger instances. Metrics are different:
use cases emit them through the injected `MetricsRecorder` port, so they stay free of
Micrometer and tests can assert on what was recorded (`RecordingMetrics`).

## Import status lifecycle

| From | To | When |
|---|---|---|
| `uploading` | `pending` | file stored completely, import enqueued |
| `uploading` | `failed` | empty, too large, wrong type, interrupted, or stale (reaper) |
| `pending` | `processing` | a worker claims it |
| `pending` | `cancelled` | cancel before any worker claims it |
| `pending` | `failed` | allowed by `ImportStatus`; no repository method performs it today (attempts are counted and failed from `processing`) |
| `processing` | `completed` | every line processed |
| `processing` | `cancelling` | cancel requested while a worker holds it |
| `processing` | `cancelled` | the worker observed the cancel and finalised |
| `processing` | `pending` | released: shutdown or a transient failure, resumed later from the checkpoint |
| `processing` | `failed` | permanent failure or attempts exhausted |
| `cancelling` | `cancelled` | the worker (or the next claimer of an expired lease) finalises |
| `cancelling` | `failed` | failure while winding down |

`completed`, `failed` and `cancelled` are terminal.

The assignment lists `pending`, `processing`, `completed`, `failed`, `cancelling` and
`cancelled`. The service adds **`uploading`**: the idempotency key is reserved and the
body is streaming to storage, but the file is not complete yet. It is needed because the
key is reserved *before* the body is read. That is what makes concurrent requests with the
same key converge on one import without reading the body twice. Between the reservation
and the end of the stream the row exists but has no file, so a worker must not claim it.
Calling that state `pending` would let a worker claim an import whose file isn't fully
written. With a separate state, neither the claim query nor the claimable partial index
(`pending`, `processing`, `cancelling`) ever sees it. If the API dies mid-upload, the reaper turns rows stuck in
`uploading` into `failed / UPLOAD_INTERRUPTED`, and the same key can be retried.
Clients normally never observe `uploading`: the 202 response is sent after the upload,
when the import is already `pending`. They see it only as the status a concurrent
duplicate request may return while the first request is still streaming. Allowed
transitions are enforced in `ImportStatus` (tested in `ImportStatusTest`) and by the
conditional `UPDATE ... WHERE status = ...` in every repository method.

## Processing workflow

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant S as Storage
    participant P as Postgres
    participant K as Kafka
    participant W as Worker
    participant R as Scoring pool

    C->>A: POST /v1/imports (Idempotency-Key)
    A->>P: INSERT imports ON CONFLICT (key) DO NOTHING
    alt key already used
        A-->>C: 202 existing import (body not read)
    else new
        A->>S: stream body (sniff, count, hash, cap)
        A->>P: file metadata; uploading -> pending
        A->>K: ImportRequested{importId}
        A-->>C: 202 {id, status: pending}
    end
    K->>W: ImportRequested
    W->>P: UPDATE ... SET lease WHERE claimable (SKIP LOCKED)
    loop every batch of 1000 lines
        W->>S: read lines (resume after checkpoint)
        W->>W: parse, validate, normalise
        W->>R: score batch (bounded queue)
        R-->>W: scored
        W->>P: BEGIN; lock import row; INSERT tx ON CONFLICT DO NOTHING RETURNING;<br/>INSERT rejections; UPDATE counters + checkpoint; COMMIT
        W->>P: cancel_requested? (between batches)
    end
    W->>P: completed | cancelled | failed | pending (released)
    W->>K: ack (or nack with delay when released)
```

### Step by step

1. **Upload** (`CreateImportUseCase`). Header validation, upload slot (429 if none),
   idempotency reservation, then the body is streamed through a sniffer (first bytes must
   be `{` after optional BOM; gzip/zip/PDF/NUL are refused) and a counting SHA-256 stream
   that throws at the byte limit. Storage writes `<key>.part` and renames atomically.
   Only then is the row marked `pending` and the event published.
2. **Dispatch** (`KafkaImportJobQueue`, `ImportWorker`). Each worker instance runs
   `maxConcurrentImports` consumer threads with `max.poll.records=1`. An event triggers a
   conditional claim; the claim, not the message, decides ownership.
3. **Read** (`NdjsonLineReader`). 64 KiB chunks, lines split on `\n`, physical line
   numbers, `\r\n`, BOM and a last line without newline handled, over-long lines flagged
   and discarded. Lines up to the checkpoint are skipped without parsing.
4. **Parse + validate + normalise** (`RecordParser`, `TransactionValidator`). Strict
   JSON (no NaN, no comments, bounded nesting), then the rules below.
5. **Score** (`WorkerPoolRiskScorer`). Batches are split into 100-record chunks on a
   fixed pool of `cores` platform threads with a bounded queue; a full queue blocks the
   submitting thread (back-pressure), never the API.
6. **Persist** (`JooqBatchWriter`). One transaction per batch (see consistency below).
7. **Finish**. Terminal transitions are conditional on the lease; the Kafka offset is
   acknowledged only after that.

### Normalisation rules

| Field | Rule |
|---|---|
| all strings | trimmed; control characters rejected in identifiers, replaced by spaces in the description |
| `transactionId`, `accountId`, `merchantId` | 1–128 chars after trimming |
| `amount` | JSON number, > 0, ≤ 4 decimals, < 10^15; stored with scale 4 |
| `currency` | trimmed, upper-cased, must be an ISO 4217 code (`usd` → `USD` accepted; see below) |
| `timestamp` | ISO-8601 with explicit offset or `Z`; converted to UTC, truncated to ms |
| `description` | optional; whitespace-collapsed; ≤ 500 chars; empty → null |
| unknown fields | ignored |
| blank lines | skipped, not counted |

**Why lower-case currencies are accepted.** The assignment specifies a "three-letter
uppercase currency code" and also asks the service to "normalize currency codes". The
service reads "uppercase" as the *stored, canonical* form rather than as an input rule:
`usd` and ` USD ` are both stored as `USD`. Case says nothing about which currency is
meant, and the rule that protects correctness is still strict. After trimming and
upper-casing, the value must be exactly three letters and a real ISO 4217 code
(`java.util.Currency`), so `US`, `USDX`, `XYZ` and `$` are rejected with
`INVALID_CURRENCY`. Rejecting `usd` would throw away a valid transaction from a
provider's export without making the data any safer. If a stricter contract is required,
it is a one-line change in `TransactionValidator`: compare before upper-casing. The
fingerprint is computed from the canonical form, so `usd` and `USD` produce the same
fingerprint.

### Fingerprint

`SHA-256("1" NUL provider NUL transactionId NUL accountId NUL merchantId NUL amount(scale 4) NUL currency NUL timestamp(UTC ms) NUL description)`.
Version `1` is both the first hashed field and a column, so a v2 can coexist and
comparisons are only made within a version.

### Risk score

Five bounded, deterministic components summed and capped at 100 (amount log-scaled up to
35; UTC hour up to 20; description up to 15; merchant hash bucket up to 15; iterated
SHA-256 over the fingerprint up to 15). The last component iterates the hash
`RISK_HASH_ITERATIONS` times and is the deliberately CPU-expensive step. Bands:
0–39 low, 40–69 medium, 70–100 high.

## Database as the correctness model

- **Idempotent creation**: `imports.idempotency_key UNIQUE`. The reservation is an
  `INSERT ... ON CONFLICT DO NOTHING RETURNING`; concurrent requests converge on one row.
  A key whose earlier attempt never stored a complete file may be reused.
- **Duplicate detection**: `transactions (provider_id, transaction_id) UNIQUE`. The batch
  insert uses `ON CONFLICT DO NOTHING RETURNING line_number`; rows not returned are
  duplicates, whether within the batch, the file, an earlier import, another worker or a
  previous process. Same id with different content: first accepted record wins; the later
  one is recorded as `DUPLICATE_TRANSACTION_CONFLICT` (identical content:
  `DUPLICATE_TRANSACTION`). Both count under `duplicates`, not `rejected`.
- **Atomic batches**: transactions, rejections, counters and `last_line_committed` are
  written in one transaction after locking the import row (`FOR UPDATE`) and checking the
  lease. A crash between any two statements loses the whole batch, which is re-read from
  the checkpoint on the next attempt. A batch that already committed (checkpoint ≥ batch
  end) is a no-op, so a retry after a lost commit acknowledgement cannot double count.
- **Job ownership**: `lease_owner` + `lease_expires_at`, renewed by a heartbeat every 10 s
  with a 30 s lease. Claims are `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)`
  and every state transition is a conditional update on owner and status.
- **Indexes**: partial index on claimable statuses (worker scans stay tiny), `(import_id)`
  on transactions (summaries), `(import_id, id)` on rejections (keyset pagination),
  `UNIQUE (import_id, line_number)` on rejections (re-delivery safe).
- **Delivery guarantee**: effectively-once. Kafka is at-least-once; the database makes
  duplicates harmless (claims are conditional, inserts are idempotent, batches are
  checkpointed).

## Failure recovery

| Event | Effect |
|---|---|
| API dies during upload | row stays `uploading`; reaper marks it `failed/UPLOAD_INTERRUPTED` after 15 min; client may retry with the same key |
| Broker unavailable at publish | row is `pending`; API answers 503 `QUEUE_UNAVAILABLE`; the worker's sweep re-publishes pending rows every 60 s |
| Worker dies mid-import | lease expires (30 s); the uncommitted Kafka offset is redelivered after rebalance and the sweep re-publishes expired leases; the next claim resumes from the checkpoint |
| Database connection lost | batch write retried with backoff (5 attempts, 200 ms → 5 s, ±25 % jitter); then the import is released for another attempt (3 attempts total) or failed (see [retry strategy](#retry-strategy)) |
| Same event delivered twice | second claim fails (live lease or terminal status) and is acknowledged |
| Batch committed, progress update "fails" | impossible by construction: they are the same transaction |
| Cancel during processing | `cancel_requested` observed between batches; in-flight batches commit; status `cancelled`; nothing after the checkpoint is written |
| SIGTERM | readiness DOWN, consumer paused, HTTP drains, active imports get `SHUTDOWN`, finish in-flight batches, release the job; grace period 30 s, then forced release (see [grace-period expiry](#graceful-shutdown-and-grace-period-expiry)); next worker resumes |

Cancellation vs shutdown: cancel ends the import permanently (`cancelled`, partial
results kept and queryable); shutdown releases it (`pending`) so it continues elsewhere.

## Retry strategy

Retries happen at three levels, each one for a different failure lifetime. A lower level
gives up before the level above it takes over.

| Level | Where | Handles | Budget | Then |
|---|---|---|---|---|
| 1. Operation | `ExponentialBackoffRetryPolicy` around each batch write | a failure lasting seconds: dropped connection, deadlock, serialisation failure, pool timeout, Postgres restarting | `RETRY_MAX_ATTEMPTS` = 5 attempts over about 3 s by default | the exception reaches the use case |
| 2. Import attempt | `ProcessImportUseCase.handleFailure` | a failure lasting minutes: the database is down for longer than level 1 waits | `PROCESSING_MAX_ATTEMPTS` = 3 claims of the import | the import is `failed` with `PROCESSING_FAILED` |
| 3. Dispatch | Kafka `nack(KAFKA_REDELIVERY_DELAY)` and the worker's 60 s sweep | lost events, dead workers, a broker outage at publish time | unbounded (it only re-publishes; level 2 caps the work) | — |

**Backoff.** `delay(n) = min(RETRY_MAX_BACKOFF, RETRY_INITIAL_BACKOFF × 2^(n-1)) × U[0.75, 1.25)`.
With the defaults the delays are 200, 400, 800 and 1600 ms (±25 %), a little over 3 s
in total. The jitter is there so that when one database hiccup hits several workers,
their retries spread out instead of landing together.

**What is transient** (`SqlStateFailureClassifier`, which walks the cause chain):
SQLSTATE class `08` (connection), `40001` (serialisation failure), `40P01` (deadlock),
`55P03` (lock not available), `57P01`–`57P03` (admin or crash shutdown, cannot connect
now), `57014` (statement timeout), class `53` (insufficient resources), Spring's
transient and recoverable `DataAccessException` types, `CannotGetJdbcConnectionException`,
`TransactionSystemException`, I/O errors, and `AppException`s marked retryable
(`STORAGE_FAILURE`, `DATABASE_UNAVAILABLE`).

**What is never retried:** constraint violations (class `23`), validation and business
errors, `LeaseLostException` (another worker owns the import, so retrying would fight
it), cancellation, interruption and programming errors. These fail on the first try,
because retrying cannot succeed and only hides the real failure.

**Why a retried batch is safe.** The batch transaction re-checks the lease and the
checkpoint under `SELECT … FOR UPDATE`. If the commit went through but its
acknowledgement was lost, the retry sees `last_line_committed ≥ batch end` and does
nothing (`alreadyCommitted = true`). If the commit did not happen, nothing from the batch
is visible and the retry writes it all. Either way each record is counted once.

**Cancellation and shutdown stop retries.** The policy checks the cancellation token
before each sleep. A worker that is shutting down does not wait out the backoff; it
releases the import so another worker can pick it up.

**Signals.** `retries_total{operation}` counts retry attempts and
`retries_exhausted_total{operation}` counts sequences that ran out. Each retry is logged
at WARN with `importId`, `batch` and `attempt`. A level-2 release is logged as
`Transient failure on attempt n/3`.

**Not retried by the server: uploads.** A failed upload is the client's to retry. The
`Idempotency-Key` makes that safe: a key whose upload never stored a complete file
(interrupted, too large, wrong type, empty) is re-opened, and a key whose file was
stored returns the original import.

## Graceful shutdown and grace-period expiry

On SIGTERM, in order:

1. `ContextClosedEvent` triggers `ImportWorker.signalShutdown()`. Readiness goes DOWN,
   the Kafka container is paused so no new records are fetched, and every active import's
   token is set to `SHUTDOWN`.
2. Each pipeline stops reading at its next line. Batches already handed to the writer
   (at most `PROCESSING_MAX_IN_FLIGHT_BATCHES`) commit normally. The import is then
   **released** (`processing → pending`, lease cleared) and its Kafka offset is left
   uncommitted.
3. `ImportWorker.stop()` waits up to `SHUTDOWN_GRACE_PERIOD` (30 s) for the active count
   to reach zero. Normally this takes one batch time, well under a second.

**If the grace period expires** with imports still active (for example, a batch stuck in
level-1 retries against a database that is down), the worker **force-releases** every
remaining import with the same conditional `release` (owner = this worker, status =
`processing`), stops the heartbeat and exits. Anything still running on that JVM is then
harmless:

- a batch transaction that already holds the import row lock finishes first, because
  `release` waits for the lock. The batch and its checkpoint are therefore either
  committed together or not at all;
- a batch that starts after the release fails the lease check under the row lock, so it
  throws `LeaseLostException` and writes nothing;
- the uncommitted offset is redelivered to another consumer after the rebalance, and the
  sweep re-publishes `pending` rows within `PROCESSING_REDISPATCH_INTERVAL` (60 s) if not.
  The next claim resumes from `last_line_committed`.

An import in `cancelling` is not released, since `release` only applies to `processing`.
Its lease runs out (30 s) and the next worker to claim it marks it `cancelled`.

If the container runtime kills the process before step 3 finishes (`stop_grace_period`
is shorter than the HTTP drain plus `SHUTDOWN_GRACE_PERIOD`), nothing is released and
recovery takes the crash path: the lease expires after 30 s and the import is
re-dispatched. The compose files set `stop_grace_period` to 45–60 s, above the 30 s
grace period, so the forced release normally gets to run.

## Summaries and the aggregation trade-off

The summary (totals, by currency, by risk level, top merchants and accounts) can be
produced three ways. The service uses the second and third (ADR-005):

| Approach | Write cost | Read cost | Consistency | Chosen |
|---|---|---|---|---|
| Incremental aggregates updated in every batch transaction | one upsert per distinct currency, risk level, merchant and account per batch, on rows every batch contends for | one small read | exact, but the hot path pays for it | no |
| Derived on read: `GROUP BY` over `transactions WHERE import_id = ?` | none | a scan of the import's rows through the `(import_id)` index | exact: it reads what has been committed | while the import runs |
| Materialised once: computed at the first read after the import is terminal, stored in `import_summaries` (JSONB) | one row, once | one primary-key read | exact; the import can no longer change | once terminal |

On top of that, Redis caches the summary for 2 s while the import is live and 1 h once
it is final. A cached live summary is only used if its totals equal the import's current
`processed` counter, so polling never shows progress going backwards.

**Why derive instead of maintaining incrementally.** Ingest throughput is the constraint
that matters. Incremental aggregates would add up to (currencies + 3 + merchants +
accounts) upserts to every batch transaction, all contending on the same aggregate rows,
and would need their own idempotency so a retried batch is not counted twice. Deriving
from `transactions` is correct by construction, because the summary and the progress
counters come from the same committed rows.

**What it costs.** A live summary of a large running import is an index scan plus four
aggregations over that import's committed rows, so its cost grows with the import. The 2 s cache caps it at one
scan per import every 2 s no matter how many clients poll, and status polling
(`GET /v1/imports/{id}`) reads the counters on the import row instead, so it never pays
for a scan. Once the import is terminal, a read costs one row lookup.

**Bounded output.** Merchants and accounts are capped at the top 100 by count, so the
response size and the sort stay bounded for a file with 500k distinct accounts. A full
breakdown would need its own paginated endpoint.

**When to revisit.** If dashboards need live summaries of many large imports at once,
the next step is a per-batch partial aggregate written into the batch transaction (one
row per batch, idempotent through the batch number) and summed on read. That is cheaper
than a per-group upsert and keeps the same once-only guarantee.

## Back-pressure and bounded concurrency

| Resource | Bound | What happens at the limit |
|---|---|---|
| concurrent uploads | `UPLOAD_MAX_CONCURRENT` (4) | 429 immediately |
| imports per worker | `PROCESSING_MAX_CONCURRENT_IMPORTS` (2) consumer threads | events wait in Kafka; `import_queue_depth` grows |
| total imports in flight | Kafka partitions (8) | more consumers than partitions idle |
| batches in memory per import | `PROCESSING_MAX_IN_FLIGHT_BATCHES` (2) × batch size (1000) | the reader blocks on a semaphore |
| scoring | `cores` threads, queue `RISK_QUEUE_CAPACITY` (4000 chunks) | submitting thread blocks |
| database connections | `DB_POOL_SIZE` (10) | callers wait up to 5 s, then a transient error → retry |
| HTTP | Tomcat max threads 200, virtual threads for handlers | connections queue at the accept level |
| line length / file size | 16 KiB / 200 MB | rejected without buffering |

Nothing creates a task per record: the only futures are the per-batch scoring futures,
already bounded by the in-flight permit.

## Observability

Structured logs (JSON with the `json` profile) carry `requestId`, `importId`, `workerId`,
`batch`, `attempt`, `errorCode`, `shutdown`. Uploaded content and descriptions are never
logged. Metrics are listed in the README with the thresholds that indicate a problem.
Health: `/health/live` (process only) and `/health/ready` (database, storage, Kafka,
worker state); Redis is reported but does not gate readiness because the caches fail soft.

## Security

Size and line limits, content sniffing instead of trusting MIME type or filename,
sanitised filenames stored only as metadata, server-generated storage keys (no path
input), strict JSON parsing, control-character rejection, bounded raw excerpts in
rejections, parameterised SQL via jOOQ, request ids instead of stack traces in
responses, low-cardinality metric tags. Authentication would be added as a servlet
filter that resolves the principal to a provider id and passes it to the same use cases.
