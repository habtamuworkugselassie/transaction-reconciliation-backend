# Benchmark

## What is measured and how

`scripts/benchmark.sh <records>` generates a deterministic NDJSON file (2 % invalid
records, 0.5 % duplicates), uploads it, and until the import is terminal:

- polls `GET /v1/imports/{id}` every second to compute average and peak throughput from
  the persisted `processed` counter (so the number reflects committed work, not parsing);
- probes `GET /health/ready` and `GET /v1/imports/{id}` every 250 ms and records their
  latency: this is API responsiveness while the worker is saturating the CPU and the
  database;
- scrapes `/metrics` every second for peak heap, process CPU, scorer pool occupancy and
  queue depth, Hikari active connections and active imports.

It writes `benchmark-results/<timestamp>.md`. `scripts/load-test.js` (k6) adds a
multi-user polling load with p95/p99 thresholds on the same endpoints.

Processing duration is `completedAt - startedAt` of the import, which excludes the upload.

## Results

Environment for the runs below: this development container, 4 vCPU, 15 GB RAM, local
PostgreSQL 16 on the same host, API and worker in one JVM (`./mvnw spring-boot:run`),
default configuration (batch 1000, 2 in-flight batches, 2 concurrent imports, scoring
pool = 4 threads, 200 hash iterations per record, Hikari 10).

| Dataset | Lines | Size | Processing | Avg throughput | Notes |
|---|---|---|---|---|---|
| 20,000 | 20,000 | 4.4 MB | 2.5 s | ~8,000 rec/s | 19,468 accepted, 383 rejected, 112 duplicates; counts match the generator exactly |
| 200,000 | 200,000 | 42.5 MB | 21.2 s (across a SIGTERM and restart) | ~9,400 rec/s | released at line 29,052 on SIGTERM, resumed by the restarted worker from that checkpoint; final 195,081 accepted / 3,617 rejected / 973 duplicates, 195,081 distinct ids, zero double counting |

API latency during the 200k import: `GET /v1/imports/{id}` answered in single-digit
milliseconds throughout (the status read is one indexed row).

### 500,000 records (full probe set)

Environment: Apple M1 Pro (10 cores, 32 GB), Docker Desktop VM with 10 CPUs and 7.75 GB,
the `docker compose` stack from this repository (separate API and worker containers,
Postgres 16, Kafka, Redis, all on the same VM), default configuration (batch 1000, 2
in-flight batches, scoring pool = 10 threads, 200 hash iterations). Command:
`scripts/benchmark.sh 500000`.

| Dataset | Lines | Size | Upload | Processing | Avg / peak throughput | Notes |
|---|---|---|---|---|---|---|
| 500,000 | 500,000 | 108.1 MB | 2.0 s | 17.5 s | ~28,500 / ~36,700 rec/s | 487,628 accepted, 9,096 rejected, 2,506 duplicates, 770 blank lines skipped; counts match the generator exactly |

| During the 500k import | p50 | p95 | p99 | max |
|---|---|---|---|---|
| `GET /v1/imports/{id}` (ms) | 5 | 10 | 13 | 13 |
| `GET /health/ready` (ms) | 13 | 20 | 97 | 97 |

Peak resource use: JVM heap 76 MB (on a 108 MB file, so memory does not grow with the
file), worker process CPU 39 %, scorer threads 7 of 10 busy, scorer queue depth 0, one
Hikari connection active out of 8. Process RSS shows "n/a" in this run because the
`process_memory_rss_bytes` gauge was added afterwards. It is now exported on Linux (every
container), and `scripts/benchmark.sh` reports its peak on the next run.

**What changed between environments.** On this 10-core machine the scoring pool is
*not* saturated: the queue stays at 0 and CPU peaks at 39 %. The limit is the single
sequential writer per import (one connection busy, batches committing back to back).
On the 4-vCPU environment above, CPU was the limit. Both match the prediction in the
interpretation below: CPU-bound on few cores, writer-bound once there are enough cores,
which is why parallel batch writers per import are the next improvement.
`benchmark-results/run-2026-11-05T13-14-27.866770Z.md` is the raw output.

## Interpretation

- **Bottleneck**: CPU in the scoring pool. With 4 threads and 200 SHA-256 iterations per
  record, the pool runs at 100 % while the database writer idles between batches
  (`risk_scorer_queue_depth` > 0, `import_batch_duration` p95 ≈ 60 ms for 1000 rows). Halving
  `RISK_HASH_ITERATIONS` roughly doubles throughput, which confirms it. The second bottleneck
  is the single writer per import: batches commit sequentially, so per-import throughput
  is bounded by `batchSize / batchDuration` even with idle CPU.
- **Memory**: bounded by design. Per import at most 2 batches × 1000 records are
  materialised plus the 64 KiB read chunk and one line buffer (≤ 16 KiB); heap use does not
  grow with file size. The 200 MB upload limit is enforced while streaming.
- **Responsiveness**: request handling runs on virtual threads and never touches the
  scoring pool; the API-only role in the compose files removes even scheduler contention.
- **Did it meet expectations?** Yes for the assignment's 500,000-record target. The
  measured 500k run completed in 17.5 s on 10 cores with status p99 of 13 ms and 76 MB
  peak heap. At the ~8–9k records/s measured on 4 cores, the same file would take about a
  minute.
- **What would be improved next**: parallel batch writers per import (ordering is not
  required; the checkpoint would become a per-range bitmap or a "lowest contiguous
  committed line" watermark), `COPY`-based inserts for the accepted rows, and moving the
  scoring pool to a separate process so CPU limits can be set per role.
