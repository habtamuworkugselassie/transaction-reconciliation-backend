# ADR-008: Progress streaming polls persisted state

**Status:** accepted

## Context

Clients want live progress without polling `GET /v1/imports/{id}` themselves. The
progress is produced by a worker process. The client is connected to an API process,
possibly behind a load balancer, and possibly to a different replica than the one that
accepted the upload.

## Decision

`GET /v1/imports/{id}/events` is a server-sent-events stream served by the API, which
re-reads the import row every second. One read per watched import per tick is shared by
all of its streams. An event is sent only when the status or a counter changed. Streams
are capped per instance, time out, and close at shutdown.

Rejected: the worker pushes progress through Kafka or Redis pub/sub to whichever API
replica holds the stream.

## Consequences

- The stream is exactly as true as the status endpoint: it reports committed batches only,
  never progress that a rollback could take back.
- No new infrastructure, and any API replica can serve any stream.
- Database load is bounded by the number of distinct imports being watched (one
  primary-key read each per second), not by the number of clients.
- Latency is up to one poll interval behind the commit. That is fine for a progress bar
  over a job that takes seconds to minutes.
- SSE rather than WebSockets: one-directional, plain HTTP through proxies, and browsers
  reconnect `EventSource` automatically.
