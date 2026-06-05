---
displayed_sidebar: howto
sidebar_position: 12
title: "Enable Durable Cache Jobs"
---

# Enable Durable Cache Jobs

Pull-through cache-fill tasks always go through the engine-backed job queue, and
that queue is **persistent in both modes**: jobs are written to the configured
object store under a hardcoded `_jobs/` prefix and survive a restart. What
`[global.job_queue]` changes is *who drains the queue* and how it scales — not
whether jobs are durable.

By default (when `[global.job_queue]` is absent) the `angos server` process
drains the queue itself, in-process: a client request enqueues a cache-fill job
and an in-process claim loop runs it. Pending jobs persist to the store and are
picked back up after a restart, but there is no cross-replica coordination and
no externally observable queue-depth gauge.

Adding `[global.job_queue]` switches draining to one or more separate `angos
worker` processes that you run alongside `angos server`, and turns on the
queue-depth gauge for autoscaling. Durable jobs are stored in the **same backend
you configured for `[metadata_store]`** (filesystem or S3), under the same
`_jobs/` prefix. The code path is identical in both modes; only who drains the
queue and how those drainers coordinate changes.

## When should I use this?

Enable durable cache jobs when:

- You run multiple `angos server` replicas behind a load balancer and want
  cross-replica deduplication: only one worker pulls each distinct blob from
  upstream, regardless of how many replicas saw the miss.
- You want KEDA or another external autoscaler to scale `angos worker` pods
  based on queue depth (the `angos_job_queue_pending` Prometheus gauge served
  by `/metrics` on the server's listener).
- You want cache-fill work drained by dedicated `angos worker` processes,
  decoupled from — and scaled independently of — the request-serving
  `angos server` processes.

For a single-node deployment, in-process draining is sufficient and you do not
need `[global.job_queue]`; jobs still persist under `_jobs/` (so they survive a
restart), but the server drains them itself rather than a separate worker.

## Configuration

The queue has no storage backend of its own. Durable jobs are written to the
**same backend you already configured for `[metadata_store]`** — filesystem or
S3 — under a hardcoded top-level `_jobs/` prefix, and the per-`lock_key`
execution lock uses the lock strategy inherited from `[metadata_store]`. There
is no `[global.job_queue.fs]` or `[global.job_queue.s3]` sub-table: enabling the
queue is just a matter of adding `[global.job_queue]`, which accepts only the
two tunables below.

```toml
[global]
max_concurrent_cache_jobs = 4   # also bounds the number of jobs each `angos worker`
                                # processes in parallel

[global.job_queue]
pending_refresh_interval_secs = 15   # how often the server refreshes the pending gauge (minimum 5)
pending_ready_horizon_secs = 600     # only jobs ready within this many seconds count toward the gauge
```

> **Note:** Because storage is inherited from `[metadata_store]`, multi-process
> pools (multiple `angos server` or `angos worker` replicas) need a
> multi-process-safe lock strategy on the metadata store — `lock_strategy.redis`
> for the filesystem backend, or the default `lock_strategy = "s3"` for the S3
> backend. The default `"memory"` lock strategy only coordinates workers within
> a single process, so a second replica would race. See
> [the configuration reference](../reference/configuration.md) for the
> `[metadata_store]` lock options.

## Running the worker

A durable-queue deployment needs both subcommands:

- `angos server` accepts client requests, enqueues cache-fill jobs on miss,
  and publishes the queue-depth gauge on `/metrics`. It does **not** process
  jobs itself.
- `angos worker` polls the queue, fetches blobs from upstream, and writes them
  into the shared blob/metadata store. Each worker processes up to
  `max_concurrent_cache_jobs` jobs in parallel; multiple workers safely share
  the queue thanks to a per-`lock_key` execution lock held on the lock strategy
  inherited from `[metadata_store]`. Run at least one.

Both subcommands hot-reload `config.toml` on disk: changes to
`[global.job_queue]`, `[repository.*]`, `[blob_store.*]`, or
`[metadata_store.*]` take effect at the next iteration; in-flight jobs always
finish on the components they started with.

### Worker subcommand options

| Flag                         | Default   | Description                                            |
|------------------------------|-----------|--------------------------------------------------------|
| `--queue <name>`             | `cache` and `replication` | Queue to drain. With no `--queue` the worker drains both the `cache` (pull-through cache-fill) and `replication` queues, each on its own pool. Repeatable (`--queue cache --queue replication`) to scale or isolate queues independently. |
| `--poll-interval <duration>` | `1s`      | Minimum wait between claim attempts when no ready job is found. If the queue contains only backed-off envelopes, the worker extends the wait up to the soonest `not_before` (capped at 1 minute, or `--poll-interval` if it is larger) to avoid polling-storm cost. |

### Example: server + worker pods

```bash
# Pod 1+: HTTP listener, enqueues jobs, publishes queue-depth gauge.
angos -c config.toml server

# Pod 2+: drains the queue. No HTTP listener.
angos -c config.toml worker
```

## Metrics

`angos server` exposes Prometheus metrics on its main listener at `GET /metrics`
(same address as `/healthz` and `/readyz`). When `[global.job_queue]` is
configured the server publishes:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `angos_job_queue_pending` | Gauge | `queue` | Pending jobs ready within the configured readiness horizon (`pending_ready_horizon_secs`, default 600 s). Refreshed by a background ticker; use this for KEDA autoscaling. Saturates at 10 000 (read as "≥ 10 000") to cap S3 `LIST` cost per refresh. |
| `angos_job_queue_enqueued_total` | Counter | `queue`, `dedup` | Jobs submitted. `dedup="hit"` means a duplicate `lock_key` was suppressed. |

`angos worker` has no HTTP listener and therefore exposes no metrics of its
own; per-execution diagnostics (claim, success, retry, dead-letter, lock-lost)
are emitted via structured logs and keyed on `lock_key`.

## Operational notes

**Dead-letter queue:** Jobs that exhaust their retry budget (5 attempts) are
moved to `_jobs/failed/cache/<storage_key>.json` (FS) or the equivalent S3
key. The `storage_key` is `<16-hex unix-millis>-<uuid>` — the millis prefix is
the `not_before` of the last retry, the UUID is the envelope id. Inspect with
`cat`/`jq` to diagnose persistent failures.

To requeue manually, move the file back into `_jobs/pending/cache/`. The
filename's millis prefix continues to drive scheduling, so to force immediate
re-execution rename the file with a zero prefix:
`0000000000000000-<uuid>.json`. A worker will pick it up on the next poll
(envelope `attempts` and `max_attempts` are preserved as-is, so a job that
already hit the retry ceiling will still go straight to DLQ on first failure
unless you also edit the body).

**Filesystem metadata store on shared storage:** When `[metadata_store]` uses
the filesystem backend, worker coordination is provided by its configured
`lock_strategy`, not by the filesystem. A shared volume only needs to be
writable by every replica and to support atomic rename within a directory.
Multi-process pools require `[metadata_store.fs.lock_strategy.redis]`; the
default `lock_strategy = "memory"` does not coordinate across processes even on
a shared mount. If you do not want to run Redis, use the S3 metadata backend
instead.

**S3 metadata store requirements:** When `[metadata_store]` uses the S3 backend
with the default `lock_strategy = "s3"`, the per-`lock_key` execution lock is
held on an S3 object backed by conditional writes — the provider must support
`put_if_none_match` and `put_if_match`. Endpoints that strip `ETag` from PUT
responses are not supported; angos fails fast at startup rather than silently
dropping jobs. Lock release uses `DELETE` with `If-Match: <etag>` on services
that support it (the default); set `delete_if_match = false` under
`[metadata_store.s3]` on endpoints without conditional delete to fall back to
an unconditional `DELETE`.

**S3 LIST cost:** Each enqueue scans `_jobs/pending/cache/` for duplicate
`lock_key`s. At the default `pending_refresh_interval_secs = 15` and with N
serve replicas each doing their own scan, total LIST rate is roughly `miss_rate
× N` calls/s. A `ListObjectsV2` returns up to 1000 keys per call, so queues
with thousands of pending jobs remain cheap. The pending-gauge ticker stops
paginating as soon as it crosses either threshold: the readiness horizon
(first key whose storage-key prefix is past `now + pending_ready_horizon_secs`)
or the 10 000-entry saturation cap. Both bound the per-tick cost regardless
of queue depth. `pending_refresh_interval_secs` is enforced to be ≥ 5 at
config load (sub-5s ticks induce LIST storms on S3).

**Backoff schedule:** Failed jobs are retried with exponential backoff:
`min(1 min × 2^attempts, 10 min)`. With the default 5-attempt budget a job
retries 4 times with delays of 2, 4, 8 and 10 minutes (24 minutes total)
before being moved to the dead-letter queue.

**Transactional engine path:** All writes — enqueue, complete, retry, and
dead-letter — are routed through the transactional engine regardless of the
metadata-store backend. On `complete`, the handler's work-product mutations (for
`cache.fetch_blob`: `Move` of staged bytes to the canonical blob path,
`Delete` of the upload-session record, and the per-namespace blob-index
grant) are merged with the pending and dedup-index deletes into one engine
transaction. The worker releases the per-`lock_key` execution lock right
after that transaction settles, so the work commit and the queue cleanup
land atomically and the next worker can claim the same `lock_key` without
waiting on TTL. The on-disk layout under `_jobs/pending/`, `_jobs/failed/`, and
`_jobs/index/` is identical for both the filesystem and S3 backends.
