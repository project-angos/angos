---
displayed_sidebar: howto
sidebar_position: 12
title: "Enable Durable Cache Jobs"
---

# Enable Durable Cache Jobs

Pull-through cache-fill tasks always go through the engine-backed job queue.
By default (when `[global.job_queue]` is absent) jobs are stored in-process
memory: a client request enqueues a cache-fill job, an in-process worker drains
it immediately, and the queue is discarded on restart. Deduplication within a
single process is guaranteed, but the queue cannot be observed externally.

Configuring `[global.job_queue]` switches the underlying storage from in-memory
to a durable backend (filesystem or S3). Jobs survive restarts, can be shared
across replicas, and are drained by one or more `angos worker` subcommands that
you run alongside `angos server`. The code path is identical in both modes; only
the storage backing changes.

## When should I use this?

Enable durable cache jobs when:

- You run multiple `angos server` replicas behind a load balancer and want
  cross-replica deduplication: only one worker pulls each distinct blob from
  upstream, regardless of how many replicas saw the miss.
- You want KEDA or another external autoscaler to scale `angos worker` pods
  based on queue depth (the `angos_job_queue_pending` Prometheus gauge served
  by `/metrics` on the server's listener).
- You want cache-fill work to survive a server restart rather than being
  discarded.

For a single-node deployment where in-process caching is sufficient there is no
need to configure `[global.job_queue]`; jobs run in-process at full speed
without any filesystem or network I/O for the queue layer.

## Configuration

Add a `[global.job_queue]` section to `config.toml`:

Pick exactly one backend sub-table — `[global.job_queue.fs]` or
`[global.job_queue.s3]`.

### Filesystem backend

Use this for a single-host deployment or when servers and workers share a
filesystem volume. Multi-process pools (multiple `angos server` or
`angos worker` replicas sharing one `root_dir`) additionally need
`lock_strategy.redis` — see below.

```toml
[global]
max_concurrent_cache_jobs = 4   # also bounds the number of jobs each `angos worker`
                                # processes in parallel

[global.job_queue]
pending_refresh_interval_secs = 15   # how often the server refreshes the pending gauge
pending_ready_horizon_secs = 600     # only jobs ready within this many seconds count toward the gauge

[global.job_queue.fs]
root_dir = "/var/lib/angos/jobs"     # must be writable by every server and worker replica
# lock_strategy defaults to "memory" (single-process scope only). For
# multi-process pools sharing root_dir, replace it with the Redis sub-table.
# The `ttl` here governs how long another worker has to wait if the
# current holder dies mid-job, and is the only TTL the consumer relies on.
#
# [global.job_queue.fs.lock_strategy.redis]
# url = "redis://localhost:6379"
# ttl = 30
```

> **Note:** Per-`lock_key` coordination is delegated to the configured
> `lock_strategy`, not to any filesystem-level primitive. The default
> `"memory"` strategy only coordinates within a single process — a second
> `angos server` or `angos worker` pointed at the same `root_dir` will race.
> Multi-process pools must configure `lock_strategy.redis`. The S3 lock
> strategy used by the S3 metadata store is rejected for the FS job store.

### S3 backend

Use this for multi-node deployments or when blob storage is already on S3.

```toml
[global.job_queue]
pending_refresh_interval_secs = 15   # keep at 15 or higher on S3 to avoid
                                     # excessive LIST requests
pending_ready_horizon_secs = 600     # autoscaler gauge horizon

[global.job_queue.s3]
bucket = "angos-jobs"
key_prefix = "_jobs"
endpoint = "https://<s3-compatible-endpoint>"
region = "us-east-1"
access_key_id = "<key-id>"
secret_key = "<secret-key>"
# The default `lock_strategy = "s3"` uses S3 conditional writes for the
# per-`lock_key` execution lock with `ttl_secs = 30`. Override the lock TTL
# (and other parameters) under `[global.job_queue.s3.lock_strategy.s3]`.
```

## Running the worker

A durable-queue deployment needs both subcommands:

- `angos server` accepts client requests, enqueues cache-fill jobs on miss,
  and publishes the queue-depth gauge on `/metrics`. It does **not** process
  jobs itself.
- `angos worker` polls the queue, fetches blobs from upstream, and writes them
  into the shared blob/metadata store. Each worker processes up to
  `max_concurrent_cache_jobs` jobs in parallel; multiple workers safely share
  the queue thanks to a per-`lock_key` execution lock held on the configured
  `lock_strategy`. Run at least one.

Both subcommands hot-reload `config.toml` on disk: changes to
`[global.job_queue]`, `[repository.*]`, `[blob_store.*]`, or
`[metadata_store.*]` take effect at the next iteration; in-flight jobs always
finish on the components they started with.

### Worker subcommand options

| Flag                         | Default | Description                                              |
|------------------------------|---------|----------------------------------------------------------|
| `--poll-interval <duration>` | `1s`    | Minimum wait between claim attempts when no ready job is found. If the queue contains only backed-off envelopes, the worker extends the wait up to the soonest `not_before` (capped at 1 minute, or `--poll-interval` if it is larger) to avoid polling-storm cost. |

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

**FS backend on shared storage:** Worker coordination is provided by the
configured `lock_strategy`, not by the filesystem. A shared volume only needs
to be writable by every replica and to support atomic rename within a
directory. Multi-process pools require `lock_strategy.redis`; the default 
`lock_strategy = "memory"` does not coordinate across processes even
on a shared mount. If you do not want to run Redis, use the S3 backend
instead.

**S3 backend requirements:** Under the default `lock_strategy = "s3"` the
per-`lock_key` execution lock is held on an S3 object backed by conditional
writes — the provider must support `put_if_none_match` and `put_if_match`.
Endpoints that strip `ETag` from PUT responses are not supported; angos
fails fast at startup rather than silently dropping jobs. Lock release uses
`DELETE` with `If-Match: <etag>` on services that support it (the default);
set `delete_if_match = false` on endpoints without conditional delete to
fall back to an unconditional `DELETE`.

**Match S3 capabilities across sections:** When `lock_strategy = "s3"` is
configured in both `[metadata_store.s3]` and `[global.job_queue.s3]`, the two
sections build independent lock backends that do not share configuration even
though they target the same S3-compatible provider. Declare the
`capabilities` block (`put_if_none_match`, `put_if_match`, `delete_if_match`)
identically in `[metadata_store.s3.capabilities]` and
`[global.job_queue.s3.capabilities]`. The metadata-store auto-detects via a
startup probe when the block is omitted; the job-queue does not — it
defaults to all-false and rejects `lock_strategy = "s3"` unless you opt in
explicitly. A divergent declaration across the two sections is almost
always a misconfiguration.

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

**Upgrade cleanup:** Earlier Angos releases wrote a per-job lease body file
under `{root_dir}/_jobs/leases/` (FS) or `{key_prefix}/_jobs/leases/` (S3) for
every active job. This release does not write or read those files; the
per-`lock_key` execution lock now lives at whatever path the configured
`lock_strategy` uses. Any files left at the old `_jobs/leases/` prefix after
upgrade are inert and can be deleted (`rm -rf {root_dir}/_jobs/leases/` on FS,
or a bulk delete of the `_jobs/leases/` prefix on S3) to reclaim storage.

**Backoff schedule:** Failed jobs are retried with exponential backoff:
`min(1 min × 2^attempts, 10 min)`. With the default 5-attempt budget a job
retries 4 times with delays of 2, 4, 8 and 10 minutes (24 minutes total)
before being moved to the dead-letter queue.

**Transactional engine path:** All writes — enqueue, complete, retry, and
dead-letter — are routed through the transactional engine regardless of the
chosen backend. On `complete`, the handler's work-product mutations (for
`cache.fetch_blob`: `Move` of staged bytes to the canonical blob path,
`Delete` of the upload-session record, and the per-namespace blob-index
grant) are merged with the pending and dedup-index deletes into one engine
transaction. The worker releases the per-`lock_key` execution lock right
after that transaction settles, so the work commit and the queue cleanup
land atomically and the next worker can claim the same `lock_key` without
waiting on TTL. The on-disk layout under `job-pending/`, `job-failed/`, and
`job-lock-index/` is identical for both the filesystem and S3 backends.
