---
displayed_sidebar: howto
sidebar_position: 12
title: "Enable Durable Cache Jobs"
---

# Enable Durable Cache Jobs

By default, pull-through cache-fill tasks run in-process: when a client requests
a blob that is not yet locally cached, Angos streams it from upstream to the
client and simultaneously spawns an in-process task to write a local copy.
That task lives in memory so it is lost on restart, cannot be observed by an
external autoscaler, and deduplication only works within a single process.

Durable cache jobs replace that in-process task with a record written to
persistent storage (filesystem or S3). Jobs are processed by the `angos worker`
subcommand, which you must run alongside `angos server` for cache-fill work to
actually happen. `angos server` only enqueues jobs and publishes the queue
depth gauge for autoscaling.

## When should I use this?

Enable durable cache jobs when:

- You run multiple `angos server` replicas behind a load balancer and want
  cross-replica deduplication: only one worker pulls each distinct blob from
  upstream, regardless of how many replicas saw the miss.
- You want KEDA or another external autoscaler to scale `angos worker` pods
  based on queue depth (the `angos_job_queue_pending` Prometheus gauge served
  by `/metrics` on the server's listener).
- You want cache-fill work to survive a server restart rather than being
  silently dropped.

For a single-node deployment where in-process caching is working well there is
no need to change anything; the legacy path continues to work unchanged.

## Configuration

Add a `[global.job_queue]` section to `config.toml`:

Pick exactly one backend sub-table — `[global.job_queue.fs]` or
`[global.job_queue.s3]`.

### Filesystem backend

Use this for a single-host deployment or when workers share a POSIX-compatible
volume (e.g. NFSv4 with working `O_EXCL`).

```toml
[global]
max_concurrent_cache_jobs = 4   # also bounds the number of jobs each `angos worker`
                                # processes in parallel

[global.job_queue]
default_lease_ttl_secs = 30          # per-job worker lease TTL in seconds (min 9)
pending_refresh_interval_secs = 15   # how often the server refreshes the pending gauge

[global.job_queue.fs]
root_dir = "/var/lib/angos/jobs"     # must be writable by every server and worker replica
```

> **Note:** The FS backend uses `O_EXCL` for lease atomicity. This works
> correctly on local POSIX filesystems and NFSv4 mounts with functional locking.
> Old NFSv3 mounts with broken `O_EXCL` are not supported; use the S3 backend
> instead.

### S3 backend

Use this for multi-node deployments or when blob storage is already on S3.

```toml
[global.job_queue]
default_lease_ttl_secs = 30          # min 9
pending_refresh_interval_secs = 15   # keep at 15 or higher on S3 to avoid
                                     # excessive LIST requests

[global.job_queue.s3]
bucket = "angos-jobs"
prefix = "_jobs"
endpoint = "https://<s3-compatible-endpoint>"
region = "us-east-1"
access_key_id = "<key-id>"
secret_key = "<secret-key>"
```

## Running the worker

A durable-queue deployment needs both subcommands:

- `angos server` accepts client requests, enqueues cache-fill jobs on miss,
  and publishes the queue-depth gauge on `/metrics`. It does **not** process
  jobs itself.
- `angos worker` polls the queue, fetches blobs from upstream, and writes them
  into the shared blob/metadata store. Each worker processes up to
  `max_concurrent_cache_jobs` jobs in parallel; multiple workers safely share
  the queue thanks to per-`lock_key` leases. Run at least one.

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
| `angos_job_queue_pending` | Gauge | `queue` | Jobs currently pending. Refreshed by a background ticker; use this for KEDA autoscaling. |
| `angos_job_queue_enqueued_total` | Counter | `queue`, `dedup` | Jobs submitted. `dedup="hit"` means a duplicate `lock_key` was suppressed. |

`angos worker` has no HTTP listener and therefore exposes no metrics of its
own; per-execution diagnostics (claim, success, retry, dead-letter,
lease-lost) are emitted via structured logs and keyed on `lock_key`.

## Operational notes

**Dead-letter queue:** Jobs that exhaust their retry budget (5 attempts) are
moved to `_jobs/failed/cache/<id>.json` (FS) or the equivalent S3 key.
Inspect them with `cat`/`jq` to diagnose persistent failures. Requeue
manually by moving the file back to `_jobs/pending/cache/`.

**FS backend on shared storage:** The FS backend requires `O_EXCL` to be atomic
on the volume. Single-host POSIX filesystems (ext4, XFS, tmpfs) and NFSv4 with
working locking are supported. NFSv3 with broken `O_EXCL` is not. If you need
multi-host FS workers, use the S3 backend.

**S3 backend requirements:** Lease ownership is tracked via the `ETag` header
returned by the storage service on `PUT`. S3-compatible endpoints that strip
`ETag` from PUT responses are not supported; lease creation fails loudly with
`S3 PUT response is missing an ETag` rather than silently dropping jobs.
Lease release uses `DELETE` with `If-Match: <etag>` on services that support
it (the default); set `delete_if_match = false` on endpoints without
conditional delete to fall back to an unconditional `DELETE`.

**S3 LIST cost:** Each enqueue scans `_jobs/pending/cache/` for duplicate
`lock_key`s. At the default `pending_refresh_interval_secs = 15` and with N
serve replicas each doing their own scan, total LIST rate is roughly `miss_rate
× N` calls/s. A `ListObjectsV2` returns up to 1000 keys per call, so queues
with thousands of pending jobs remain cheap. Do not set
`pending_refresh_interval_secs` below 5 on S3.

**Backoff schedule:** Failed jobs are retried with exponential backoff:
`min(1 min × 2^attempts, 10 min)`. With the default 5-attempt budget a job
retries 4 times with delays of 2, 4, 8 and 10 minutes (24 minutes total)
before being moved to the dead-letter queue.
