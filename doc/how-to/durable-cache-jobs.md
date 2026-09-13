---
displayed_sidebar: howto
sidebar_position: 12
title: "Enable Durable Cache Jobs"
---

# Enable Durable Cache Jobs

Pull-through cache-fill tasks always go through the durable job queue, and
that queue is **persistent in both modes**: jobs are written to the **same
backend you configured for `[metadata_store]`** (filesystem or S3), under a
hardcoded `_jobs/` prefix, and survive a restart. The code path is identical in
both modes. What `[global.job_queue]` changes is *who drains the queue* and how
those drainers coordinate and scale, not whether jobs are durable.

By default (when `[global.job_queue]` is absent) the `angos server` process
drains the queue itself, in-process: a client request enqueues a cache-fill job
and an in-process claim loop runs it. Pending jobs persist to the store and are
picked back up after a restart, but there is no cross-replica coordination and
no externally observable queue-depth gauge.

Adding `[global.job_queue]` switches draining to one or more separate `angos
worker` processes that you run alongside `angos server`, and turns on the
queue-depth gauge for autoscaling.

## When should I use this?

Enable durable cache jobs when:

- You run multiple `angos server` replicas behind a load balancer and want
  cross-replica deduplication: only one worker pulls each distinct blob from
  upstream, regardless of how many replicas saw the miss.
- You want KEDA or another external autoscaler to scale `angos worker` pods
  based on queue depth (the `angos_job_queue_pending` Prometheus gauge served
  by `/metrics` on the server's listener).
- You want cache-fill work drained by dedicated `angos worker` processes,
  decoupled from, and scaled independently of, the request-serving
  `angos server` processes.

For a single-node deployment, in-process draining is sufficient and you do not
need `[global.job_queue]`; jobs still persist under `_jobs/` (so they survive a
restart), but the server drains them itself rather than a separate worker.

## Configuration

The queue has no storage backend of its own. Durable jobs are written to the
**same backend you already configured for `[metadata_store]`** (filesystem or
S3) under a hardcoded top-level `_jobs/` prefix, and workers serialise on
per-`lock_key` claim keys created atomically on that backend. There is no
`[global.job_queue.fs]` or `[global.job_queue.s3]` sub-table: enabling the
queue is just a matter of adding `[global.job_queue]`, which accepts only a
few tunables (the two shown below, plus the retry and claim-lease settings in
the [configuration reference](../reference/configuration.md#durable-job-queue-globaljob_queue)).

```toml
[global]
max_concurrent_cache_jobs = 4   # also bounds the number of jobs each `angos worker`
                                # processes in parallel

[global.job_queue]
pending_refresh_interval_secs = 15   # how often the server refreshes the pending gauge (minimum 5)
pending_ready_horizon_secs = 600     # only jobs ready within this many seconds count toward the gauge
```

> **Note:** Because storage is inherited from `[metadata_store]`, the durable
> queue prefers the backend's atomic create-if-absent to be honest (`link(2)`
> on FS, `If-None-Match: *` on S3). A startup probe creates a scratch claim
> key twice; when the second create succeeds the queue degrades to advisory
> claims with a logged warning, where a claim race may run an idempotent job
> more than once. Correctness is unaffected.

## Running the worker

A durable-queue deployment needs both subcommands:

- `angos server` accepts client requests, enqueues cache-fill jobs on miss,
  and publishes the queue-depth gauge on `/metrics`. It does **not** process
  jobs itself.
- `angos worker` polls the queue, fetches blobs from upstream, and writes them
  into the shared blob/metadata store. Each worker processes up to
  `max_concurrent_cache_jobs` jobs in parallel; multiple workers safely share
  the queue thanks to leased per-`lock_key` claim keys on the
  `[metadata_store]` backend. Run at least one.

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
| `angos_job_queue_failed` | Gauge | `queue` | Dead-lettered jobs currently under `_jobs/failed/<queue>/`. Refreshed by the same ticker and saturates at the same 10 000 cap; keeps dead-letters observable even though `angos worker` has no metrics endpoint. |
| `angos_job_queue_enqueued_total` | Counter | `queue`, `dedup` | Jobs submitted. `dedup="hit"` means a duplicate `lock_key` was suppressed. |

`angos worker` has no HTTP listener and therefore exposes no metrics of its
own; per-execution diagnostics (claim, success, retry, dead-letter, lock-lost)
are emitted via structured logs and keyed on `lock_key`.

## Operational notes

**Dead-letter queue:** Jobs that exhaust their retry budget (5 attempts) are
moved to `_jobs/failed/<queue>/<storage_key>.json` (FS) or the equivalent S3
key: `_jobs/failed/cache/` for cache-fill jobs, `_jobs/failed/replication/`
for replication jobs. The `storage_key` is `<16-hex unix-millis>-<uuid>`: the
millis prefix is the `not_before` of the last retry, the UUID is the envelope
id. Inspect with `cat`/`jq` to diagnose persistent failures. The `_jobs` admin
API and UI list, retry, and delete failed jobs per queue, selected with
`?queue=cache` (the default) or `?queue=replication`.

**Orphan jobs after a configuration change:** Removing a repository (or its
upstreams) from the configuration leaves its pending cache jobs to fail and
dead-letter. `angos scrub` deletes those orphans from both the
pending and dead-letter partitions; combine with `--dry-run` to preview.

To requeue manually, move the file back into `_jobs/pending/<queue>/`. The
filename's millis prefix continues to drive scheduling, so to force immediate
re-execution rename the file with a zero prefix:
`0000000000000000-<uuid>.json`. A worker will pick it up on the next poll
(envelope `attempts` and `max_attempts` are preserved as-is, so a job that
already hit the retry ceiling will still go straight to DLQ on first failure
unless you also edit the body).

**Filesystem metadata store on shared storage:** worker coordination rides on
an atomic `link(2)`-based create-if-absent for the claim keys. A shared volume
must be writable by every replica and should enforce that atomic create; NFS
implementations get this wrong often enough that the startup probe verifies
it, degrading to advisory claims with a logged warning otherwise.

**S3 metadata store requirements:** the claim keys are created with
`PutObject` + `If-None-Match: *`, so the provider should support that
conditional write and surface it honestly. The startup probe verifies it;
endpoints that ignore `If-None-Match` degrade to advisory claims with a
logged warning, where a claim race may run an idempotent job more than once.

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
`min(100 ms × 2^attempts, 10 s)`, where `attempts` counts the failures so
far. With the default 5-attempt budget a job retries 4 times with delays of
200 ms, 400 ms, 800 ms and 1.6 s (3 seconds total) before being moved to the
dead-letter queue.

**Write path:** Enqueue, complete, retry, and dead-letter are ordered
idempotent writes with no transaction, on any metadata-store backend. Workers
serialise on leased claim keys under `_jobs/claims/`, one per `lock_key`,
created atomically and refreshed while the job runs; the worker releases the
claim right after the job's writes settle, so the next worker can claim the
same `lock_key` without waiting on the lease TTL (`claim_ttl_secs`, default
60). The on-disk layout under `_jobs/pending/`, `_jobs/failed/`, and
`_jobs/index/` is identical for both the filesystem and S3 backends.
