---
displayed_sidebar: howto
sidebar_position: 11
title: "Configure Replication"
---

# Configure Replication

Mirror a repository's content to one or more downstream registries as it changes. Two Angos instances configured as each other's downstreams form an active-active pair. See [Bi-Directional Replication](../explanation/replication.md) for the concepts.

## Prerequisites

- Two or more reachable Angos instances (or any OCI-compliant downstream registry).
- A credential on each downstream that is allowed to push (`put-manifest`, blob uploads). See [Set Up Access Control](set-up-access-control.md).

## Declare a Downstream

Replication is configured per repository, alongside `upstream`. Add one `[[repository."<name>".downstream]]` table per downstream:

```toml
[repository."nginx"]

[[repository."nginx".downstream]]
name = "eu-region"                    # local identifier (appears in logs and metrics)
url = "https://angos-eu.example.com"
username = "replicator"
password = "..."
mode = "event+reconcile"              # "event+reconcile" | "event-only" | "reconcile-only"
namespace_filter = ["^nginx/.*"]      # optional regex list; empty matches all namespaces
max_concurrent_pushes = 4             # optional; per-manifest blob fan-out (default 4)
```

### Downstream Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | string | required | Local identifier for this downstream (used in logs and the `downstream` metric label) |
| `url` | string | required | Downstream registry base URL |
| `mode` | string | `"event+reconcile"` | `"event+reconcile"`, `"event-only"`, or `"reconcile-only"` |
| `namespace_filter` | [string] | `[]` (all) | Regex patterns; a namespace replicates to this downstream only if it matches one |
| `max_concurrent_pushes` | usize | `4` | Concurrent blob pushes per manifest for this downstream |
| `prune` | bool | `false` | When `true`, reconciliation also **deletes** tags present on this downstream but absent locally (authoritative one-way mirror). **Leave `false` for active-active peers** — see [Reconcile on Demand](#reconcile-on-demand). |
| `username` / `password` | string | - | Basic auth for the downstream |
| `max_redirect` | u8 | `5` | Maximum redirects to follow |
| `server_ca_bundle` | string | - | CA bundle to verify the downstream's TLS certificate |
| `client_certificate` / `client_private_key` | string | - | mTLS to the downstream (both required together) |

### Modes

| Mode | Live pushes on mutation | Included in `scrub --replicate` |
|------|-------------------------|---------------------------------|
| `event+reconcile` | Yes | Yes |
| `event-only` | Yes | No |
| `reconcile-only` | No | Yes |

## Global Knobs

One `[global]` field tunes replication across all repositories:

```toml
[global]
max_concurrent_replication_jobs = 4                # worker concurrency for replication jobs (must be > 0)
```

- `max_concurrent_replication_jobs` bounds how many replication jobs each `angos worker` (or the in-process drain) handles in parallel. Default `4`; must be greater than zero.

Restrict who may push inbound replication writes through the CEL `access_policy` (for example, gate `put-manifest` on `identity.id == "replicator"`).

## Worker vs In-Process Drain

How replication work is drained depends on `[global.job_queue]`:

### In-Process (server self-drains)

With **no** `[global.job_queue]` section, the server drains the replication queue itself, in-process. This is the simplest setup -- run only `angos server` on each instance -- and is ideal for a single-instance or demo deployment. Jobs still persist to the configured fs/S3 store (under `_jobs/`) and resume after a restart; what you give up versus a separate worker is cross-replica coordination and the queue-depth gauge, not durability.

### Separate Worker

With `[global.job_queue]` configured, the server only enqueues jobs; you must run a worker to drain them. A bare `angos worker` drains **both** the replication and cache queues (each on its own pool); pass `--queue replication` to drain replication alone, for example to scale it independently:

```bash
angos -c config.toml worker                      # drains both cache and replication
angos -c config.toml worker --queue replication  # replication only
```

This is the multi-replica, horizontally-scalable configuration: draining is decoupled from serving and can be scaled independently. (Pending pushes persist under `_jobs/pending/replication/` and resume after a restart in both modes.) See [Enable Durable Cache Jobs](durable-cache-jobs.md) for the job-queue setup, KEDA autoscaling, and `angos worker` details.

Because the queue is drained by separate processes, `[global.job_queue]` requires a **shared** metadata-store lock strategy (`[metadata_store.s3.lock_strategy.s3]` or a `[lock_strategy.redis]` table) so workers serialize on the same jobs; the default in-process `memory` lock is rejected at startup with this section. The in-process mode above (no `[global.job_queue]`) runs in a single process and works with any lock strategy.

## Two-Instance Active-Active Example

Configure each instance with the other as a downstream for the same repository. With no `[global.job_queue]`, each server self-drains in-process -- no separate worker needed.

**Instance A** (`config-a.toml`):

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[global]
max_concurrent_replication_jobs = 4

[blob_store.fs]
root_dir = "/data"

[repository."nginx"]

[repository."nginx".access_policy]
default = "allow"

[[repository."nginx".downstream]]
name = "instance-b"
url = "http://angos-b:8000"
mode = "event+reconcile"
```

**Instance B** (`config-b.toml`) is the mirror image, pointing its downstream at instance A.

Push to A and the tag appears on B within a few seconds:

```bash
docker push localhost:8000/nginx/app:v1
# ... shortly after ...
docker pull localhost:8001/nginx/app:v1   # served from B
```

## Reconcile on Demand

When the event path misses a change (an instance was down, or two instances drifted after a partition), reconcile explicitly:

```bash
# Preview the pushes that would be enqueued -- enqueues nothing
angos -c config.toml scrub --replicate --dry-run

# Enqueue (and, for an in-process queue, drain) the diverging tags
angos -c config.toml scrub --replicate
```

By default reconciliation is **additive**: it pushes diverging or downstream-missing tags and never deletes. With `--dry-run` it previews the work without enqueuing anything: it lists an `EnqueueReplicationPush` for each diverging or downstream-missing tag and, for any downstream marked `prune = true`, an `EnqueueReplicationDelete` for each downstream-only tag.

A downstream marked `prune = true` is treated as an **authoritative one-way mirror**: reconciliation also enumerates its tags (via the OCI `list-tags` endpoint) and deletes any that are absent locally, so it converges exactly to the local tag set. Pruning is **one-way-mirror-only**: enabling it on an active-active peer would delete a tag the peer authored that has not yet replicated back. The delete carries a `source_ts` so the receiver applies last-writer-wins rather than deleting unconditionally, but that only protects a downstream tag dated in the future relative to the reconcile decision — a peer's legitimately-newer tag whose `created_at` predates the reconcile run is still removed. Leave `prune = false` for active-active peers. Re-running is a no-op once converged (coalesced by the queue). Schedule it like any other maintenance task:

```bash
# Cron: reconcile every replicated repository nightly at 4 AM
0 4 * * * /usr/bin/angos -c /etc/registry/config.toml scrub --replicate
```

## Observability

Replication exposes Prometheus metrics on `/metrics`:

```promql
# Push rate by downstream and outcome (pushed, superseded, failed)
sum by (downstream, outcome) (rate(angos_replication_push_total[5m]))

# Seconds since the last successful push per downstream (staleness)
time() - angos_replication_last_success_timestamp_seconds

# Pending replication backlog
angos_job_queue_pending{queue="replication"}
```

See [Metrics Reference](../reference/metrics.md) for the full list.

## Troubleshooting

**Pushes never reach the downstream:**
- Confirm the downstream `url` is reachable from the source instance.
- Verify the credential is authorized to push on the downstream (`put-manifest`, blob uploads).
- If `[global.job_queue]` is configured, ensure an `angos worker` is running (it drains the replication queue by default) -- the server only enqueues.

**A tag does not overwrite on the downstream:**
- The downstream copy may be newer (last-writer-wins): a `409 REPLICATION_SUPERSEDED` is convergence, not failure.
- The downstream tag may be immutable: a `409 CONFLICT` surfaces and the job retries -- relax immutability or pick a different tag.

**Reconciliation reports nothing to do but instances differ:**
- Confirm the downstream's `mode` includes reconciliation (`event+reconcile` or `reconcile-only`).
- Confirm the namespace matches the downstream's `namespace_filter`.

**Debug logging:**
```bash
RUST_LOG=angos::replication=debug ./angos server
```

## Reference

- [Bi-Directional Replication](../explanation/replication.md) - concepts and design
- [Configuration Reference](../reference/configuration.md) - all downstream and global options
- [Metrics Reference](../reference/metrics.md) - replication metrics
- [API Endpoints Reference](../reference/api-endpoints.md) - replication headers and `409 REPLICATION_SUPERSEDED`
