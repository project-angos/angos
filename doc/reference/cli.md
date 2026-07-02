---
displayed_sidebar: reference
sidebar_position: 2
title: "CLI"
---

# CLI Reference

Angos command-line interface.

## Synopsis

```
angos [-c <config>] <command> [<args>]
```

## Global Options

| Option                | Description                                         |
|-----------------------|-----------------------------------------------------|
| `-c, --config <path>` | Path to configuration file (default: `config.toml`) |
| `--help, help`        | Display usage information                           |

---

## Commands

### server

Run the registry HTTP server.

```bash
angos server
angos -c /etc/registry/config.toml server
```

The server starts listening on the configured `bind_address` and `port`. It handles:
- OCI Distribution API requests
- Extension API endpoints
- Web UI (if enabled)
- Health and metrics endpoints

**Environment Variables:**

| Variable   | Description                                                       |
|------------|-------------------------------------------------------------------|
| `RUST_LOG` | Log level filter (e.g., `info`, `debug`, `angos=debug`) |

**Examples:**

```bash
# Run with info logging
RUST_LOG=info angos server

# Run with debug logging for specific module
RUST_LOG=angos::registry=debug angos server

# Run with custom config
angos -c production.toml server
```

---

### scrub

Check storage for inconsistencies and perform maintenance tasks.

```bash
angos scrub [options]
```

`scrub` is a **rebuild-and-sweep** operation: it re-derives every revision's derived metadata (links, blob-index grants, referrer back-links, `media_type`) from the current manifest, then raw-enumerates storage and reclaims what the rebuild does not account for. It is **report-only by default**: without `--commit` it performs no storage mutation at all except the run marker; the sweep deletions, the rebuild's additive repairs, the legacy blob-index layout convergence, and the legacy `_registry/` prune all land only under `--commit`. Every garbage-collection reap is additionally age-gated on the maintenance grace (`maintenance_grace_secs`, default 48h): a candidate is deleted only once its backend `last_modified` is older than the grace, and an object without a `last_modified` is never reaped. Two categories keep their own gates: de-configured namespace deletion is config-driven (not age-gated), and the upload cleaners take their age from the `-u`/`-p` flag durations. There are no per-checker flags to select; the rebuild and sweep always run. Note that garbage collection also happens online during normal server operation.

**Active options:**

| Option                    | Short  | Description                                                                                        |
|---------------------------|--------|----------------------------------------------------------------------------------------------------|
| `--commit`                |        | Global deletion gate: enable destructive actions across every sweep category. Off by default (report-only). Mutually exclusive with `--dry-run`. A mutating configuration is refused when the metadata store's `lock_strategy` is `memory` (no cross-process exclusion) |
| `--delete-unrecognized`   |        | Also delete unrecognized keys, gated on `--commit`. Without `--commit` the pass classifies and tallies would-delete counts; with `--commit --delete-unrecognized` each unrecognized key older than the maintenance grace is deleted |
| `--uploads <duration>`    | `-u`   | Check upload sessions: remove broken or partial state and uploads older than the given duration (e.g., `1h`, `30m`, `2d`) |
| `--multipart <duration>`  | `-p`   | Abort orphan S3 multipart uploads older than duration (S3 only)                                    |
| `--replication-orphans`   |        | Delete replication jobs (pending and dead-lettered) whose downstream or repository is no longer configured                                       |
| `--cache-orphans`         |        | Delete cache jobs (pending and dead-lettered) whose repository is no longer configured for pull-through                                          |
| `--jobs`                  |        | Structural job reconcile: drop dangling `_jobs/index` lock-key entries whose pending envelope is gone and rebuild the missing or stale dedup entry of a present pending job (a claimed, in-flight job is skipped); with `--prune-unknown` also remove unknown-named queue directories quiescent for the maintenance grace |
| `--prune-unknown`         |        | Opt-in modifier (off by default): escalate structurally-invalid objects (invalid-named namespaces, unknown job-queue directories) from report-only to deleted. Run report-only first |
| `--reclaim-engine`        |        | Run one on-demand pass of the transaction engine's pure-delete janitors (orphan `.tx-bodies/` staging older than 1h, expired `.tx-locks/`). Honours report-only/`--commit` |

**Deprecated options** (accepted for compatibility, emit a one-time warning, will be removed in a future release):

| Option                    | Short  | Status | Replacement                                                                              |
|---------------------------|--------|--------|------------------------------------------------------------------------------------------|
| `--dry-run`               | `-d`   | deprecated (writes nothing to registry state, like the report-only default; only the best-effort `latest.json` run marker) | the report-only default (omit flags); `--commit` to delete |
| `--retention`             | `-r`   | deprecated (still works; mutates retention unless `-d`, like the dedicated subcommand) | `angos policy --retention` |
| `--replicate`             |        | deprecated (still works; mutates replication unless `-d`, like the dedicated subcommand) | `angos replication` |

The 1.3.0 per-checker flags (`-m`/`--manifests`, `-l`/`--links`, `-M`/`--media-types`, `-t`/`--tags`, `-R`/`--referrers`, `-b`/`--blobs`, `-n`/`--orphan-namespaces`, `--orphan-grants`, `--reconcile-blob-index`) are **removed** and fail argument parsing. Their work runs unconditionally in the rebuild and sweep; see the [upgrade guide](../how-to/upgrade.md) for the per-flag mapping.

**Exit codes:** `0` clean, `1` refused at entry (lock held, or a mutating configuration on a `memory` lock strategy), `2` degraded (a rebuild-fatal namespace, an incomplete walk, a failed delete or action, a partial de-configured delete or grant revoke, or a convergence failure), `3` aborted (lock lost or max hold elapsed mid-run). Schedulers such as systemd and Kubernetes treat `2` and `3` as failures.

**Durability:** scrub overwrites a liveness marker at `_scrub-audit/latest.json` at run end with the run's status, exit code, mode (`commit` / `report-only` / `dry-run`, so a watcher can tell performed deletions from would-counts), and tallies; the log lines are the record of individual deletions. Each run holds the single-instance maintenance lock (shared with `policy` and `replication`, capped by `maintenance_lock_max_hold_secs`); a concurrent maintenance command refuses; an aborted run simply re-runs from scratch next time. Manifest pushes hold the per-blob lock for each newly referenced blob, so storage maintenance can never reclaim bytes a concurrent push references.

**Run Summary:**

Every `scrub` run ends with an `info!`-level summary block: per-category counts of the actions taken (or that would be taken, under report-only), plus a "Report-only findings" section listing what was observed but never acted on. A report-only run reports `would` counts; a `--commit` run reports `done` counts (the same categories). The summary is a log line (visible at `RUST_LOG=info`), not stdout, and appears after the rebuild and sweep complete. The same summary is emitted by [`policy`](#policy) and [`replication`](#replication). The structural `--jobs` reconcile is not reflected in these counts (it repairs the job store directly, outside the action path); it logs its own retired/re-indexed/found totals on a separate line.

**Examples:**

```bash
# Report-only classification (writes nothing but the run marker)
angos scrub

# Rebuild-and-sweep, committing deletions (age-gated on the maintenance grace)
angos scrub --commit

# Also delete unrecognized keys older than the maintenance grace (needs --commit)
angos scrub --commit --delete-unrecognized

# Enforce only retention policies (`angos scrub --retention` is a deprecated alias)
angos policy --retention

# Remove incomplete uploads older than 1 hour once committed
angos scrub --commit --uploads 1h

# Cleanup orphan S3 multipart uploads older than 24 hours
angos scrub --commit --multipart 24h

# Reclaim engine-owned staging/lock keys in the same run
angos scrub --commit --reclaim-engine

# Run with verbose logging
RUST_LOG=info angos scrub
```

**Scheduling:**

Run scrub as a scheduled task for regular maintenance with a systemd timer. Validate with a report-only run first, then commit on the recurring job:

```ini
# systemd timer (daily at 03:00): rebuild-and-sweep, committing deletions
# /etc/systemd/system/angos-scrub.service
[Service]
Type=oneshot
ExecStart=/usr/bin/angos -c /etc/registry/config.toml scrub --commit

# /etc/systemd/system/angos-scrub.timer
[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enforce retention with a sibling timer running `angos policy --retention`.

```yaml
# Kubernetes CronJob
apiVersion: batch/v1
kind: CronJob
metadata:
  name: registry-maintenance
spec:
  schedule: "0 3 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: scrub
            image: ghcr.io/project-angos/angos:latest
            args: ["-c", "/config/config.toml", "scrub", "--commit"]
          restartPolicy: OnFailure
```

---

### policy

Enforce storage policy: retention. An operation flag (`--retention`) is required; running `angos policy` with no flag does nothing. "Mutate by default" means that once an operation is selected it mutates unless `-d` is given. Load config the same way as `scrub`.

```bash
angos policy [options]
```

The deprecated `scrub --retention` alias runs the same checker and emits a one-time warning pointing here. Replication reconcile is a sync operation, not policy enforcement, so it lives in its own [`angos replication`](#replication) subcommand.

A mutating run (`-r` without `-d`) holds the shared maintenance lock for its duration and is refused when another maintenance command (`scrub`, `policy`, or `replication`) holds it, or when the metadata store's `lock_strategy` is `memory` (no cross-process exclusion). A dry run takes no lock.

Like `scrub`, the run ends with a [summary](../how-to/run-storage-maintenance.md#run-summary) of counts (dry-run shows `would`, mutate shows `done`).

**Exit codes:** `0` clean, `1` refused at entry (lock held, or a mutating run on a `memory` lock strategy), `2` degraded (an incomplete walk or a failed namespace), `3` aborted (lock lost or max hold elapsed mid-run). Schedulers such as systemd and Kubernetes treat `2` and `3` as failures.

**Options:**

| Option            | Short | Description |
|-------------------|-------|-------------|
| `--dry-run`       | `-d`  | Preview what would be changed without applying anything |
| `--retention`     | `-r`  | Enforce retention policies |

**Examples:**

```bash
# Enforce retention policies
angos policy --retention

# Preview retention enforcement (deletes nothing)
angos policy --retention --dry-run
```

---

### replication

Reconcile replicated namespaces with their downstreams. This is a **sync operation**, not policy enforcement, so it has its own subcommand. Mutate by default; `-d` previews. Load config the same way as `scrub`.

```bash
angos replication [options]
```

The deprecated `scrub --replicate` alias runs the same reconcile and emits a one-time warning pointing here.

A mutating run (no `-d`) holds the shared maintenance lock for its duration and is refused when another maintenance command (`scrub`, `policy`, or `replication`) holds it, or when the metadata store's `lock_strategy` is `memory` (no cross-process exclusion). A dry run takes no lock.

The reconcile walks every replicated namespace against all its configured downstreams. By default reconciliation is additive: it enqueues a replication push for each diverging or downstream-missing tag and never deletes, then drains the enqueued jobs to convergence in-process. The drain processes only immediately-claimable jobs; replication jobs already backed off from earlier failures are left for `angos worker` to retry, so a single run converges the reconcile's own enqueues but not a backlog of failed jobs. A push that fails during the drain marks the run degraded (exit `2`), even though its job is rescheduled for a later retry. A downstream marked `prune = true` is treated as an authoritative one-way mirror: reconciliation also enqueues a replication delete for each downstream-only tag, so prune is one-way-only by design and unsafe for active-active peers (even with receiver-side last-writer-wins it can remove a peer's newer tag). Combine with `--dry-run` to preview without enqueuing or pushing anything. See [Configure Replication](../how-to/configure-replication.md).

Like `scrub`, the run ends with a [summary](../how-to/run-storage-maintenance.md#run-summary) of counts (dry-run shows `would`, mutate shows `done`).

**Exit codes:** `0` clean, `1` refused at entry (lock held, or a mutating run on a `memory` lock strategy), `2` degraded (an incomplete walk, a failed namespace, or a downstream push that failed to converge during the drain), `3` aborted (lock lost or max hold elapsed mid-run). Schedulers such as systemd and Kubernetes treat `2` and `3` as failures.

**Options:**

| Option            | Short | Description |
|-------------------|-------|-------------|
| `--dry-run`       | `-d`  | Preview what would be reconciled without enqueuing or pushing anything |

**Examples:**

```bash
# Reconcile every replicated repository with its downstreams
angos replication

# Preview replication reconciliation (enqueues nothing)
angos replication --dry-run
```

---

### worker

Process durable background jobs from the job queue. With no `--queue` argument
the worker drains **both** the pull-through cache queue and the replication
queue, each on its own worker pool. Pass `--queue` (repeatable) to drain
specific queues instead, e.g. `angos worker --queue replication`.

```bash
angos worker [options]
angos -c /etc/registry/config.toml worker
```

Requires `[global.job_queue]` to be configured in `config.toml`. Run at least
one `angos worker` alongside `angos server` whenever durable jobs are
enabled: the server only enqueues jobs; it does not process them. The worker
hot-reloads `config.toml` just like `angos server`: changes to
`[global.job_queue]`, `[repository.*]`, `[blob_store.*]`, or
`[metadata_store.*]` take effect at the next claim; in-flight jobs always
finish on the components they started with.

**Options:**

| Option | Default | Description |
|---|---|---|
| `--queue <name>` | `cache` and `replication` | Queue to drain. Repeatable (`--queue cache --queue replication`); each queue runs its own worker pool sized by `max_concurrent_cache_jobs` / `max_concurrent_replication_jobs`. |
| `--poll-interval <duration>` | `1s` | Minimum idle sleep between claim attempts. When the queue contains only backed-off envelopes, the worker extends the wait up to the soonest `not_before` (capped at 1 minute, or `--poll-interval` if it is larger). |

**Example:**

```bash
angos -c config.toml worker
```

---

### argon

Generate Argon2 password hashes for basic authentication.

```bash
angos argon
```

Interactive command that prompts for a password and outputs the Argon2 hash. Use this hash in the `auth.identity.<name>.password` configuration.

**Example:**

```bash
$ angos argon
Input Password: ********
$argon2id$v=19$m=19456,t=2,p=1$randomsalt$hashvalue
```

Then use in configuration:

```toml
[auth.identity.alice]
username = "alice"
password = "$argon2id$v=19$m=19456,t=2,p=1$randomsalt$hashvalue"
```

---

## Exit Codes

| Code  | Description                                   |
|-------|-----------------------------------------------|
| 0     | Success                                       |
| 1     | General error (invalid config, runtime error, refused maintenance run) |
| 2     | Maintenance commands (`scrub`, `policy`, `replication`): completed degraded (an incomplete walk, a failed namespace, or a failed action) |
| 3     | Maintenance commands (`scrub`, `policy`, `replication`): aborted mid-run (lock lost or max hold elapsed) |

Schedulers such as systemd and Kubernetes treat any non-zero code, including `2` and `3`, as a failed run.

---

## Logging

Angos uses the `RUST_LOG` environment variable for log configuration.

**Log Levels:**
- `error` - Errors only
- `warn` - Warnings and errors
- `info` - Informational messages (recommended for production)
- `debug` - Detailed debugging information
- `trace` - Very verbose tracing
