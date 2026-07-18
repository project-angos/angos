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

Walk the store, repair inconsistencies, and quarantine unrecognized objects.

```bash
angos scrub [options]
```

Scrub streams every object key in both stores (blob and metadata), categorizes it by shape, and validates it concurrently, in three ordered passes: links and job records first, then blob-index shards, then blob data. It always runs the full set of checks:

- Repairs every link a manifest implies (config, layer, sub-manifest, digest revision), the `referenced_by` back-links, and missing blob-index grants.
- Removes tags whose target manifest blob is missing, revisions whose manifest blob is missing, orphan referrer entries, stale blob-index entries, and tag or namespace directories whose names violate the OCI grammar.
- Deletes objects whose content is unreadable (a link, job record, or index shard that does not parse).
- Reclaims blobs with no references (re-checked under the blob-data lock, so it is safe alongside a live server).
- Moves any key that matches no known angos layout to `_lost_and_found/` in the same store, preserving its bytes for inspection. Emptying that prefix is the operator's job. With `--delete-unknown` such keys are deleted outright instead.
- Runs the transaction engine's janitor sweeps (orphaned staging bodies, expired lock objects); janitor sweeps run only in scrub, while serving processes run only the recovery loop.

Scrub is purely structural: it takes no age thresholds and no configuration-relative decisions. Time-based reclamation and orphan-namespace clearing belong to [`angos prune`](#prune).

Because a repair can create new derivable state, a heavily damaged store may need more than one run to fully converge; run scrub until it reports zero changes.

**Warning:** scrub quarantines keys it does not recognize, so it must be run from the same angos version as the server fleet. After an upgrade, run `scrub -d` first and review the report.

**Options:**

| Option                | Short  | Description                                                                 |
|-----------------------|--------|------------------------------------------------------------------------------|
| `--dry-run`           | `-d`   | Preview what would be changed without applying anything                     |
| `--concurrency <N>`   |        | Number of keys validated concurrently per pass (default 25)                  |
| `--delete-unknown`    |        | Delete unrecognized keys outright instead of quarantining them              |

**Examples:**

```bash
# Preview everything scrub would do
angos scrub --dry-run

# Full structural check and repair
angos scrub

# Faster walk on a large store
angos scrub --concurrency 32

# Discard unrecognized keys instead of keeping them under _lost_and_found/
angos scrub --delete-unknown
```

**Scheduling:**

Run scrub as a scheduled task for regular maintenance, with a Kubernetes CronJob or a systemd timer:

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
            args: ["-c", "/config/config.toml", "scrub"]
          restartPolicy: OnFailure
```

On a host install, use a systemd timer instead; see [Run Storage Maintenance](../how-to/run-storage-maintenance.md#systemd-timer) for the unit files.

---

### prune

Enforce retention policies and reclaim aged upload-lifecycle leftovers.

```bash
angos prune [options]
```

Applies the global and per-repository retention policies to every namespace (see [Configure Retention Policies](../how-to/configure-retention-policies.md) for the policy syntax and what is protected from deletion), then reclaims everything gated on the `-u` age window:

- Upload sessions older than the window, or with broken session state.
- Orphan S3 multipart uploads older than the window whose session marker is gone (a crash between opening the multipart and writing the marker).
- Byteless blob-index entries: a grant written by an upload whose bytes never landed.

Grant-only blob ownership (a blob uploaded whose manifest never landed) is decided by the **retention policies** like any other untagged content: the subject carries no tag and `pushed_at` is the upload time, the `-u` window only shields in-flight pushes from consideration, and with no policies configured the grant is retained.

It also performs the configuration-relative cleanup, always on:

- **Orphan namespaces**: every namespace not owned by any configured `[repository]` loses its revisions, tags, in-flight uploads, and blob-ownership grants. The blast radius is every namespace whose owning repository is no longer in your config, so run `--dry-run` after config changes. Refused when no repositories are configured, so an emptied config can never wipe the registry.
- **Orphan jobs**: queued replication and cache jobs whose downstream or repository is no longer configured.

Prune is the config-and-time command: run it against the same configuration file the servers use. It refuses to start when a retention rule uses `image.last_pulled_at` or `top_pulled` while `update_pull_time` is disabled: pull times would never be recorded, so those rules would match nothing and actively pulled images would be deleted.

**Options:**

| Option              | Short | Description                                                              |
|---------------------|-------|---------------------------------------------------------------------------|
| `--dry-run`         | `-d`  | Preview what would be deleted without changes                            |
| `--uploads <dur>`   | `-u`  | Age window for upload-lifecycle reclamation (default `1h`)               |
| `--concurrency <N>` |       | Namespaces, uploads, blobs, or shards checked concurrently per sweep (default 25) |

**Examples:**

```bash
# Preview retention enforcement and upload reclamation
angos prune --dry-run

# Enforce retention policies; reap upload leftovers older than 1 hour
angos prune

# Keep in-flight uploads alive for up to a day
angos prune --uploads 24h
```

Schedule `prune` like `scrub`, with a Kubernetes CronJob or a systemd timer; see [Configure Retention Policies](../how-to/configure-retention-policies.md#scheduled-enforcement) for complete examples.

---

### replicate

Reconcile every replicated namespace against all its configured downstreams.

```bash
angos replicate [options]
```

By default reconciliation is additive: it enqueues a replication push for each diverging or downstream-missing tag and never deletes, then drains the enqueued jobs in-process. A downstream marked `prune = true` is treated as an authoritative one-way mirror: reconciliation also enqueues a replication delete for each downstream-only tag, so it is one-way-only by design and unsafe for active-active peers (even with receiver-side last-writer-wins it can remove a peer's newer tag). See [Configure Replication](../how-to/configure-replication.md).

**Options:**

| Option      | Short | Description                                    |
|-------------|-------|------------------------------------------------|
| `--dry-run` | `-d`  | Preview what would be enqueued without changes |

**Examples:**

```bash
# Preview replication reconciliation (enqueues nothing)
angos replicate --dry-run

# Reconcile every replicated repository with its downstreams
angos replicate
```

---

### migrate

Convert pre-JSON bare-digest link files to the current JSON format. Needed only for registries seeded from a raw Docker `distribution` on-disk layout, whose links the serving paths no longer read.

```bash
angos migrate [options]
```

Walks every link object once and rewrites each bare-digest file as JSON, leaving already-JSON links and unrecognizable files untouched. It is idempotent, so an interrupted run can be re-run. See [Upgrade Guide](../how-to/upgrade.md#legacy-link-metadata-breaking-change).

**Options:**

| Option      | Short | Description                                    |
|-------------|-------|------------------------------------------------|
| `--dry-run` | `-d`  | Report what would be rewritten without changes |

**Examples:**

```bash
# Preview which links would be rewritten
angos migrate --dry-run

# Rewrite bare-digest links as JSON
angos migrate
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
| 1     | General error (invalid config, runtime error) |

---

## Logging

Angos uses the `RUST_LOG` environment variable for log configuration.

**Log Levels:**
- `error` - Errors only
- `warn` - Warnings and errors
- `info` - Informational messages (recommended for production)
- `debug` - Detailed debugging information
- `trace` - Very verbose tracing
