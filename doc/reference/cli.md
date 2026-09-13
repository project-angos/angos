---
displayed_sidebar: reference
sidebar_position: 2
title: "CLI"
---

# CLI Reference

Angos command-line interface.

## Synopsis

```
angos [-c <config...>] <command> [<args>]
```

## Global Options

| Option                | Description                                         |
|-----------------------|-----------------------------------------------------|
| `-c, --config <path>` | Path to a configuration file (default: `config.toml`). Repeatable: files are merged in order and a later file wins. See [Multiple Configuration Files](configuration.md#multiple-configuration-files) |
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

Scrub streams every object key in both stores (blob and metadata), categorizes it by shape, and validates it concurrently, in three ordered passes: tag entries, records and job records first, then the blob-index reference keys, then blob data. It always runs the full set of checks:

- Repairs every revision and referrer record a manifest implies, and re-issues missing blob-index grants.
- Removes tags whose target manifest blob is missing, revisions whose manifest blob is missing, orphan referrer records, and stale blob-index entries.
- Reclaims the filesystem listings of layers no `index = true` repository uses, the same pass as [`reconcile index`](#reconcile-index); images outside those repositories index again when opened.
- Deletes queued jobs, pending or dead-lettered, whose downstream or repository is no longer configured; [`reconcile`](#reconcile) re-issues the work if the configuration returns.
- Deletes objects whose content is unreadable (a job record or access entry that does not parse).
- Reclaims blobs with no references, past the reclamation grace period and fenced by a `v2/gc/` run marker at apply time, so it is safe alongside a live server.
- Moves any key that matches no known angos layout to `_lost_and_found/` in the same store, preserving its bytes for inspection. This covers every retired shape, including the pre-1.7 link files and the transaction engine's `.tx-*` keys. Emptying that prefix is the operator's job. With `--delete-unknown` such keys are deleted outright instead.

Scrub deletes only what is dead or derivable: it takes no age thresholds, and its configuration-relative decisions, which listings to keep and which queued jobs still resolve, concern state that comes back on its own. Retention, time-based reclamation and orphan-namespace clearing, which delete live content by policy, belong to [`angos prune`](#prune).

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

It also clears **orphan namespaces**, always on: every namespace not owned by any configured `[repository]` loses its revisions, tags, in-flight uploads, and blob-ownership grants. The blast radius is every namespace whose owning repository is not in your config, so run `--dry-run` after config changes. Refused when no repositories are configured, so an emptied config can never wipe the registry.

Prune is the config-and-time command: run it against the same configuration file the servers use. It refuses to start when a retention rule uses `image.last_pulled_at` or `top_pulled` while `update_pull_time` is disabled: pull times would never be recorded, so those rules would match nothing and actively pulled images would be deleted.

**Options:**

| Option              | Short | Description                                                              |
|---------------------|-------|---------------------------------------------------------------------------|
| `--dry-run`         | `-d`  | Preview what would be deleted without changes                            |
| `--uploads <dur>`   | `-u`  | Age window for upload-lifecycle reclamation (default `1h`)               |
| `--concurrency <N>` |       | Namespaces, uploads, blobs, or index entries checked concurrently per sweep (default 25); each namespace adds a small fixed tag-read fan-out of its own |

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

### reconcile

On-demand passes that bring stored content in line with the configuration.
Each enqueues jobs rather than acting inline, so its work gets the event
path's retry, backoff and coalescing.

```bash
angos reconcile replication [options]
angos reconcile scan [options]
angos reconcile index [options]
```

#### reconcile replication

Reconcile every replicated namespace against all its configured downstreams.

By default reconciliation is additive: it enqueues a replication push for each diverging or downstream-missing tag and never deletes, then drains the enqueued jobs in-process. A downstream marked `prune = true` is treated as an authoritative one-way mirror: reconciliation also enqueues a replication delete for each downstream-only tag, so it is one-way-only by design and unsafe for active-active peers (even with receiver-side last-writer-wins it can remove a peer's newer tag). See [Configure Replication](../how-to/configure-replication.md).

| Option      | Short | Description                                    |
|-------------|-------|------------------------------------------------|
| `--dry-run` | `-d`  | Preview what would be enqueued without changes |

#### reconcile scan

Enqueue a scan job for every image manifest of a `scan = true` repository that carries no report, so images pushed before scanning was enabled, or whose scan failed past its retries, get one. The running server or a worker drains the jobs; the command returns once they are enqueued. See [Scan Images](../how-to/scan-images.md).

| Option      | Short | Description                                              |
|-------------|-------|----------------------------------------------------------|
| `--dry-run` | `-d`  | Preview what would be enqueued without changes           |
| `--force`   |       | Scan every image again, attaching a fresh report to each |

#### reconcile index

Enqueue a filesystem index job for every tar layer of the images of an `index = true` repository that has no listing yet, so the web UI opens them without an "Indexing" wait, and reclaim the listings of every other layer. A layer shared by several images is enqueued once, and kept while any `index = true` repository uses it. Any image indexes itself the first time its filesystem is opened, so this is for having the listings ready ahead of that, or, with `--force`, for walking every layer again; the reclaim drops what those on-demand opens left behind in repositories without the flag, which index again when opened. The running server or a worker drains the jobs; the command returns once they are enqueued and the listings reclaimed. See [Explore Image Filesystems](../how-to/explore-image-filesystems.md).

| Option      | Short | Description                                                 |
|-------------|-------|-------------------------------------------------------------|
| `--dry-run` | `-d`  | Preview what would be enqueued and reclaimed without changes |
| `--force`   |       | Walk every layer again, rewriting its listing               |

**Examples:**

```bash
# Preview replication reconciliation (enqueues nothing)
angos reconcile replication --dry-run

# Reconcile every replicated repository with its downstreams
angos reconcile replication

# Give every unreported image a scan
angos reconcile scan

# Re-scan everything after a scanner database update
angos reconcile scan --force

# Index the layers of every image nobody has opened yet, and reclaim the
# listings of repositories that no longer index
angos reconcile index
```

---

### worker

Process durable background jobs from the job queue. With no `--queue` argument
the worker drains the pull-through cache queue, the replication queue, the
layer index queue and, when `[global.scan]` is configured, the scan queue,
each on its own worker pool. Pass `--queue` (repeatable) to drain specific
queues instead, e.g. `angos worker --queue replication`.

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
| `--queue <name>` | every configured queue | Queue to drain: `cache`, `replication` or `scan`. Repeatable; each queue runs its own worker pool sized by `max_concurrent_cache_jobs`, `max_concurrent_replication_jobs` or `max_concurrent_scan_jobs`. |
| `--poll-interval <duration>` | `1s` | Minimum idle sleep between claim attempts. When the queue contains only backed-off envelopes, the worker extends the wait up to the soonest `not_before` (capped at 1 minute, or `--poll-interval` if it is larger). |

**Example:**

```bash
angos -c config.toml worker
```

---

### scanner

Run the scanner service: it answers each `POST /scan` naming an image with the
SARIF report of the named scanner, pulling the image under the `[scanner]`
identity. The registry's scan jobs call it.

```bash
angos scanner <scanner>
angos -c /etc/angos/scanner.toml scanner grype
```

Reads the `[scanner]` section alone, so its configuration file need not
describe a registry. The scanner, `grype` or `trivy`, must be on `PATH`. A
request is checked against `[scanner] token` when one is set. See
[Scan Images](../how-to/scan-images.md).

**Arguments:**

| Argument | Description |
|---|---|
| `<scanner>` | `grype` or `trivy` |

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
