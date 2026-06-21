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

The scrub command performs storage maintenance and integrity checks. You must specify which checks to run using the flags below. Note that garbage collection happens online during normal server operation.

**Options:**

| Option                    | Short  | Description                                                                                        |
|---------------------------|--------|----------------------------------------------------------------------------------------------------|
| `--dry-run`               | `-d`   | Preview what would be removed without making changes                                               |
| `--uploads <duration>`    | `-u`   | Check upload sessions: remove broken or partial state and uploads older than the given duration (e.g., `1h`, `30m`, `2d`) |
| `--multipart <duration>`  | `-p`   | Cleanup orphan S3 multipart uploads older than duration (S3 only)                                  |
| `--tags`                  | `-t`   | Check and fix tag references; remove tags whose target manifest blob is missing; delete tag directories whose names violate the OCI tag grammar |
| `--manifests`             | `-m`   | Recreate missing/mismatched manifest links; report (never delete) references to missing config/layer blobs or index children |
| `--blobs`                 | `-b`   | Remove orphaned blobs and prune stale blob-index entries                                           |
| `--retention`             | `-r`   | Enforce retention policies                                                                         |
| `--links`                 | `-l`   | Fix links format inconsistencies; remove revisions whose manifest blob is missing; prune phantom referrer back-links |
| `--reconcile-blob-index`  |        | Rebuild blob-index entries missing relative to the manifests that reference each blob; repairs an index corrupted out-of-band (storage corruption, manual tampering). Reads every manifest, so it is expensive |
| `--migrate`               |        | Migrate the on-disk storage layout: rewrite legacy single-file blob indexes into the sharded layout and prune the pre-1.3 namespace-registry index. Scans every blob; run once after upgrade, not on routine scrubs |
| `--media-types`           | `-M`   | Backfill missing `media_type` on manifest links; remove revisions whose manifest blob is missing   |
| `--referrers`             | `-R`   | Check for and remove orphan referrer links whose referrer manifest is no longer a current revision |
| `--replicate`             |        | Reconcile every replicated namespace against all its configured downstreams. By default reconciliation is additive: it enqueues a replication push for each diverging or downstream-missing tag and never deletes. A downstream marked `prune = true` is treated as an authoritative one-way mirror: reconciliation also enqueues a replication delete for each downstream-only tag, so prune is one-way-only by design and unsafe for active-active peers (even with receiver-side last-writer-wins it can remove a peer's newer tag). Combine with `--dry-run` to preview. See [Configure Replication](../how-to/configure-replication.md). |
| `--replication-orphans`   |        | Delete replication jobs (pending and dead-lettered) whose downstream or repository is no longer configured                                       |
| `--cache-orphans`         |        | Delete cache jobs (pending and dead-lettered) whose repository is no longer configured for pull-through                                          |
| `--orphan-grants <duration>` |     | Revoke blob-ownership grants older than the duration (e.g. `24h`) that no manifest references, reclaiming the bytes; cleans up blobs a replication push uploaded before its manifest lost last-writer-wins or dead-lettered |
| `--orphan-namespaces`     | `-n`   | Remove revisions, tags, and in-flight uploads for namespaces not owned by any configured repository, and reclaim their layer/config blob bytes by revoking those blobs' ownership grants when no still-configured namespace shares them. Manifest blob bytes are not reclaimed here; run with `--blobs` in the same pass to reclaim them once the links are gone. Destructive: run with `--dry-run` first. Ignored (deletes nothing) when no repositories are configured, so an emptied config can never wipe the whole registry. |

**Examples:**

```bash
# Preview all maintenance operations
angos scrub -d -t -m -b -r

# Run full storage integrity check
angos scrub -t -m -b -r

# Enforce only retention policies
angos scrub --retention

# Check blob storage integrity
angos scrub --blobs

# Remove incomplete uploads older than 1 hour
angos scrub --uploads 1h

# Cleanup orphan S3 multipart uploads older than 24 hours
angos scrub --multipart 24h

# Preview retention policy enforcement
angos scrub --retention --dry-run

# Preview replication reconciliation (enqueues nothing)
angos scrub --replicate --dry-run

# Reconcile every replicated repository with its downstreams
angos scrub --replicate

# Preview clearing namespaces no longer owned by any configured repository
angos scrub --orphan-namespaces --dry-run

# Run with verbose logging
RUST_LOG=info angos scrub -t -m -b -r
```

**Scheduling:**

Run scrub as a scheduled task for regular maintenance:

```bash
# Cron example (daily at 3 AM) - full maintenance
0 3 * * * /usr/bin/angos -c /etc/registry/config.toml scrub -t -m -b -r
```

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
            args: ["-c", "/config/config.toml", "scrub", "-t", "-m", "-b", "-r"]
          restartPolicy: OnFailure
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
