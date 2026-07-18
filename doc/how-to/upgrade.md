---
displayed_sidebar: howto
sidebar_position: 99
title: "Upgrade Angos"
---

# Upgrade Angos

This guide covers breaking configuration changes introduced across releases and the steps needed to migrate an existing deployment.

---

## 1.0.x → 1.1.0

### Redis Lock Configuration (Breaking Change)

#### What Changed

The Redis lock table has moved from `[metadata_store.*.redis]` to `[metadata_store.*.lock_strategy.redis]`.

**Who is affected:** Only deployments that explicitly configured Redis distributed locking. If you are using the default in-memory lock strategy (i.e., your configuration does not contain a `[metadata_store.*.redis]` table), no action is required.

The old form is still accepted for backward compatibility but is deprecated and will be removed in a future release.

#### Migrate a Filesystem Metadata Store

**Before:**

```toml
[metadata_store.fs]
root_dir = "/data/metadata"

[metadata_store.fs.redis]
url = "redis://localhost:6379"
ttl = 10
key_prefix = "locks"
```

**After:**

```toml
[metadata_store.fs]
root_dir = "/data/metadata"

[metadata_store.fs.lock_strategy.redis]
url = "redis://localhost:6379"
ttl = 10
key_prefix = "locks"
```

#### Migrate an S3 Metadata Store

**Before:**

```toml
[metadata_store.s3]
bucket = "my-registry-meta"
region = "us-east-1"

[metadata_store.s3.redis]
url = "redis://localhost:6379"
ttl = 10
```

**After:**

```toml
[metadata_store.s3]
bucket = "my-registry-meta"
region = "us-east-1"

[metadata_store.s3.lock_strategy.redis]
url = "redis://localhost:6379"
ttl = 10
```

Rename the section header in your configuration file and restart the registry. No data migration is required.

---

### New Features Available in 1.1.0

#### S3-Native Distributed Locking

S3 metadata store deployments can now use S3 conditional writes for distributed locking instead of Redis, eliminating the need for a separate Redis instance:

```toml
[metadata_store.s3.lock_strategy.s3]
ttl_secs = 30
```

See [Distributed Locking](../reference/configuration.md#distributed-locking) in the configuration reference for full options and tuning guidance.

---

## 1.1.x → 1.2.0

### Redirect Configuration Split

#### What Changed

`global.enable_redirect` has been deprecated and replaced by two separate flags:

- `global.enable_blob_redirect`: controls HTTP 307 redirects for blob (layer/config) downloads.
- `global.enable_manifest_redirect`: controls HTTP 307 redirects for manifest downloads.

Both default to `true`, preserving historical behavior. The old `enable_redirect` field was accepted as a fallback for both new flags in 1.2.0 through 1.3.x and is **removed in 1.4.0**; migrate to the two new flags.

Additionally, presigned manifest URLs now include a `response-content-type` query parameter so that S3 serves the correct OCI/Docker media type after a redirect, rather than `binary/octet-stream`. This fixes `podman pull` and `skopeo copy` failures against Angos deployments with S3-backed storage and redirects enabled.

#### Migration

**If you had `enable_redirect = false` as a workaround for Podman/Skopeo manifest parsing errors**, you can now enable redirects:

```toml
[global]
enable_blob_redirect = true
enable_manifest_redirect = true
```

Or simply remove the `enable_redirect = false` line, since the default for both new flags is `true`.

**If you want to keep redirects disabled**:

```toml
[global]
enable_blob_redirect = false
enable_manifest_redirect = false
```

**If you still set `enable_redirect`**, it is ignored as of 1.4.0 (the key is no longer read); set `enable_blob_redirect` and `enable_manifest_redirect` instead.

### Extension API Path Change (Breaking Change)

#### What Changed

The angos extension API moved from the `/v2/_ext/...` prefix to the top-level `/_ext/...` prefix, so `/v2` is reserved for the OCI Distribution API.

#### Migration

Update any clients of the old `/v2/_ext/...` endpoints to the new `/_ext/...` paths. See the [Extension API](../reference/api-endpoints.md#extension-api-not-part-of-the-oci-specification) reference for the full endpoint list.

---

## 1.2.x → 1.3.0

### Durable Queue Shared Lock (Breaking Change)

#### What Changed

A configuration that declares `[global.job_queue]` while the metadata store effectively uses the in-process `memory` lock strategy is now rejected at process boot (during config load and validation, or at startup once the provider probe resolves). The check fires only when `[global.job_queue]` is present; configurations without it (the in-process queue) are unaffected.

**Who is affected:** Deployments that enabled the durable out-of-process queue with `[global.job_queue]` but left `lock_strategy` unset on a filesystem metadata store or on an S3 provider without conditional-operation support; there the lock falls back to in-process `memory`, so a 1.2.0 durable-queue config that omitted `lock_strategy` booted on 1.2.0 (which had no such check) but fails to boot after upgrade. On S3 providers with conditional-operation support, an unset `lock_strategy` defaults to the shared S3 lock and such configs keep booting. The boot failure reports:

```text
[global.job_queue] needs a shared lock strategy so workers serialize on the same jobs across processes; the in-process 'memory' lock cannot coordinate across processes. Set the metadata store's lock_strategy to "s3" or "redis", or remove [global.job_queue] to use the in-process queue.
```

#### Migration

Set a shared `lock_strategy` on the metadata store. The valid strategies depend on the backend.

**S3 metadata store**, use either the S3 CAS lock or Redis:

```toml
[metadata_store.s3.lock_strategy.s3]
ttl_secs = 30
```

```toml
[metadata_store.s3.lock_strategy.redis]
url = "redis://localhost:6379"
ttl = 10
```

**Filesystem metadata store**, the only valid shared lock is Redis (the `s3` strategy is not supported on filesystem storage):

```toml
[metadata_store.fs.lock_strategy.redis]
url = "redis://localhost:6379"
ttl = 10
```

**Alternatively, for either backend**, remove `[global.job_queue]` to fall back to the single-process in-process queue, which keeps working with the `memory` lock.

See [Distributed Locking](../reference/configuration.md#distributed-locking) in the configuration reference for full options.

### Worker Dual-Queue Default (Breaking Change)

#### What Changed

`angos worker` invoked with no `--queue` now drains both the `cache` and `replication` queues, each on its own worker pool. In 1.2.0 a bare `angos worker` drained only the `cache` queue.

**Who is affected:** Operators who ran a bare `angos worker` and relied on it to mean cache-only. After upgrade the same invocation also drains replication.

The `--queue` flag is now repeatable. Unknown values are rejected when the worker builds its components:

```text
unknown queue '<value>'; expected 'cache' or 'replication'
```

#### Migration

To keep the prior cache-only behavior, change the invocation:

```text
angos worker --queue cache
```

To run replication only, use `angos worker --queue replication`. The bare `angos worker` remains valid but now drains both queues. The worker subcommand still requires `[global.job_queue]` to be configured.

### Mount-Blob Default-Deny (Breaking Change)

#### What Changed

A cross-repo blob mount (`POST /v2/{namespace}/blobs/uploads/?mount={digest}`) is now authorized as its own CEL action, `mount-blob`, distinct from `start-upload`. In 1.2.0 the same request was authorized as `start-upload`, so an upload allow-rule covered mounts.

**Who is affected:** Deployments running a `default = "deny"` access policy. Container clients (Docker, containerd) send `?mount=` opportunistically on push, so an identity allowed to `start-upload` but not `mount-blob` has those pushes rejected. Deployments with no access policy, or `default = "allow"` with no mount-deny rule, need no action.

#### Migration

Under `default = "deny"`, grant `mount-blob` to every identity already allowed to upload. Add it to the upload action set:

```toml
[repository."app".access_policy]
default = "deny"
rules = [
  "identity.id == 'replicator' && request.action in ['put-manifest', 'delete-manifest', 'start-upload', 'update-upload', 'complete-upload', 'mount-blob']",
]
```

Or add a standalone rule:

```toml
"identity.id == 'replicator' && request.action == 'mount-blob'"
```

See [Set Up Access Control](set-up-access-control.md) for the full `mount-blob` action reference.

### Content-Derived Namespace Catalog

#### What Changed

The `_catalog` listing is now derived directly from stored content rather than from a maintained namespace-registry index. A namespace is listed exactly when it holds at least one revision or tag, so the catalog is deterministic and strongly consistent.

**Who is affected:** No one needs to act. Pre-existing namespace-registry index objects (`_registry/namespaces.json` and `_registry/ns/*.json`) written by earlier versions are no longer read or written. They are inert; run `scrub` on your current version before upgrading to have them removed automatically, or leave them in place and delete them manually later.

### Manifest-Reference Validation Now Permissive by Default

#### What Changed

1.2.0 began rejecting a manifest push at the manifest endpoint when a referenced config, layer, or child manifest was not already present and owned by the target namespace (returning `MANIFEST_BLOB_UNKNOWN`). That broke some `docker buildx`/`bake` pushes of multi-manifest image indexes and provenance/SBOM attestations whose children are not namespace-local at validation time.

This is now controlled by `global.allow_missing_manifest_references`, which **defaults to `true`** (the pre-1.2.0 permissive behavior). No configuration change is required to restore working `docker bake` pushes after upgrade. A reference whose content the namespace does not own is accepted but left unreadable: it resolves as unknown on a later pull (`BLOB_UNKNOWN` for a blob, `MANIFEST_UNKNOWN` for a child manifest) until its content is pushed, so namespace isolation holds in either mode (a caller never gains read access to a blob digest it never uploaded).

**Who is affected:** Anyone who relied on 1.2.0's strict rejection. To reject such pushes outright instead of accepting them with dangling references, opt back in:

```toml
[global]
allow_missing_manifest_references = false
```

`subject` referrers are accepted regardless of this setting, and pull-through cache-fill writes are trusted, independent of the flag.

### In-Process Job Queue Moved to the Metadata Store

#### What Changed

The blob store is now pure storage with no transaction engine. The in-process job queue (used when `[global.job_queue]` is absent) persists its `_jobs/` records on the metadata store instead of the blob store.

**Who is affected:** Only deployments that run the in-process queue **and** place the blob store and metadata store on separate backends. When both share one backend (the default), the `_jobs/` location is physically unchanged and no action is required.

On a split-backend deployment, drain the in-process queue before upgrading: `_jobs/` records still pending on the blob backend become invisible to the new queue after the upgrade. Cache-fill jobs re-enqueue on the next pull, but an in-flight `event-only` replication push is not re-driven by `angos replicate`, so re-push affected tags if the queue was not drained. Leftover `_jobs/`, `.tx-log/`, `.tx-bodies/`, and `.tx-locks/` objects on the blob backend are inert and can be deleted manually.

---

## 1.3.0 → 1.3.1

### New Features Available in 1.3.1

#### S3 Capabilities Declaration

You can declare your S3 provider's conditional operation support upfront to skip the startup probe and enable performance optimizations:

```toml
[metadata_store.s3]
conditional_operations = true
```

See [Conditional Operations](../reference/configuration.md#conditional-operations-metadata_stores3conditional_operations) in the configuration reference for details on when to use this and which providers support it.

---

## 1.3.x → 1.4.0

### Blob-Index Layout Migration (Breaking Change, Data Loss Risk)

#### What Changed

The legacy single-file blob index (`.../<digest>/index.json`) is no longer read, written, or migrated at runtime. Blob references now live only in the sharded `refs/<namespace>.json` layout introduced in 1.2.0. The scrub action that migrated `index.json` files into shards is also removed.

**Who is affected:** Deployments upgraded from a pre-1.2.0 layout that never completed the migration. Any blob whose references still live only in an `index.json` file becomes unreferenced after upgrade, so the blob can be reclaimed by a later scrub and pulls of it fail.

#### Migration (run before upgrading)

On your **current version**, run a scrub once to migrate every legacy `index.json` into the sharded layout (the layout migration runs on any `angos scrub` invocation):

```text
angos scrub
```

Then upgrade. If you have already run `angos scrub` on 1.2.0 or later, no action is required; the migration is idempotent and your indexes are already sharded.

### Legacy Link Metadata (Breaking Change)

#### What Changed

Link files stored in the pre-JSON bare-digest format (a single digest string, as written by the upstream Docker `distribution` implementation) are no longer read at runtime. Angos writes link metadata as JSON with a `created_at` timestamp, and only that format is parsed by the serving paths now. A bare-digest link no longer resolves, and because a read precedes every write and delete, it cannot be re-pushed or removed through the API until it is rewritten.

**Who is affected:** Deployments seeded from a raw `distribution` on-disk layout whose links were never rewritten by angos (for example a tag that has not been pushed, retagged, or otherwise touched since the import). Native angos deployments write JSON links from the start and are unaffected.

#### Migration

After upgrading, run `angos migrate` to rewrite every bare-digest link as JSON:

```text
angos migrate
```

Run it before serving the affected repositories. The command is idempotent, so it is safe to re-run and leaves already-JSON links untouched; pass `--dry-run` to report what it would rewrite without changing anything. A migrated link is written without a `created_at`, so it never wins replication last-writer-wins and retention treats it as oldest.

### Manifest Media Type Backfill (Breaking Change)

#### What Changed

A manifest link records the manifest's `media_type`. The serving paths no longer read the manifest blob to recover a link that lacks one, and the `scrub --media-types` backfill that populated missing values is removed. A HEAD or GET of a manifest whose link has no stored `media_type` is now served without a `Content-Type`.

**Who is affected:** Deployments with manifest links written before `media_type` was stored that were never re-pushed or backfilled. Every push since then stores the `media_type`, so a registry that has run the backfill once is unaffected.

#### Migration (run before upgrading)

On your **current version**, run the backfill once to populate every missing `media_type` from the manifest body:

```text
angos scrub --media-types
```

Then upgrade. If you have already run it, no action is required; the backfill is idempotent.

### Manifest Push Policy Input (Breaking Change)

#### What Changed

A `put-manifest` action now exposes `request.digest` and `request.tags` to CEL access policies instead of `request.reference`. A by-digest push carries `request.digest`; the tags the push creates (the target tag of a by-tag push, or the `?tag=` parameters of a by-digest push) are carried in `request.tags`. The read and delete manifest actions still expose `request.reference`.

**Who is affected:** Deployments whose `access_policy` rules gate a manifest push on `request.reference`. The webhook authorization headers (`X-Registry-Reference`) are unchanged.

#### Migration

Rewrite any rule that matched a push on `request.reference` to use `request.digest` and/or `request.tags`, for example `has(request.digest)` for a by-digest push or `'latest' in request.tags` to match a created tag.

### Scrub and Prune Rework (Breaking Change)

#### What Changed

The maintenance commands were redesigned around a structure-vs-config split:

- `angos scrub` is now a single concurrent walk that always runs every structural check. **All selection flags are removed** (`--tags`, `--manifests`, `--blobs`, `--links`, `--reconcile-blob-index`, `--referrers`, `--uploads`, `--multipart`, `--orphan-grants`, `--orphan-namespaces`, `--replication-orphans`, `--cache-orphans`, and the deprecated `--retention`/`--replicate`); an invocation still passing one **fails to start**. Scrub deletes objects with unreadable content, moves keys matching no known angos layout to a `_lost_and_found/` prefix, and runs the transaction engine's janitor sweeps, which no longer run as background loops in the server and worker.
- `angos prune` now owns configuration-relative and time-based reclamation. **Orphan-namespace clearing and the orphan-job sweep always run**, and a single `-u/--uploads` window (default `1h`) gates upload sessions, orphan S3 multiparts, and byteless blob-index entries. Grant-only blob ownership is decided by the retention policies.

**Who is affected:** Every deployment with scheduled maintenance: cron jobs, systemd timers, Kubernetes CronJobs, and Compose profiles invoking scrub or prune with flags. Also any deployment that edits `[repository]` config: prune now clears namespaces no configured repository owns, without a flag.

#### Migration

Update every scheduled invocation before upgrading the maintenance schedule:

| Old invocation | Replacement |
| --- | --- |
| `scrub --tags --manifests --blobs` (any combination of check flags) | `scrub` |
| `scrub --uploads 1h` / `scrub --multipart 24h` | `prune` (default window `1h`) or `prune -u <dur>` |
| `scrub --orphan-grants 24h` | add a time-based retention rule, e.g. `image.pushed_at > now() - days(1)` |
| `scrub --orphan-namespaces` | `prune` (always on) |
| `scrub --replication-orphans` / `scrub --cache-orphans` | `prune` (always on) |
| `scrub --retention` / `scrub --replicate` | `angos prune` / `angos replicate` |

Then, on the upgraded version:

1. Run `angos scrub --dry-run` first and review the report, especially what would be quarantined; run scrub from the same angos version as the server fleet.
2. Run `angos prune --dry-run` and confirm the orphan-namespace deletions only cover namespaces you intend to drop; every namespace whose owning `[repository]` is no longer configured is now cleared by a plain `prune`.
3. Schedule both commands periodically: scrub also reclaims the transaction engine's garbage (orphaned staging bodies, expired lock objects), which serving processes no longer sweep on their own.

### Removed Configuration Keys (Breaking Change)

#### What Changed

Several long-deprecated configuration keys are no longer read. A configuration still using them now silently falls back to the default instead of the intended value.

| Removed key | Replacement |
| --- | --- |
| `global.enable_redirect` | `global.enable_blob_redirect` + `global.enable_manifest_redirect` (both default `true`) |
| `access_policy.default_allow` | `access_policy.default = "allow"` or `"deny"` |
| `cache_store` section | `cache` |
| `storage` section | `blob_store` |
| `[metadata_store.s3.capabilities]` table | `metadata_store.s3.conditional_operations` |

#### Migration

Rename each key in your configuration. Every replacement was accepted alongside the old key in earlier releases, so the rename is safe to apply on your current version before upgrading. Replace the `capabilities` table with `conditional_operations = true` only when all three of its booleans were `true`, otherwise `conditional_operations = false`; a leftover `capabilities` table is ignored and the registry probes the provider at startup.
