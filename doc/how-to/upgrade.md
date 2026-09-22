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

Update any clients of the old `/v2/_ext/...` endpoints to the new `/_ext/...` paths. When upgrading to 1.5.0 or later, skip this step and follow the [1.5.0 mapping table](#extension-api-moved-into-the-reserved-namespace-breaking-change) instead, since `/_ext/` no longer exists there.

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

On a split-backend deployment, drain the in-process queue before upgrading: `_jobs/` records still pending on the blob backend become invisible to the new queue after the upgrade. Cache-fill jobs re-enqueue on the next pull, but an in-flight `event-only` replication push is not re-driven by `angos reconcile replication`, so re-push affected tags if the queue was not drained. Leftover `_jobs/`, `.tx-log/`, `.tx-bodies/`, and `.tx-locks/` objects on the blob backend are inert and can be deleted manually.

---

## 1.3.0 → 1.3.1

### New Features Available in 1.3.1

#### S3 Capabilities Declaration

You can declare your S3 provider's conditional operation support upfront to skip the startup probe and enable performance optimizations:

```toml
[metadata_store.s3]
conditional_operations = true
```

> `conditional_operations` is ignored as of 1.6.0: there is no lock backend to
> probe for. See [Unknown Keys](../reference/configuration.md#unknown-keys).

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

> `angos migrate` was removed in 1.7.0, and 1.7.0 reads no link file at all. If
> you are upgrading past 1.6.x, run this step on 1.6.x or earlier and then
> `angos scrub`; see [1.6.x &rarr; 1.7.0](#16x--170).

After upgrading, run `angos migrate` to rewrite every bare-digest link as JSON:

```text
angos migrate
```

Run it before serving the affected repositories. The command is idempotent, so it is safe to re-run and leaves already-JSON links untouched; pass `--dry-run` to report what it would rewrite without changing anything. Each rewrite is committed against the link body it read, so a push landing while the run is in progress is kept rather than reverted. A migrated link is written without a `created_at`, so it never wins replication last-writer-wins and retention treats it as oldest.

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
| `scrub --retention` / `scrub --replicate` | `angos prune` / `angos reconcile replication` |

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

## 1.4.0 → 1.4.1

### Manifest Media Type Backfill

> `angos migrate` was removed in 1.7.0. If you are upgrading past 1.6.x, run
> this step on 1.6.x or earlier.

A manifest link records the `media_type` served as the `Content-Type` of a manifest HEAD or GET. A link written before `media_type` was stored, or rewritten by the 1.4.0 `angos migrate`, has none and is served without a `Content-Type`, which go-containerregistry clients such as kaniko reject. `angos migrate` now backfills it from the manifest body:

```text
angos migrate
```

Run it once after upgrading. The command is idempotent and leaves links that already carry a `media_type` untouched; native angos pushes have always stored it, so a registry that never imported a raw `distribution` layout needs no action.

---

## 1.4.5 → 1.5.0

### `[blob_store]` Is Required (Breaking Change)

#### What Changed

A configuration must name a blob store, and an `fs` backend whose `root_dir` is empty is refused. Both now fail startup with an error naming the section.

**Who is affected:** any deployment whose configuration omits `[blob_store]` entirely, or sets `root_dir = ""`.

Angos used to fall back to the filesystem backend rooted at the empty path, which resolves every object against the process working directory. In a container that is the ephemeral layer rather than the mounted volume, so the registry looked healthy and lost every pushed image when the pod restarted.

#### Migration

Name the storage explicitly. If you were relying on the old default, your data is under the directory the process was started from; move it to the path you configure here.

```toml
[blob_store.fs]
root_dir = "/var/lib/angos"
```

The metadata store still inherits this root unless `[metadata_store]` overrides it.

### OIDC Providers Are No Longer Typed (Breaking Change)

#### What Changed

`auth.oidc.<name>.provider` is gone. A provider is an issuer plus how its tokens are validated, so every entry takes the same options and nothing selects between provider types.

**Who is affected:** every deployment with an `[auth.oidc.*]` table, and any access policy reading `identity.oidc.provider_type`.

The `provider` key is now an unknown field, which TOML ignores rather than rejects. An entry that relied on the GitHub defaults therefore fails to load with `missing field 'issuer'` instead of naming `provider` as the cause.

#### Migrate a GitHub Actions Provider

**Before:**

```toml
[auth.oidc.github-actions]
provider = "github"
```

**After:**

```toml
[auth.oidc.github-actions]
issuer = "https://token.actions.githubusercontent.com"
jwks_uri = "https://token.actions.githubusercontent.com/.well-known/jwks"
required_claims = ["repository", "actor"]
```

`jwks_uri` is optional; without it the registry discovers the endpoint from the issuer. `required_claims` preserves the repository/actor check the GitHub provider performed on every token; drop it only if you want tokens missing those claims to reach your access policy.

#### Migrate a Generic Provider

Delete the `provider = "generic"` line. Nothing else changes.

### `identity.oidc.provider_type` Is Removed (Breaking Change)

It only ever held `"GitHub Actions"` or `"Generic OIDC"`, a distinction that no longer exists. Rewrite any policy rule using it to test `identity.oidc.provider_name`, which is the name of the `[auth.oidc.<name>]` entry that authenticated the token:

```toml
# Before
'identity.oidc != null && identity.oidc.provider_type == "GitHub Actions"'

# After
'identity.oidc != null && identity.oidc.provider_name == "github-actions"'
```

The field is also gone from the denial audit log, and from the payload of registry tokens issued by `auth.token_service`. Tokens minted before the upgrade stay valid: the extra field is ignored when they are validated.

### Cached JWKS Refetched Once

JWKS and discovery documents are now cached by issuer alone rather than by issuer and provider type. Existing cache entries are not read after the upgrade, so each issuer is fetched once more than usual on the first requests. No action is required.

### Extension API Moved Into the Reserved Namespace (Breaking Change)

The angos extension endpoints moved from the top-level `/_ext/` prefix into the extension namespace the distribution spec reserves, `/v2/_angos/`. The spec fixes the shape (`_<extension>/<component>/<module>`); `angos` is the extension name.

| Before | After |
| --- | --- |
| `GET /_ext/_repositories` | `GET /v2/_angos/repositories/list` |
| `GET /_ext/<repository>/_namespaces` | `GET /v2/_angos/namespaces/list?repository=<repository>` |
| `GET /_ext/<namespace>/_revisions` | `GET /v2/<namespace>/_angos/revisions/list` |
| `GET /_ext/<namespace>/_uploads` | `GET /v2/<namespace>/_angos/uploads/list` |
| `GET /_ext/_jobs` | `GET /v2/_angos/jobs/list` |
| `GET /_ext/_jobs/failed` | `GET /v2/_angos/jobs/failed` |
| `POST /_ext/_jobs/failed/<key>/retry` | `POST /v2/_angos/jobs/failed?key=<key>` |
| `DELETE /_ext/_jobs/<state>/<key>` | `DELETE /v2/_angos/jobs/<state>?key=<key>` |
| `GET /_ui/config` | `GET /v2/_angos/ui/config` |

Query parameters are unchanged. The web UI is served from `/` as before; only the API moved. Update any script or dashboard calling the old paths.

### Media Types Are Bare Outside Headers (Breaking Change)

A media type now carries no parameter section anywhere but a `Content-Type` header, where the spec has a registry ignore one. A `mediaType` or an `artifactType` naming parameters is refused instead of silently reduced to the type ahead of them.

#### Migration

None. A client sending parameters on a manifest `Content-Type` still has them ignored, as before. A media type an older angos recorded with its parameters, which it did when a push carried them and the body declared no `mediaType` of its own, is read bare on the way out, so stored content keeps serving and is rewritten bare on its next push.

---

## 1.5.x → 1.6.0

### Transaction Engine Removed

The internal transaction engine (its intent log, crash-recovery loop,
janitors, and lock backends) is gone: every write path now uses write-once
ordered keys, blob reclamation is fenced by the `v2/gc/` marker protocol, and
the durable queue serialises workers with atomically created claim keys.

The coordination keys that configured it, `lock_strategy` (with its
`redis`/`s3` sub-tables), a bare `[metadata_store.*.redis]` table, and
`conditional_operations`, are accepted and silently ignored, so existing
configs keep loading. Remove them at your convenience.

**Upgrade honesty:** recovery is gone, so a store carrying a previous
binary's mid-crash transaction is not replayed after the upgrade. Leftover
`.tx-log/`, `.tx-bodies/`, and `.tx-locks/` keys are reclaimed as garbage by
`angos scrub` once past the reclamation grace period, and any torn legacy
write surfaces as a scrub-repairable inconsistency the validators repair
from content. Upgrade from a cleanly stopped (or already-recovered) 1.5.x;
run `angos scrub` once after the upgrade.

### Reclaiming Space Now Requires Scrub

Both delete endpoints answer `202 Accepted` and leave the bytes in place:
a manifest or blob delete removes the metadata that references content, and
`angos scrub` is what reclaims the content itself. A deployment that deletes
regularly and never sweeps grows without bound, so schedule `angos scrub`
(see [Run storage maintenance](run-storage-maintenance.md)). Operators
watching disk immediately after a delete see the lag this introduces.

### New Knobs

`[global] gc_grace_secs` (default 300) is the reclamation grace period: scrub
leaves keys and bytes younger than it alone, so it can never race an
in-flight push. Raise it if pushes routinely stall longer than five minutes;
lower it only for offline maintenance against a store with no live traffic.

`[global.job_queue] claim_ttl_secs` (default 60) is the job-claim lease, and
bounds how quickly a crashed worker's jobs are taken over.

`access_time_debounce_secs` joins the ignored keys: access times are written
inline, one entry per stamped pull.

### A New Transient Response

A write racing an active reclamation answers `503 RECLAMATION_IN_PROGRESS`
with `Retry-After`, where previous versions blocked on a lock. Clients that
honour `Retry-After` (docker, containerd, oras) retry without intervention;
custom clients should treat it as retryable rather than fatal.

### Backend Requirements

The job queue prefers a backend whose create-if-absent is atomic and probes
for one at startup (`link(2)` on a filesystem, `If-None-Match: *` on S3). A
backend that cannot enforce it still runs, with a logged warning: claim races
may then execute an idempotent job more than once. Nothing else in the write
path needs a conditional write.

### No Action Needed

Upload sessions begun by an earlier version resume and complete unchanged,
their metadata rewritten to the current shape on the next chunk. Legacy tag,
revision, referrer, shard, and layer/config link shapes keep answering reads
until scrub converts them, and the catalog lists a namespace that predates
the index once scrub backfills its key.

---

## 1.6.x → 1.7.0

### Legacy Link Files Are No Longer Read (Breaking Change, Data Loss Risk)

#### What Changed

Tag `current/link`, revision, referrer, and layer/config/index-child link files
under `v2/repositories/<namespace>/` are no longer read, converted, or repaired.
Tags resolve from their entries, revisions and referrers from their records, and
every other reference from its key under `v2/ref/`. `v2/repositories/` now holds
upload sessions only; a leftover link file matches no known layout, so `angos
scrub` moves it to `_lost_and_found/`.

**Who is affected:** deployments upgraded from 1.5.x or earlier that never ran a
full `angos scrub` on 1.6.x. Any tag, revision or referrer still stored only as a
link file resolves as absent after the upgrade, and a blob pinned only by a
converted layer or config reference key is reclaimed by the next scrub.

#### Migration (run before upgrading)

On 1.6.x, run scrub to completion and let it finish without errors:

```text
angos scrub
```

Scrub converts every link file into the current shape and re-homes tracked pins
onto per-referrer reference keys. Then upgrade. A store already scrubbed on
1.6.x needs no action.

### `angos migrate` Is Removed (Breaking Change)

#### What Changed

The command rewrote pre-JSON bare-digest link files as JSON and backfilled a
manifest link's `media_type`. Both outputs are link files, which 1.7.0 no longer
reads, so the command can no longer repair anything.

**Who is affected:** deployments seeded from a raw Docker `distribution` on-disk
layout whose links were never rewritten by angos.

#### Migration (run before upgrading)

On 1.6.x, run `angos migrate` and then `angos scrub`, in that order:

```text
angos migrate
angos scrub
```

Migrate rewrites the bare-digest links as JSON, and scrub then converts them into
tag entries and revision records. After upgrading there is no way to recover a
link angos never rewrote.

### Blob-Index Shards Are No Longer Read (Breaking Change, Data Loss Risk)

#### What Changed

The per-namespace JSON shards at `v2/blobs/<alg>/<prefix>/<hash>/refs/<ns>.json`
are no longer read, merged into a blob's index, or converted. A blob's
references live only as keys under `v2/ref/`. A leftover shard matches no known
layout, so `angos scrub` quarantines it.

Scrub also stops walking `v2/blobs/` on the metadata store: with no shards to
convert, that pass has nothing to do, and the reference pass now covers
`v2/ref/` alone.

**Who is affected:** deployments upgraded from a pre-1.2.0 layout that never
completed a scrub on 1.6.x. A blob whose only reference is an unconverted shard
reads as unreferenced, so the next scrub reclaims its bytes.

#### Migration (run before upgrading)

On 1.6.x:

```text
angos scrub
```

Scrub converts every shard into per-link reference keys. This is the same run
the link-file migration above requires, so one scrub covers both.

### Legacy Access-Time Keys Are No Longer Read

#### What Changed

A tag's or revision's last pull comes from its append-only access entries under
`v2/ns/<ns>!atime/<target>!/`. The pre-1.6 single keys at
`v2/ns/<ns>!atime/tag/<tag>` and `v2/ns/<ns>!atime/rev/<alg>/<hash>` are no
longer read, and scrub no longer retires them; a leftover is quarantined.

**Who is affected:** nobody's data. Access times are advisory. A target whose
only record is a legacy key reports no last-pull time until its next pull, which
matters only for retention rules keyed on `last_pulled_at`: such a target reads
as never pulled and so as eligible under an age rule.

#### Migration

Optional. Running `angos scrub` on 1.6.x stamps nothing new, so the honest fix
is to let the next pull re-stamp. If you gate deletion on `last_pulled_at`,
widen the window for one retention cycle after upgrading.

### Legacy Upload Artifacts Are No Longer Read

#### What Changed

An upload session's durable state is its `session.json`. The pre-1.6 `startedat`
marker and per-offset `hashstates/<offset>` checkpoints are no longer read, so a
session carrying only those cannot resume: the next `PATCH` or `PUT` answers
`BLOB_UPLOAD_UNKNOWN`, and `angos prune` reaps the session on its next run
regardless of the `-u` window, since a session with no record can never
complete.

**Who is affected:** only uploads left in flight across the upgrade by a 1.5.x
or earlier binary. A session begun on 1.6.x already carries a `session.json`,
rewritten on its first chunk.

#### Migration

None. Let in-flight uploads drain before upgrading if you want to avoid the
failed resumes; clients re-push on `BLOB_UPLOAD_UNKNOWN`.

### Listings No Longer Walk the Legacy Tree

#### What Changed

The catalog, tag, revision and referrer listings resolve from `v2/ns/` and
`v2/cat/` alone. They no longer merge in a walk of
`v2/repositories/<namespace>/_manifests/`, and a namespace's content probe no
longer counts a legacy tag directory.

**Who is affected:** the same stores as the link-file change above, and in the
same direction. Content left unconverted already failed to resolve after that
change; now it stops appearing in listings too, so a tag no longer lists as a
name that 404s when pulled.

#### Migration (run before upgrading)

The `angos scrub` run the link-file section already requires. No separate step.

### Transaction-Engine Leftovers Are Quarantined, Not Reclaimed

`.tx-log/`, `.tx-bodies/` and `.tx-locks/` keys are no longer a shape angos
recognizes, so `angos scrub` moves any that survive to `_lost_and_found/`
instead of deleting them once past the grace period. A store scrubbed on 1.6.x
has none. If yours does, note that quarantine copies the object before removing
the original, and `.tx-bodies/` can hold large staged upload bodies: either run
`angos scrub` on 1.6.x first, or delete the three prefixes by hand.

---

## 1.7.x → 1.7.2

### `immutable_tags` Refuses Only an Overwrite

`immutable_tags` used to refuse every push naming a protected tag, including
the first one that created it and a re-push of the content it already held.
That contradicted its own how-to, and made a release tag impossible to publish
while the flag was on. A push is now refused only when the tag already exists
and points at a different digest.

**Who is affected:** deployments relying on the old behaviour as a blanket
"no pushes to these tags" rule. Creating a protected tag now succeeds, so a
workflow that expected a `409` for a first push gets a `201`.

#### Migration

None. To keep a tag unwritable altogether, deny the push at the authorization
layer instead: `immutable_tags` protects content, not the namespace.

## 1.7.x → 1.8.0

### The Container Image Runs Unprivileged (Breaking Change)

The image used to run `angos` as root. It now runs as UID and GID 65534, so
every file the registry reads or writes must be accessible to that user: the
data directory of a filesystem store, the TLS private key, and any client
certificate files it is given.

**Who is affected:** deployments that bind-mount a root-owned data directory
into the container, or mount a private key readable only by root. The
registry fails to start, or refuses every write, with a permission error.
S3-backed deployments and manifests that already set `runAsUser` are
unaffected.

#### Migration

Hand the data directory to the new user before starting the upgraded image:

```bash
sudo chown -R 65534:65534 /path/to/data
```

On Kubernetes, set `securityContext.fsGroup: 65534` on the pod so a mounted
volume is writable, or keep an explicit `runAsUser` that owns the volume. A
private key must be readable by UID 65534.

### A Referrer No Longer Pins Its Subject (Breaking Change)

`prune` used to keep any manifest carrying a referrer, so a signature, an SBOM
or a scan report held its image out of retention for as long as the referrer
itself survived, while the referrer was judged as untagged content of its own.
Retention now judges the image by the rules alone, skips its referrers while
it resolves, and reclaims them with it in the same run.

**Who is affected:** deployments relying on a signature or an attestation to
keep an untagged image out of retention. Such an image is deleted on the next
`prune` once no rule retains it.

#### Migration

Run `angos prune --dry-run` before the first run on the new version, and add
a rule retaining the images that were kept only by their referrers, such as an
age rule or a tag naming them.

### `angos replicate` Is Now `angos reconcile replication` (Breaking Change)

The replication reconciliation pass moved under a `reconcile` command that
also carries the new `reconcile scan`. Its options and behaviour are
unchanged.

**Who is affected:** any cron entry, runbook or pipeline invoking
`angos replicate`; it now fails with an unknown-subcommand error.

#### Migration

Replace `angos replicate` with `angos reconcile replication`.

## 1.8.0 → 1.9.0

### The Link Cache Is Gone

Tags, revisions and referrers were read through a cache bounded by
`link_cache_ttl`, so a replica could answer with a tag a peer had already
moved until the TTL elapsed. Every such read now goes to the metadata store,
and `link_cache_ttl` joins the ignored keys.

**Who is affected:** deployments that set `link_cache_ttl`, and multi-instance
deployments pointing the cache at a shared Redis so that replicas would agree.
The key is ignored rather than refused, so configurations keep loading. A
repeated tag or revision read now costs one backend round trip, which on S3
shows up as more `GET` requests against hot tags.

#### Migration

None. Remove `link_cache_ttl` at your convenience; the cache backend still
serves authentication, so a configured Redis stays in use.

### A Pull Angos Cannot Record Fails

A manifest pull served from a pull-through cache hit or as a presigned
redirect used to be served even when writing its access-time entry failed,
where a local pull already failed. All three now fail, since retention
reclaims by that record and serving content angos then treats as never pulled
would delete live images.

**Who is affected:** deployments with `update_pull_time` enabled whose
metadata store can reject writes while still serving reads, such as a bucket
at a quota. Those pulls answer `500` instead of being served unrecorded.

#### Migration

None. Disable `update_pull_time` if retention does not need last-pull times.

### Per-Role Reference Keys Are Quarantined

An angos before 1.8.0 recorded a referenced blob with one key per role,
`r/layer`, `r/config` or `r/idx.<algo>.<hash>` under the blob's reference
directory. Since 1.8.0 a push pins each referenced digest through the
referring manifest instead, and such a key has counted for nothing. It is now
not recognised at all, so `angos scrub` treats it like any key matching no
known layout: quarantined by default, deleted under `--delete-unknown`, and
counted either way.

**Who is affected:** stores that ran an angos before 1.8.0 and still hold
those keys. Read access and reclamation are unaffected, since the keys already
vouched for nothing; the next scrub run simply reports more quarantined keys
than before.

#### Migration

None. Run `angos scrub` to clear them, with `--delete-unknown` if you would
rather not keep the quarantine copies.

### `namespace_walk_concurrency` Must Be Non-Zero

A `0` used to be read as `1`. It is now refused when the configuration loads,
so a mistake fails loudly instead of quietly running the walks unparallelised.

**Who is affected:** deployments that set `namespace_walk_concurrency = 0`.
The server, and every offline command, exits with a configuration error.

#### Migration

Remove the setting to take the default of 128, or set it to at least 1.

---

## 1.9.x → 1.10.0

### `scan = true` Is Now a `scan` Table (Breaking Change)

A repository opts into vulnerability scanning with a `[repository."<name>".scan]`
table, which also carries its report refresh rules, in place of the
`scan = true` flag; the flag now fails to parse.

**Who is affected:** every deployment with `scan = true` on a repository; the
registry refuses the configuration at startup.

#### Migration

**Before:**

```toml
[repository."apps"]
scan = true
```

**After:**

```toml
[repository."apps".scan]
```
