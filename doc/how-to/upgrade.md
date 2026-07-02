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

#### S3 Capabilities Declaration

You can declare your S3 provider's conditional operation support upfront to skip the startup probe and enable performance optimizations:

```toml
[metadata_store.s3.capabilities]
put_if_none_match = true
put_if_match = true
delete_if_match = true
```

See [Conditional Capabilities](../reference/configuration.md#conditional-capabilities-metadata_stores3capabilities) in the configuration reference for details on when to use this and which providers support each capability.

---

## 1.1.x → 1.2.0

### Redirect Configuration Split

#### What Changed

`global.enable_redirect` has been deprecated and replaced by two separate flags:

- `global.enable_blob_redirect`: controls HTTP 307 redirects for blob (layer/config) downloads.
- `global.enable_manifest_redirect`: controls HTTP 307 redirects for manifest downloads.

Both default to `true`, preserving historical behavior. The old `enable_redirect` field is still accepted as a fallback for both new flags and will emit a deprecation warning at startup.

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

**If you want to keep using `enable_redirect`**, no immediate action is required. The field continues to work as a fallback but will print a warning at startup. Update to the new fields at your convenience.

### Extension API Path Change (Breaking Change)

#### What Changed

The angos extension API moved from the `/v2/_ext/...` prefix to the top-level `/_ext/...` prefix, so `/v2` is reserved for the OCI Distribution API.

#### Migration

Update any clients of the old `/v2/_ext/...` endpoints to the new `/_ext/...` paths. See the [Extension API](../reference/api-endpoints.md#extension-api-not-part-of-the-oci-specification) reference for the full endpoint list.

---

## 1.2.x → 1.3.0

### Durable Queue Shared Lock (Breaking Change)

#### What Changed

A configuration that declares `[global.job_queue]` while the metadata store uses the default in-process `memory` lock strategy is now rejected at process boot (during config load and validation). The check fires only when `[global.job_queue]` is present; configurations without it (the in-process queue) are unaffected.

**Who is affected:** Deployments that enabled the durable out-of-process queue with `[global.job_queue]` but left `lock_strategy` at its default. `memory` is the default lock, so a 1.2.0 durable-queue config that omitted `lock_strategy` booted on 1.2.0 (which had no such check) but fails to boot after upgrade. The boot failure reports:

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

**Who is affected:** No one needs to act. Pre-existing namespace-registry index objects (`_registry/namespaces.json` and `_registry/ns/*.json`) written by earlier versions are no longer read or written, and a committed scrub (`scrub --commit`) prunes them automatically; a report-only run leaves them in place.

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

---

## 1.3.x → 1.4.0

### Scrub Redesigned to Rebuild-and-Sweep

#### What Changed

`scrub` is now a single **rebuild-and-sweep** operation with behavior changes operators must know:

- **Report-only by default.** `scrub` classifies everything but performs no storage mutation at all without the new `--commit` gate; its only report-only write is the best-effort `_scrub-audit/latest.json` run marker. The sweep deletions, the rebuild's additive repairs, the legacy blob-index convergence, and the `_registry/` prune all land only under `--commit`. A scheduled job that previously deleted (or relied on scrub's repairs) must now pass `--commit`.
- **The per-checker flags are removed.** The work each one did is folded into the unconditional per-revision rebuild (links, blob-index grants, referrer back-links, `media_type`) and the raw-enumeration sweep (orphan blobs, stale and bare grants, orphan links, missing-body revisions, de-configured namespaces, unrecognized keys). Passing a removed flag is now an argument-parsing error, so an unedited 1.3 scheduled job fails at startup instead of running with different semantics.
- **Every garbage-collection reap is age-gated on the new maintenance grace** (`[global] maintenance_grace_secs`, default 48 hours): a candidate is deleted only once its backend `last_modified` is older than the grace, and an object without a `last_modified` is never reaped. A bare blob-ownership self-grant (a blob a namespace owns with no link referencing it) is revoked once past the grace: what 1.3 did only under the opt-in `--orphan-grants <age>` flag now happens on every `--commit` run. An explicit blob DELETE still reclaims immediately.
- **New exit codes.** `scrub` now exits `2` (degraded) and `3` (aborted) in addition to `0`/`1`. Schedulers treat any non-zero code as a failure: a systemd oneshot unit reports the run failed, and a Kubernetes CronJob with `restartPolicy: OnFailure` re-runs the pod. Re-running a degraded or aborted scrub is safe.
- **One maintenance lock.** Every `scrub` run and every mutating `policy`/`replication` run takes the single `maintenance:registry` lock; a concurrent maintenance command refuses with exit 1. Any mutating run is refused on the `memory` lock strategy. The new `[global] maintenance_lock_max_hold_secs` knob (default 24 hours) caps one run's hold; size it above your worst-case scrub duration.

#### Migration

Update scheduled scrub jobs:

- Remove `-m` / `--manifests`, `-l` / `--links`, `-M` / `--media-types`, `-t` / `--tags`, `-R` / `--referrers`, `-n` / `--orphan-namespaces`, `-b` / `--blobs`, `--orphan-grants`, and `--reconcile-blob-index` from every invocation: they now fail argument parsing. The rebuild and sweep do their work automatically.
- Replace `-d` / `--dry-run` with the report-only default (omit all flags). Both write nothing to registry state (the only write is the best-effort `_scrub-audit/latest.json` run marker); the alias is deprecated and mutually exclusive with `--commit`.
- Add `--commit` to any job that must actually delete. A mutating run is refused when the metadata store's `lock_strategy` is `memory`; set it to `s3` or `redis`, or keep the job report-only.
- Adjust alerting for the new exit codes: systemd and Kubernetes treat `2` (degraded) and `3` (aborted) as failures, so a completed-but-degraded run pages like a crash unless your monitoring reads the `_scrub-audit/latest.json` marker or the logs.
- Retention (`-r` / `--retention`) and replication (`--replicate`) still work but are deprecated aliases for `angos policy` and `angos replication`.

**Complete the rolling upgrade before the first committed scrub.** Run `angos scrub --commit` only after every replica runs 1.4.0 and no 1.3 scrub can still start: the 1.4 sweep's safety model (maintenance lock, grace, per-blob push locks) assumes no 1.3 maintenance process is mutating the same storage. Report-only runs are safe at any point.

The removed per-checker flags map onto the unified passes as follows:

| Old flag | Status | Replacement |
|----------|--------|-------------|
| `-m, --manifests`         | removed (parse error) | unified rebuild (re-derives manifest links, runs unconditionally) |
| `-l, --links`             | removed (parse error) | rebuild (referrer back-links) plus sweep (phantom/orphan link removal) |
| `-M, --media-types`       | removed (parse error) | rebuild (`media_type` re-derivation) |
| `-t, --tags`              | removed (parse error) | sweep missing-body deletion; invalid-tag cleanup runs unconditionally |
| `-R, --referrers`         | removed (parse error) | sweep orphan-referrer removal |
| `-n, --orphan-namespaces` | removed (parse error) | sweep config-ownership deletion (report-only, then `--commit`) |
| `-b, --blobs`             | removed (parse error) | sweep orphan-blob byte reap plus stale tracked-grant purge |
| `--orphan-grants`         | removed (parse error) | sweep bare self-grant revocation after the maintenance grace |
| `--reconcile-blob-index`  | removed (parse error) | rebuild grant re-derivation |
| `-d, --dry-run`           | deprecated (mutates no registry state) | the report-only default (omit flags); `--commit` to delete |
| `-r, --retention`         | deprecated (still works) | `angos policy --retention` |
| `--replicate`             | deprecated (still works) | `angos replication` |

#### Converge the Blob Index Fleet-Wide

Legacy single-file blob indexes (`index.json`) keep working at runtime, and every committed scrub (`--commit`) automatically drains them into the sharded `refs/{namespace}.json` layout (a report-only run counts them without rewriting). After upgrading, **run `scrub --commit` once across every replica in your fleet** so the whole registry converges; preview with a report-only run first.

#### Deferred: Legacy Blob-Index Read Fallback Removal

The runtime still keeps the legacy single-file blob-index **read fallback**: a blob whose index has not yet converged is still served from its legacy `index.json`, so a manifest referenced only through legacy state stays readable. This fallback is **not removed in 1.4.0**.

A future release will drop the read fallback, but only after fleet-wide convergence is verified. Before that release, run `scrub` across the fleet (as above) so no blob is left in legacy-only form. Until then no action is forced beyond running scrub to converge.
