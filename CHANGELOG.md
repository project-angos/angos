# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Bi-directional replication mirrors manifest pushes and deletes to per-repository downstreams over the durable job queue, with on-demand reconciliation via `scrub --replicate`.
- Cross-repository blob mount (`POST /v2/{namespace}/blobs/uploads/?mount={digest}[&from={repository}]`) grants an already-present blob to the target namespace with no upload, which replication uses to skip re-uploading blobs a downstream already holds.
- The `_jobs` admin API accepts `?queue=cache|replication` (default `cache`), so failed replication jobs can be listed, retried, and deleted like cache jobs.

### Changed

- A blob-upload `POST` carrying `?mount=<digest>` is now authorized as a distinct `mount-blob` action, so a default-deny access policy must grant `mount-blob` for cross-repo mounts that previously evaluated as `start-upload`.
- `angos worker` with no `--queue` now drains both the `cache` and `replication` queues, so pass `--queue cache` to restore the previous cache-only default.
- `angos worker` now rejects an unknown `--queue` value at startup.
- A `[global.job_queue]` configured with the in-process `memory` lock is now rejected at startup, breaking for previously-running configs that relied on the default lock: set the metadata store's `lock_strategy` to `s3` or `redis`, or remove `[global.job_queue]` to use the in-process queue.
- A blob-upload `POST` with a malformed `?digest=` now returns `400` instead of silently starting a `202` upload session that ignores the digest.

## 1.2.0 - 2026-06-03

### Added

- Stale lock objects under `.tx-locks/` are reclaimed automatically by a periodic janitor running on every server and worker replica; no operator action is required.
- New `scrub --referrers` flag: checks every revision in each namespace and removes any referrer link whose referrer manifest no longer has a current digest revision link, preventing ghost descriptors from appearing in the OCI Referrers API response.
- Durable cache jobs: the optional `[global.job_queue]` section persists pull-through cache-fill jobs in the `[metadata_store]` backend so they survive restarts, drained by the new `angos worker` subcommand.
- Maximum manifest body size enforcement.
- Warning log when a listener flips between insecure and TLS during configuration hot-reload.

### Changed

- Blob upload sessions now use a single resumable streaming upload in place of the previous multipart-based protocol, and in-flight sessions do not survive the upgrade (clients retry and `scrub --uploads` reaps the stale staging artifacts).
- The angos extension API — the `_repositories`, `_namespaces`, `_revisions`, and `_uploads` discovery endpoints, plus the new `_jobs` admin endpoints moved from the `/v2/_ext/...` prefix to the top-level `/_ext/...` prefix, so `/v2` now serves only OCI Distribution Spec routes. This is breaking for clients of the `/v2/_ext/...` endpoints shipped in v1.1.1; update them to the `/_ext/...` paths.
- The four registry subsystems (metadata, job, upload, and manifest stores) now write atomically through a single transactional-engine design instantiated per subsystem, each with its own lock domain (metadata and job share one instance; the blob store and the in-process job queue each have their own), adding three new top-level prefixes — `.tx-log/`, `.tx-bodies/`, and `.tx-locks/`, that operators should factor into bucket policies. A `_jobs/` prefix is also added when the durable job queue (`[global.job_queue]`) is enabled.
- Blob deletion follows the OCI distribution lifecycle, with blob ownership tracked independently from manifest references.
- Manifests with missing blob or descriptor references are rejected at push time.
- Stricter OCI semantics for blob range requests and request-header parsing.
- Pull-through cached blobs are indexed, so they participate in reachability checks and garbage collection.
- Upstream blob failures and 404s return `BLOB_UNKNOWN` instead of `MANIFEST_UNKNOWN`.
- OCI digest validation rejects uppercase algorithm and hex characters.
- Server errors return the OCI `INTERNAL_ERROR` code in JSON responses.
- Non-boolean CEL access-policy rule results are fail-closed in both `allow` and `deny` modes.
- Access-policy CEL runtime evaluation errors are now fail-closed: a DENY rule that throws at request time denies the request instead of falling through to default-allow.
- Empty or whitespace-only CEL rules now fail with a clear configuration error at load time instead of panicking the process.
- Configuration is fully validated at load time (URLs, Redis cache URLs, Argon2 hashes, CEL rules, sampling rates, webhook refs, regexes, listener timeouts); invalid values now fail at startup with a clear error instead of later at runtime.- OIDC providers are tried in deterministic order, and the `mTLS` `auth_method` label is preserved when basic auth also succeeds.
- Auth-webhook transport failures surface as errors instead of silent denials.
- Webhook authorization responses with status 429 or 5xx no longer pin denials in the decision cache; only explicit 2xx and 401/403 outcomes are cached, and unavailable responses re-probe the webhook on the next request.
- OIDC authentication debug logs no longer include the full token claims map; only provider name/type and the `sub`/`iss` claims are logged to avoid leaking user/CI metadata at `debug` level.
- TLS listener configuration now supports an explicit `client_auth = "optional" | "required"` setting (default `"optional"` when `client_ca_bundle` is set, preserving existing behavior); operators can now enforce mTLS at the TLS handshake with `client_auth = "required"`. Configs without `client_ca_bundle` continue to load unchanged.
- Outbound `[registry_client]` blocks with a partial mTLS pair (`client_certificate` set without `client_private_key`, or vice versa) now fail at configuration load instead of silently disabling outbound client authentication.
- Webhook retries are capped at 16 with saturating backoff arithmetic.
- The S3 shard-key hash uses SHA-256, so shard placement is stable across processes and builds.
- Filesystem metadata now uses the same sharded blob-index and namespace-registry layout as S3, while pre-existing legacy `index.json` and `namespace_registry.json` files keep working and only `scrub` migrates them to the sharded layout.
- The Redis client reuses a multiplexed connection across operations and backs off on lock contention.
- The S3 backend now uses a custom HTTP client in place of the AWS SDK: focused retry / timeout / signing tailored to the operations the registry actually issues, smaller binary, and no AWS SDK transitive dependencies.
- Filesystem cleanup of empty ancestor directories walks up from a deleted leaf, removing empty parents until the first non-empty directory or the store root, and is rooted-subtree guarded; a misshaped path can never walk above the configured root.
- UI, documentation-website and Rust dependencies upgraded.
- Scrub's orphan manifest deletion now plans link operations through the runtime write path's `link_plan::delete` primitive, so it also removes any tag links still pointing at an orphaned manifest digest (previously left dangling).
- Scrub's orphan manifest deletion now tolerates manifest blobs that fail to parse by removing the digest link (and any tags pointing at it) while still surfacing blob-read failures so operators notice broken storage.
- Overlapping repository prefixes (e.g. `team` and `team/app`) are rejected at startup rather than resolved non-deterministically at runtime.
- `fs::BlobStore::read` on a missing blob now returns `BlobNotFound` (previously `ReferenceNotFound`), aligning the FS backend with S3.
- The six S3 connection fields duplicated across `blob_store`, `metadata_store`, and `job_store` are now defined once in `registry::s3_connection::S3ConnectionConfig`, and `[blob_store.s3]` now requires the `region` key.

#### Performance

- Uniform S3 object writes are streamed end-to-end through S3's multipart API; large-blob copies use S3 multipart copy.
- Blob-index lock contention reduced; ownership checks and GC cleanup no longer read the full index, and redundant writes are skipped.
- Filesystem metadata link locks now include namespace, reducing unrelated repository contention while blob-index locks stay digest-global across metadata backends.
- Existing blob data is reused on upload completion; empty / zero-byte rewrites are skipped on both uniform and nonuniform paths.
- Lower per-frame overhead in upload streaming.

### Deprecated

- The boolean `default_allow` flag is now an alias for the typed `AccessMode` setting and will be removed in a future release.
- The `cache_store` and `storage` configuration keys are legacy aliases.

### Removed

- Deprecated fields removed from `config.example.toml`.

### Fixed

- Transactional-engine recovery is now race-free against the original owner and idempotent on replay, so a crashed write picked up by another replica cannot collide with the original or re-apply mutations that already landed.
- Scrub now flushes the metadata store's access-time buffer at exit so retention timestamps are persisted across runs on the S3 backend.
- Scrub `--links` now prunes stale `referenced_by` entries whose revision no longer exists and removes the link and its blob-index entry when the set becomes empty, reclaiming storage.
- Scrub `--links` and `--media-types` now remove revision links whose manifest blob is permanently missing, cascading a `DeleteOrphanManifest` action to drop the digest link and every tag pointing at it while still propagating transient storage errors.
- Scrub `--tags` now removes tags whose target manifest blob is missing, cascading a `DeleteOrphanManifest` action per digest to drop the digest revision link and every tag link pointing at it.
- Scrub `-r` no longer aborts the whole namespace when one orphan manifest's blob is missing or unreadable: absent blobs are handled gracefully, transient errors retry next run, and a failure on one revision no longer blocks the rest.
- Scrub orphan-blob deletion now acquires the blob-data lock and re-checks ownership before deleting, preventing a concurrent upload from losing its bytes when scrub classifies the blob as orphan.
- Scrub on S3 now converges namespaces whose only artifact is an upload session, making them visible to `UploadChecker` so their stale bytes are cleaned up by `scrub --uploads`.
- `scrub --uploads` now also aborts the in-flight S3 multipart upload recorded in a stale session, so an abandoned upload no longer leaves behind an orphan multipart.
- Scrub `-b` now purges all blob-index entries (including ownership markers) for any blob whose backing bytes are absent, so runtime `can_read` no longer reports such blobs as accessible.
- Scrub `-m` now validates that each revision's digest link points back at its own digest and rewrites it on mismatch, repairing a corrupt link even when its blob is unreachable.
- Scrub's orphan manifest deletion now uses `delete_with_referrer` for tracked links (config, layer, child-manifest), preventing spurious link removal for blobs still referenced by other manifests.
- Hardened S3 uploads and blob-index locking against contention and partial-failure scenarios.
- Failed upload cleanups no longer abort the client request; orphaned S3 probe objects are surfaced via warning instead of silently leaking.
- Multipart uploads and parts are aborted on drop or panic, so failed uploads don't leave orphans on S3.
- Repository initialization no longer blocks the async runtime, fixing startup stalls on registries with many repositories.
- Upstream bearer tokens: per-upstream cache scope, serialized refreshes, TTL cache, and URL-encoded query parameters.
- The registry client fails with a clear error on missing upstream host authority instead of falling back to an `"unknown"` cache key.
- `rustls` is initialized before custom CA bundles so configured roots are honored, and the `TlsAcceptor` is cloned out of its `ArcSwap` guard before the handshake, fixing a race during certificate hot-reload.
- Shutdown logs `SIGTERM` registration failures (falling back to ctrl-c) and shuts the tracer provider down gracefully.
- Registry-initialization errors are preserved and surfaced to the operator.
- Error responses no longer panic the handler; a 500 is always produced.
- A poisoned capability-cache mutex is recovered from instead of crashing.
- Circuit-breaker race fixed: the breaker no longer occasionally stays open after recovery.
- Authentication: invalid basic credentials rejected, OIDC algorithm allowlist enforced, missing OIDC claims return `Unauthorized`, OIDC fetch failures return 503, JWKs refresh on unknown `kid`, and webhook auth configs validated at construction.
- Webhook async deliveries abort cleanly on shutdown timeout.
- Valkey `contrib/kubernetes` manifest: corrected an inverted `securityContext`.
- Documentation: fixed read-only example snippets.

### Security

- Webhook, S3 and basic-auth secrets wrapped in a `Secret` type to prevent leaks via debug logging.
- OIDC claims redacted from authorization-denial logs; webhook cache keys hashed.
- Argon2id parameters pinned explicitly to OWASP-recommended values.
- Several dependency vulnerabilities addressed.

## [1.1.1] - 2026-04-09

See the corresponding GitHub release notes.

[Unreleased]: https://github.com/project-angos/angos/compare/v1.1.1...HEAD
[1.1.1]: https://github.com/project-angos/angos/releases/tag/v1.1.1
