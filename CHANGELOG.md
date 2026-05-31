# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.2.0 [Unreleased]

### Added

- Stale lock objects under `.tx-locks/` are reclaimed automatically by a periodic janitor running on every server and worker replica; no operator action is required.
- New `scrub --referrers` flag: checks every revision in each namespace and removes any referrer link whose referrer manifest no longer has a current digest revision link, preventing ghost descriptors from appearing in the OCI Referrers API response.
- Durable cache jobs: optional `[global.job_queue]` section routes pull-through cache-fill jobs through a persistent filesystem or S3 store (per-key leases with heartbeat, exponential backoff, dead-letter on max-attempts). `angos server` enqueues jobs and publishes the `angos_job_queue_pending` gauge on `/metrics`; the new `angos worker` subcommand drains the queue. When the section is omitted the existing in-process `TaskQueue` is used unchanged. Documented in `doc/how-to/durable-cache-jobs.md` with a commented block in `config.example.toml`.
- Maximum manifest body size enforcement.
- Warning log when a listener flips between insecure and TLS during configuration hot-reload.

### Fixed

- Transactional-engine recovery is now race-free against the original owner and idempotent on replay, so a crashed write that another replica picks up cannot collide with the original or re-apply mutations that already landed.
- Scrub now flushes the metadata store's access-time buffer at exit so retention timestamps are persisted across runs on the S3 backend.
- Scrub `--links` now prunes stale entries in a tracked link's `referenced_by` set whose corresponding revision no longer exists, preventing phantom parent digests from accumulating indefinitely. When removing the last referrer causes `referenced_by` to become empty the link and its blob-index entry are also removed, reclaiming storage.
- Scrub `--links` and `--media-types` now remove revision links whose underlying manifest blob is permanently missing from storage, rather than silently skipping them on every run. A `DeleteOrphanManifest` action cascades to remove the digest link and every tag pointing at that digest; transient storage errors are still propagated so operators notice backend failures.
- Scrub `--tags` now removes tags whose target manifest blob is missing from storage, rather than silently converging them to self-consistent but permanently broken metadata. A single `DeleteOrphanManifest` action per affected digest cascades to remove both the digest revision link and every tag link pointing at the same digest.
- Scrub `-r` no longer aborts the entire namespace when one orphan manifest's blob is missing or unreadable: a legitimately absent blob is handled gracefully (metadata links are still removed), a transient storage error retries on the next run, and a failure on any single revision no longer prevents subsequent revisions in the same namespace from being processed.
- Scrub orphan-blob deletion now acquires the blob-data lock and re-checks ownership before deleting, preventing a concurrent upload from losing its bytes when scrub classifies the blob as orphan between the upload hashing step and the ownership grant.
- Scrub on S3 now converges namespaces whose only artifact is an upload session (e.g. a client that crashed mid-push): the namespace registry tree walk includes any namespace that has an underscore-prefixed child, matching filesystem backend behaviour. Such namespaces are now visible to `UploadChecker` and their stale bytes are cleaned up by `scrub --uploads`.
- As part of the upload-session rework, `scrub --uploads` now also aborts the in-flight S3 multipart upload recorded in a stale session — not just the metadata-layer session artifacts — so an abandoned upload no longer leaves behind an orphan multipart that only `scrub --multipart` could reap.
- Scrub `-b` now purges all blob-index entries (including ownership markers) for any blob whose backing bytes are absent, so runtime `can_read` no longer reports such blobs as accessible to clients.
- Scrub `-m` now validates that each revision's digest link points back at the revision's own digest, and rewrites the link when a mismatch is found. The check runs before the manifest blob is read, so a corrupt link is repaired even when its blob is unreachable.

### Changed

- The storage layer now uses a single resumable streaming upload capability in place of the previous multipart-based one; the state at rest is unchanged in layout. The `multipart_part_size` and `multipart_uniform_parts` keys keep their existing positions under `[blob_store.s3]`. In-flight upload sessions do not survive the upgrade — clients retry, and `scrub --uploads` reaps the stale staging artifacts.
- The four registry subsystems (metadata, job, upload, and manifest stores) now execute their multi-step writes through a single transactional engine. The on-disk layout under existing key families is unchanged, but three new top-level prefixes appear in the configured storage — `.tx-log/`, `.tx-bodies/`, and `.tx-locks/` — which operators should factor into bucket policies and lifecycle rules. A process crash mid-write either commits fully or rolls back fully, eliminating the "blob landed, links failed" orphan class; recovery and cleanup of intent records run automatically on every server and worker replica. See `doc/aip/transactional-engine.md` for the design.
- The `[global.job_queue.s3]` `prefix` key was renamed to `key_prefix` for consistency with the other S3 configurations (`[blob_store.s3]`, `[metadata_store.s3]`). Operators upgrading from a configuration that uses the old `prefix` key must rename the field; otherwise the configured prefix is silently dropped and durable jobs land at the bucket root.
- The `[global.job_queue]` `default_lease_ttl_secs` key has been removed; operators upgrading from a configuration that still sets it must drop the line. The per-`lock_key` execution lock TTL is now governed by the configured lock backend's settings (e.g. `S3LockConfig.ttl_secs` when `lock_strategy = "s3"`), so there is no longer a job-queue-level TTL knob.
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
- Configuration is fully validated at load time (URLs, Redis cache URLs, Argon2 hashes, CEL rules, sampling rates, webhook refs, regexes, listener timeouts); invalid values now fail at startup with a clear error instead of later at runtime.
- OIDC providers are tried in deterministic order, and the `mTLS` `auth_method` label is preserved when basic auth also succeeds.
- Auth-webhook transport failures surface as errors instead of silent denials.
- Webhook authorization responses with status 429 or 5xx no longer pin denials in the decision cache; only explicit 2xx and 401/403 outcomes are cached, and unavailable responses re-probe the webhook on the next request.
- OIDC authentication debug logs no longer include the full token claims map; only provider name/type and the `sub`/`iss` claims are logged to avoid leaking user/CI metadata at `debug` level.
- TLS listener configuration now supports an explicit `client_auth = "optional" | "required"` setting (default `"optional"` when `client_ca_bundle` is set, preserving existing behavior); operators can now enforce mTLS at the TLS handshake with `client_auth = "required"`. Configs without `client_ca_bundle` continue to load unchanged.
- Outbound `[registry_client]` blocks with a partial mTLS pair (`client_certificate` set without `client_private_key`, or vice versa) now fail at configuration load instead of silently disabling outbound client authentication.
- Webhook retries are capped at 16 with saturating backoff arithmetic.
- The S3 shard-key hash uses SHA-256, so shard placement is stable across processes and builds.
- Filesystem metadata now uses the same sharded blob-index and namespace-registry layout as S3 for newly written data. Pre-existing legacy `index.json` and `namespace_registry.json` files keep working: runtime reads consult the sharded layout first and fall back to the legacy file, and writes are applied in place to a legacy file when one is present so the layout does not split mid-blob. There is no auto-migration on the hot path; only `scrub` (and only `scrub`) converts legacy data into the sharded layout and deletes the legacy file afterwards.
- The Redis client reuses a multiplexed connection across operations and backs off on lock contention.
- The S3 backend now uses a custom HTTP client in place of the AWS SDK: focused retry / timeout / signing tailored to the operations the registry actually issues, smaller binary, and no AWS SDK transitive dependencies.
- Filesystem cleanup of empty ancestor directories is depth-bounded (2 levels for blob shards, 3 for index and namespace-registry shards, 4 for link containers) and rooted-subtree guarded; a misshaped path can never walk above the configured root.
- UI, documentation-website and Rust dependencies upgraded.
- Scrub's orphan manifest deletion now plans link operations through the same `link_plan::delete` primitive used by the runtime write path, eliminating divergence between scrub and runtime decisions about which links to remove. As a consequence, scrub also removes any tag links still pointing at an orphaned manifest digest (previously left dangling).
- Scrub's orphan manifest deletion tolerates manifest blobs that fail to parse: it removes the digest link (and any tags pointing at it) so the next pass can clean up the dangling config/layer/child links. Blob-read failures are still surfaced so operators notice broken storage.
- Overlapping repository prefixes (e.g. `team` and `team/app`) are rejected at startup rather than resolved non-deterministically at runtime.
- `blob_store` backends (S3 and FS) now route all storage primitives through the shared `angos-storage` capability traits instead of calling `angos-s3-client` and `tokio::fs` directly. On-disk layout is unchanged. `fs::BlobStore::read` on a missing blob now consistently returns `BlobNotFound` (previously `ReferenceNotFound`), aligning FS with the S3 backend and with `size`/`reader` on the same backend.
- The cached `S3UploadState.parts` wire format (cache key `upload_state:<ns>:<uuid>`) switched from `angos_s3_client::UploadedPart` to `angos_storage::Part`, renaming `e_tag` to `etag` (serialized transparently as a plain string). Pre-upgrade entries using the `e_tag` key deserialize transparently via a serde alias, so the upgrade is wire-compatible; the `list_parts` re-discovery fallback only triggers on genuinely corrupt cache entries.
- The six S3 connection fields (`access_key_id`, `secret_key`, `endpoint`, `bucket`, `region`, `key_prefix`) that were duplicated across `blob_store`, `metadata_store`, and `job_store` S3 configurations are now defined once in `registry::s3_connection::S3ConnectionConfig`. Credentials are wrapped in `Secret<String>` in all three module configs; the `blob_store` S3 path was the only one that previously stored credentials as plain `String` (via embedding `angos_s3_client::BackendConfig` directly), which meant debug output could expose them. The operator-visible TOML shape under `[blob_store.s3]`, `[metadata_store.s3]`, and `[global.job_queue.s3]` is unchanged, with one alignment: `[blob_store.s3]` now consistently requires the `region` key (matching the documented schema and the existing `[metadata_store.s3]` behaviour). Previously a missing `region` silently fell back to an empty string, which would then fail at runtime when the S3 client tried to sign a request.

#### Performance

- Uniform S3 multipart uploads are streamed end-to-end; large-blob copies use S3 multipart copy.
- Blob-index lock contention reduced; ownership checks and GC cleanup no longer read the full index, and redundant writes are skipped.
- Filesystem metadata link locks now include namespace, reducing unrelated repository contention while blob-index locks stay digest-global across metadata backends.
- Existing blob data is reused on upload completion; empty / zero-byte rewrites are skipped on both uniform and nonuniform paths.
- Lower per-frame overhead in upload streaming.

### Deprecated

- The boolean `default_allow` flag is now an alias for the typed `AccessMode` setting and will be removed in a future release.
- The `cache_store` and `storage` configuration keys are legacy aliases.

### Removed

- Deprecated fields removed from `config.example.toml`.
- The `lease_*` keys under `[global.job_queue]` are gone along with the legacy lease subsystem they configured.

### Fixed

- Scrub's orphan manifest deletion now uses `delete_with_referrer` for tracked links (config, layer, child-manifest), preventing spurious link removal for blobs still referenced by other manifests.
- Hardened S3 uploads and blob-index locking against contention and partial-failure scenarios.
- Failed upload cleanups no longer abort the client request; orphaned S3 probe objects are surfaced via warning instead of silently leaking.
- Multipart uploads and parts are aborted on drop or panic, so failed uploads don't leave orphans on S3.
- Repository initialization no longer blocks the async runtime, fixing startup stalls on registries with many repositories.
- Upstream bearer tokens: per-upstream cache scope, serialized refreshes, TTL cache, and URL-encoded query parameters.
- The registry client fails with a clear error on missing upstream host authority instead of falling back to an `"unknown"` cache key.
- `rustls` is initialized before custom CA bundles, so configured roots are honored. The `TlsAcceptor` is cloned out of its `ArcSwap` guard before the handshake, fixing a race during certificate hot-reload.
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
