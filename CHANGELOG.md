# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- New `scrub --referrers` flag: checks every revision in each namespace and removes any referrer link whose referrer manifest no longer has a current digest revision link, preventing ghost descriptors from appearing in the OCI Referrers API response.

### Fixed

- Scrub `--links` now prunes stale entries in a tracked link's `referenced_by` set whose corresponding revision no longer exists, preventing phantom parent digests from accumulating indefinitely. When removing the last referrer causes `referenced_by` to become empty the link and its blob-index entry are also removed, reclaiming storage.
- Scrub `--links` and `--media-types` now remove revision links whose underlying manifest blob is permanently missing from storage, rather than silently skipping them on every run. A `DeleteOrphanManifest` action cascades to remove the digest link and every tag pointing at that digest; transient storage errors are still propagated so operators notice backend failures.
- Scrub `--tags` now removes tags whose target manifest blob is missing from storage, rather than silently converging them to self-consistent but permanently broken metadata. A single `DeleteOrphanManifest` action per affected digest cascades to remove both the digest revision link and every tag link pointing at the same digest.
- Scrub `-r` no longer aborts the entire namespace when one orphan manifest's blob is missing or unreadable: a legitimately absent blob is handled gracefully (metadata links are still removed), a transient storage error retries on the next run, and a failure on any single revision no longer prevents subsequent revisions in the same namespace from being processed.
- Scrub orphan-blob deletion now acquires the blob-data lock and re-checks ownership before deleting, preventing a concurrent upload from losing its bytes when scrub classifies the blob as orphan between the upload hashing step and the ownership grant.
- Scrub on S3 now converges namespaces whose only artifact is an upload session (e.g. a client that crashed mid-push): the namespace registry tree walk includes any namespace that has an underscore-prefixed child, matching filesystem backend behaviour. Such namespaces are now visible to `UploadChecker` and their stale bytes are cleaned up by `scrub --uploads`.
- Scrub `-b` now purges all blob-index entries (including ownership markers) for any blob whose backing bytes are absent, so runtime `can_read` no longer reports such blobs as accessible to clients.

## [1.2.0]

### Added

- Maximum manifest body size enforcement.
- Warning log when a listener flips between insecure and TLS during configuration hot-reload.

### Changed

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
- Filesystem metadata now uses the same sharded blob-index and namespace-registry layout as S3, and scrub migrates legacy filesystem and S3 metadata files to the sharded layout.
- The Redis client reuses a multiplexed connection across operations and backs off on lock contention.
- The S3 backend now uses a custom HTTP client in place of the AWS SDK: focused retry / timeout / signing tailored to the operations the registry actually issues, smaller binary, and no AWS SDK transitive dependencies.
- Filesystem cleanup of empty ancestor directories is depth-bounded (2 levels for blob shards, 3 for index and namespace-registry shards, 4 for link containers) and rooted-subtree guarded; a misshaped path can never walk above the configured root.
- UI, documentation-website and Rust dependencies upgraded.
- Scrub's orphan manifest deletion now plans link operations through the same `link_plan::delete` primitive used by the runtime write path, eliminating divergence between scrub and runtime decisions about which links to remove. As a consequence, scrub also removes any tag links still pointing at an orphaned manifest digest (previously left dangling).
- Scrub's orphan manifest deletion tolerates manifest blobs that fail to parse: it removes the digest link (and any tags pointing at it) so the next pass can clean up the dangling config/layer/child links. Blob-read failures are still surfaced so operators notice broken storage.
- Overlapping repository prefixes (e.g. `team` and `team/app`) are rejected at startup rather than resolved non-deterministically at runtime.

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
