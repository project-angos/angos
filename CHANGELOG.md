# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.4.0 - Unreleased

### Removed

- The deprecated `access_policy.default_allow` boolean is removed; set the typed `default = "allow"` or `default = "deny"` instead.

## 1.3.2

### Security

- `X-Forwarded-For`/`X-Real-IP` are no longer trusted unconditionally: the client IP used by access policies and webhooks now comes from these headers only when the peer is listed in the new `global.trusted_proxies` option, otherwise from the socket address.
- Basic-auth response timing no longer reveals whether a username exists: an unknown username now costs the same Argon2 verification as a known one.
- Duplicate usernames across `[auth.identity]` entries are now rejected at startup instead of nondeterministically shadowing each other's identity id.

### Added

- New `manifest.pull` and `blob.pull` event-webhook kinds fire on successful `GET` requests (including redirect responses), so pulls can be tracked externally; prefer the `async` delivery policy for these high-volume events.
- Event payloads gain an `actor.internal` field naming the internal process behind an operation: retention deletions now emit `manifest.delete`/`tag.delete` with `internal = "prune"`, and pull-through cache fills emit `manifest.push`/`blob.push` with `internal = "cache"`.

### Changed

- On an S3 metadata store, an unset `lock_strategy` now defaults to the shared S3 lock when the provider supports conditional operations, instead of the in-process memory lock.
- With CAS coordination, access times are now stamped inline as a single conditional write whose lost races are no-ops; `access_time_debounce_secs` only applies to lock-coordinated deployments.
- Retention deletions (`angos prune`) now run through the registry's standard delete path: blob bytes are reclaimed immediately once unreferenced, and the deletion replicates to downstreams marked `prune = true` only.
- Graceful shutdown now drains in-flight async webhook deliveries to completion instead of abandoning them after a fixed timeout; each delivery stays bounded by its own request timeout, retry cap, and backoff ceiling.
- Replication and pull-through no longer cap a whole transfer at 5 minutes: the registry client uses a connection timeout plus a per-read stall timeout (new `connect_timeout_secs`/`read_timeout_secs`), so a large blob that keeps progressing is not dead-lettered.
- `required`-policy webhooks now default to `max_retries = 3` (with the existing exponential backoff), so a transient endpoint failure no longer immediately fails the client operation; an explicit `max_retries` still wins.
- Webhook events now fire before the operation is performed instead of after it commits: delivery is at-least-once, so a performed operation can no longer go unnotified, while a rejected or failed operation may leave a false-positive intent event.

### Fixed

- The `webhook_authorization_*` and `event_webhook_*` metrics are now exported on `/metrics`; they were previously registered against a registry the endpoint does not serve.
- The S3 circuit breaker now covers multipart-upload and listing operations, so an unhealthy backend fails fast instead of being hammered by blob-upload part traffic.
- The repository and namespace listings of the web UI now include namespaces whose only content is in-progress uploads, so those uploads can be inspected and cancelled.
- Upload-only namespaces are now discovered on the blob store, where upload sessions live, so the web UI listings and `scrub --orphan-namespaces` see them when the blob and metadata stores are separate backends.

## 1.3.1

### Added

- New `angos replicate` and `angos prune` commands reconcile replication downstreams and enforce retention policies as standalone runs.
- The web UI upload list has per-row checkboxes and a select-all toggle, so many in-progress uploads can be cancelled in one action.
- New `conditional_operations` boolean on `[metadata_store.s3]` declares the provider's conditional-request support as one all-or-nothing set.

### Deprecated

- `scrub --replicate` and `scrub --retention` are deprecated in favor of `angos replicate` and `angos prune`.
- The `[metadata_store.s3.capabilities]` table is deprecated in favor of `conditional_operations`; it is still accepted and enables CAS only when all three flags are true.

### Changed

- Storage coordination now runs entirely on the metadata store: the blob store holds only blob bytes (its backend no longer carries `.tx-log/`/`.tx-bodies/` prefixes) and the in-process job queue persists on the metadata store.
- S3 CAS coordination now requires conditional deletes (`DeleteObject` with `If-Match`) alongside conditional puts, making lock release and lock reclaim race-free; providers lacking the full set fall back to lock-based coordination.

## 1.3.0

### Added

- Bi-directional replication mirrors manifest pushes and deletes to per-repository downstreams over the durable job queue; `scrub --replicate` reconciles on demand.
- A path on a pull-through upstream's or replication downstream's `url` is the namespace prefix the content maps to (`<repo>/x` ↔ `<path>/x`), so a repository can mirror a prefixed remote namespace or fan out into sibling repositories; a bare host maps verbatim.
- New `scrub --replication-orphans` and `scrub --cache-orphans` flags delete pending and dead-lettered replication and cache jobs whose downstream or pull-through repository is no longer configured.
- New `scrub --orphan-namespaces` (`-n`) flag removes revisions, tags, and in-flight uploads for namespaces not owned by any configured repository and reclaims their layer/config blob bytes (combine with `--blobs` to also reclaim manifest blob bytes); it is destructive (dry-run first) and refuses to run when no repositories are configured.
- Cross-repository blob mount (`POST /v2/{namespace}/blobs/uploads/?mount={digest}[&from={repository}]`) grants an already-present blob to the target namespace with no upload.
- Blob and manifest pushes, pulls, and deletes now accept sha512 digests in addition to sha256.
- A by-digest manifest push supports the `?tag=` query parameter to create one or more tags pointing at the pushed manifest, returning the accepted tags in an `OCI-Tag` response header.
- The `_jobs` admin API accepts `?queue=cache|replication` (default `cache`), so failed replication jobs can be listed, retried, and deleted like cache jobs.
- New `angos_replication_*` metrics (`angos_replication_push_total`, `angos_replication_last_success_timestamp_seconds`, `angos_replication_reconcile_total`) expose per-downstream push health and reconcile outcomes, and the existing `angos_job_queue_pending` gauge gains a `queue="replication"` series for the new replication queue backlog.
- New server-published `angos_job_queue_failed{queue}` gauge reports dead-lettered jobs per queue, so replication failures stay observable even when `angos worker` drains the queue.
- New `[global] allow_missing_manifest_references` knob (default `true`) accepts manifest pushes with absent or unowned references, leaving them unreadable; set `false` to reject with `MANIFEST_BLOB_UNKNOWN`.
- New `[global] max_blob_size` knob (default `100GiB`) caps the total size of a single blob upload, rejecting a larger upload with `BLOB_UPLOAD_INVALID`.
- Scrub `--tags` removes tag directories whose names violate the OCI tag grammar.
- New `scrub --reconcile-blob-index` flag rebuilds blob-index grants missing relative to the manifests that reference each blob, repairing an index corrupted out-of-band; it reads every manifest, so it is expensive.

### Changed

- S3 operation retries now wait an exponential, jittered backoff instead of retrying immediately, so a throttled bucket is no longer hammered.
- The server's in-process job drain backs off exponentially after a claim error instead of retrying every 100ms.
- Webhook delivery retry backoff is capped at ten seconds.
- A blob-upload `POST` carrying `?mount=` is authorized as the new `mount-blob` action; container clients send it opportunistically on push, so a default-deny policy must grant `mount-blob` alongside `start-upload` or those pushes fail.
- `angos worker` with no `--queue` now drains both the `cache` and `replication` queues (pass `--queue cache` for the former cache-only behavior) and rejects unknown `--queue` values at startup.
- A `[global.job_queue]` using the in-process `memory` lock strategy is now rejected at startup (breaking): set the metadata store's `lock_strategy` to `s3` or `redis`, or remove `[global.job_queue]` to use the in-process queue.
- A blob-upload `POST` with a malformed `?digest=`, `?mount=`, or `?from=` now returns `400` instead of silently starting an upload session that ignores the value.
- The `_catalog` listing is derived directly from stored content (deterministic and strongly consistent); the maintained namespace-registry index is removed and its now-unused `_registry/` objects are pruned by `scrub`.
- Tags, repository names, upload session IDs, and manifest/descriptor media types are now strictly validated against their OCI grammars, so a request carrying a malformed value is rejected with `400` where an earlier version might have accepted it.
- Repository names exceeding the OCI 255-character limit are now rejected where an earlier version accepted them.

### Fixed

- Blob uploads using chunked transfer-encoding without `Content-Length`, as sent by `docker push`, are now accepted and streamed to EOF.
- Manifests are now stored in the blob store, so a registry with its blob and metadata stores on separate backends no longer returns 404 on manifest read or delete.
- Pulling a pull-through upstream at the repository root now maps the namespace verbatim instead of building a malformed `/v2//` request URL that upstreams reject.

## 1.2.0 - 2026-06-03

### Added

- Stale lock objects under `.tx-locks/` are reclaimed automatically by a periodic janitor running on every server and worker replica; no operator action is required.
- New `scrub --referrers` flag: checks every revision in each namespace and removes any referrer link whose referrer manifest no longer has a current digest revision link, preventing ghost descriptors from appearing in the OCI Referrers API response.
- Durable cache jobs: the optional `[global.job_queue]` section persists pull-through cache-fill jobs in the `[metadata_store]` backend so they survive restarts, drained by the new `angos worker` subcommand.
- Maximum manifest body size enforcement.
- Warning log when a listener flips between insecure and TLS during configuration hot-reload.

### Changed

- Blob upload sessions use a single resumable streaming upload; in-flight sessions do not survive the upgrade, so clients retry and `scrub --uploads` reaps the stale staging artifacts.
- The angos extension API moved from the `/v2/_ext/...` prefix to the top-level `/_ext/...` prefix (breaking): update clients of the v1.1.1 `/v2/_ext/...` endpoints to the new paths.
- The registry subsystems now write atomically through a per-subsystem transactional engine, adding new top-level prefixes (`.tx-log/`, `.tx-bodies/`, `.tx-locks/`, and `_jobs/` when the durable job queue is enabled) that operators should factor into bucket policies.
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
- TLS listeners support an explicit `client_auth = "optional" | "required"` setting so operators can enforce mTLS at the handshake.
- Outbound `[registry_client]` blocks with a partial mTLS pair (`client_certificate` set without `client_private_key`, or vice versa) now fail at configuration load instead of silently disabling outbound client authentication.
- Webhook retries are capped at 16 with saturating backoff arithmetic.
- The S3 shard-key hash uses SHA-256, so shard placement is stable across processes and builds.
- Filesystem metadata now uses the same sharded blob-index and namespace-registry layout as S3, with legacy `index.json` and `namespace_registry.json` files still readable until `scrub` migrates them.
- The Redis client reuses a multiplexed connection across operations and backs off on lock contention.
- The S3 backend now uses a custom HTTP client in place of the AWS SDK, dropping the AWS SDK transitive dependencies and shrinking the binary.
- Filesystem cleanup of empty ancestor directories walks up from a deleted leaf, removing empty parents until the first non-empty directory or the store root, and is rooted-subtree guarded; a misshaped path can never walk above the configured root.
- UI, documentation-website and Rust dependencies upgraded.
- Scrub's orphan manifest deletion now also removes tag links pointing at an orphaned digest and tolerates unparseable manifest blobs while still surfacing blob-read failures.
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
- Scrub reliably removes orphaned and stale links, tags, media-types, and blob-index entries, cascading deletions and tolerating missing or unreadable manifest blobs without aborting the namespace.
- Scrub orphan manifest deletion no longer removes blobs still referenced by other manifests.
- Scrub orphan-blob deletion acquires the blob-data lock and re-checks ownership before deleting, so a concurrent upload cannot lose its bytes.
- Scrub on S3 converges namespaces whose only artifact is an upload session, so `scrub --uploads` cleans up their stale bytes and aborts the recorded in-flight multipart upload.
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
