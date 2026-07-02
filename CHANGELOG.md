# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.4.0 (UNRELEASED)

### Added

- New `angos policy` subcommand enforces retention.
- New `angos replication` subcommand reconciles replicated repositories with their downstreams.
- New `[global] maintenance_grace_secs` knob (default 48h): every scrub garbage-collection reap requires the candidate's backend `last_modified` to be older than the grace, and an object without one is never reaped.
- New `[global] maintenance_lock_max_hold_secs` knob (default 24h) caps how long one maintenance run may hold the shared maintenance lock before it self-cancels.
- New `[global] max_concurrent_scrub_tasks` knob bounds per-node scrub/policy/replication fan-out (default 16).
- New `scrub --delete-unrecognized` flag deletes keys matching no known storage layout, gated on `--commit` and the maintenance grace.
- New `scrub --jobs` flag reconciles the job dedup index both ways: it retires dangling entries whose pending envelope is gone and re-indexes a pending job whose entry is missing or stale, skipping claimed in-flight jobs.
- New `scrub --prune-unknown` modifier opts into deleting otherwise report-only structurally-invalid objects (invalid-named namespaces, unknown job-queue directories).
- New `scrub --reclaim-engine` flag runs one on-demand pass of the engine's pure-delete janitors (orphan `.tx-bodies/` staging, expired `.tx-locks/`).
- `scrub`, `policy`, and `replication` end every run with a summary of action counts and findings, and scrub writes its status, exit code, run mode (`commit` / `report-only` / `dry-run`, so a watcher can tell performed deletions from would-counts), and tallies to `_scrub-audit/latest.json`.

### Changed

- `scrub` is redesigned to **rebuild-and-sweep**: every run re-derives each revision's links, ownership grants, referrer back-links, and media type from its manifest, then sweeps raw storage for orphan blobs, stale grants, orphan links, missing-body revisions, de-configured namespaces, and unrecognized keys.
- `scrub` is **report-only by default**: without `--commit` it performs no storage mutation at all except the run marker; sweep deletions, the rebuild's repairs, the legacy blob-index convergence, and the `_registry/` prune all require `--commit`.
- De-configured namespace deletion is scoped to the namespace's own data; a nested namespace that still resolves to a configured repository is never touched.
- Stale blob-index entries of every kind (tag/digest/referrer as well as layer/config/manifest) are purged once past the maintenance grace, so a phantom entry can no longer pin bytes or shield a manifest from retention indefinitely.
- `scrub`, `policy`, and `replication` exit `0` clean, `1` refused, `2` degraded, `3` aborted; schedulers such as systemd and Kubernetes treat `2` and `3` as failures.
- Every `scrub` run and every mutating `policy`/`replication` run takes the single shared maintenance lock, and a mutating run is refused on the `memory` lock strategy.
- Manifest pushes hold the per-blob lock for each newly referenced blob, so storage maintenance can never reclaim bytes a concurrent push references.
- An unreferenced bare blob-ownership self-grant is revoked once older than the maintenance grace, reclaiming bytes that pinned storage indefinitely; an explicit blob DELETE still reclaims immediately.
- The scrub rebuild preserves each link's creation and last-pull times, so last-pull retention rules stay accurate across maintenance runs.
- Namespaces not owned by any configured repository are deleted by the sweep under `--commit` (grants revoked, uploads included), refusing to run when no repositories are configured.
- Scrub also visits namespaces holding no manifests, reclaiming orphan links and stranded uploads such namespaces previously retained.

### Deprecated

- `scrub --dry-run` (`-d`) is deprecated in favour of the report-only default; it writes nothing to registry state (only the run marker) and is mutually exclusive with `--commit`.
- `scrub --retention` (`-r`) is a deprecated alias for `angos policy --retention`; it still runs unchanged.
- `scrub --replicate` is a deprecated alias for `angos replication`; it still runs unchanged.
- The legacy single-file blob-index read fallback is retained so an unconverged blob is still served from its legacy `index.json`, with removal deferred until after fleet-wide convergence.

### Removed

- The scrub per-checker flags `-m`/`--manifests`, `-l`/`--links`, `-M`/`--media-types`, `-t`/`--tags`, `-R`/`--referrers`, `-n`/`--orphan-namespaces`, `-b`/`--blobs`, `--orphan-grants`, and `--reconcile-blob-index` are removed and fail argument parsing; their work runs unconditionally in the rebuild and sweep.

### Fixed

- Scrub removes phantom back-references and the orphan links they kept alive, reclaiming the blob bytes they pinned.
- A bare self-grant re-confirmed by a concurrent upload dedup or cross-repo mount (which rewrites the grant shard without creating a link file) is kept by the sweep: the revocation re-probes the shard's freshness under the per-blob lock.
- A tag concurrently re-pointed at a new manifest survives a manifest-delete or missing-body cascade: cascade tag deletes are conditioned on the target they were planned against.
- The lock janitor honors a lock's declared recovery margin, so a slow maintenance run no longer has its lock stolen mid-run.
- A corrupt job dedup-index entry is retired and rebuilt automatically instead of silently suppressing every later replication or cache job with the same key.
- The periodic body janitor reaps orphan `.tx-bodies/` staging directories whose transaction intent never landed; a path bug previously made every sweep a silent no-op.

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
