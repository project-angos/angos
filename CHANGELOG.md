# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.4.6 - UNRELEASED

### Added

- An optional token service exchanges a client's credential for a registry-signed bearer token at `GET /token`, so a short-lived CI credential no longer has to outlive the push it starts.

### Fixed

- An upstream token response that omits `expires_in` is now cached for the 60 seconds the spec defines as its default rather than an hour, so angos stops sending a token long after its issuer stopped honouring it.

## 1.4.5

### Fixed

- A transaction intent record that no longer decodes is reclaimed by the engine janitor once it ages out, instead of sitting in the log forever and holding off every scrub repair as though a transaction were still in flight.
- A manifest declaring both image content (`config`/`layers`) and index content (`manifests`) is refused on push, since the image spec makes the two mutually exclusive; one already stored stays readable as the index it lists.
- A malformed `?artifactType=` on the referrers endpoint is rejected rather than silently dropped, which used to turn a bad filter into an unfiltered listing of every referrer.
- A transaction read that recorded a key as absent now conflicts when a zero-length object appears at that key, instead of mistaking it for the absence it recorded and writing over another writer's fresh entry.
- A multipart part upload whose response carries no `ETag` now fails at that part, naming it, instead of defaulting to an empty string that the S3 backend rejects later at `CompleteMultipartUpload` with no clue which part was at fault.
- `angos migrate` now rewrites each link inside a transaction that reads it, so a tag push landing mid-run is kept instead of being silently reverted to its pre-push target.
- A blob PUT naming a session the registry does not hold is refused at the upload endpoint rather than relying on the storage layer to reject the write.
- A job's `lock_key` is now a validating type that rejects the `%` reserved by the dedup-index encoding, so two distinct lock keys can no longer encode to one index path and falsely coalesce.
- The namespace listing skips a directory whose name is not a valid namespace instead of failing the whole request, so one stray directory no longer breaks the web UI's view of a repository.
- The cross-namespace blob-reference check now reads shard contents rather than presence alone, so a legacy or corrupt empty shard in another namespace no longer blocks a blob's reclamation forever.
- The Redis cache URL is held in the redacting `Secret` wrapper, so a password carried in its userinfo no longer reaches logs when the loaded configuration is debug-formatted.
- Every failing required event webhook is now logged, so when several fail in one dispatch the ones after the first are no longer visible in metrics alone.
- Redis connections are managed rather than per-call: the lock backend opens one connection instead of one per operation, and both it and the cache backend reconnect on their own, so a Redis restart no longer disables the authorization, JWKS, and upstream-token caches until angos is restarted.
- Aborting an upload another replica already aborted now counts as done rather than failing, so two prune runs sharing a bucket no longer report a spurious error.
- S3 multipart uploads respect the protocol's part limits: a large append is split to stay under the 5 GiB per-part ceiling instead of failing the push, a configured part size below the 5 MiB floor is raised to it rather than failing at completion, and an upload past 10,000 parts is refused before its bytes are streamed.
- An upload write whose body is longer than its declared length is rejected on the S3 backend as it already was on the others, instead of silently discarding the excess.
- A chunked upload now keeps one hasher checkpoint instead of one per chunk, so the listing every append and finalize performs no longer grows with the number of chunks, and completing an upload probes its liveness marker rather than reading the whole session.
- Cached upstream bearer tokens are scoped to the credential that obtained them, so two clients configured against the same registry with different usernames no longer serve each other's tokens and act as the wrong identity.
- Upstream request logging moved to debug and no longer includes the query string, keeping the signed state in a server-assigned upload-session URL out of the logs and pull-through probe traffic out of info-level output.
- `angos migrate` now warns past a link it cannot read or rewrite and reports the count in its summary, instead of one defective object aborting the whole run.
- A JWT whose key id is absent from the cached JWKS now forces at most one provider refetch a minute rather than one per request, so unauthenticated tokens carrying random key ids can no longer amplify into outbound requests to the identity provider.
- Resuming a child listing from a directory name on the S3 backend no longer re-emits that directory forever or skips siblings sorting between it and its delimiter, so paging through a namespace's children terminates and returns every child.
- An upstream or storage outage is no longer reported to clients as a missing image: a non-404 upstream status on a blob fetch and a backend fault on a local manifest read now surface as errors instead of collapsing into 404, and a routine manifest miss is logged at debug rather than error.
- A cached upstream token now indexes the URL it served, so requests to a token-auth upstream stop paying an extra unauthenticated round trip and 401 for every URL other than the one that first obtained the token.
- A streamed blob download from S3 is no longer cut off once the per-attempt timeout elapses, so pulling a large layer over a slow connection completes; a transfer that stalls is still ended by a read timeout that resets on every read.
- Multipart cleanup now treats only a genuinely absent session marker as proof that an upload was abandoned, so a transient backend failure during a prune pass no longer aborts an in-progress upload and destroys the parts it had already committed.
- An upstream that refuses a request after angos refreshes its token now reports a denial rather than an opaque failure, so a replication job whose credential lacks the scope dead-letters immediately instead of retrying to its attempt limit, and reads classify it the same way writes already did.
- Raising `angos prune --concurrency` no longer multiplies into that many in-flight metadata reads squared, since the per-namespace tag reads keep their own fixed fan-out instead of borrowing the knob that already bounds the namespace walk.
- A client header listed in an authorization webhook's `forward_headers` now reaches the webhook with every one of its values instead of just the first, and the extra values are part of the decision cache key.
- A replication push whose blob or child-manifest transfer fails now lets its siblings finish instead of dropping them mid-transfer, which left their upload sessions open on the downstream until its own garbage collection.
- A replication delete job now carries the referrer's subject, so a retry that finds the manifest already gone downstream can still drop its stale descriptor from the OCI 1.0 referrers fallback index.
- Pushing a manifest whose `schemaVersion` is not 2 is now refused instead of stored with none of its blobs linked, leaving them to be reclaimed as orphans.
- A malformed `?from=` on a blob-upload POST is now refused instead of ignored when no `?mount=` accompanies it, matching the `?mount=` and `?digest=` values on the same request.
- An upload directory whose name is not a session id is now quarantined by scrub like any other unrecognized key, instead of being reported as a session that only prune could reach.
- A directory whose name is not a valid namespace no longer appears in the `_catalog` listing or the admin namespace listing, matching how a malformed tag directory is already dropped; scrub still reports and reclaims it.
- An `Accept` member that is not a media range is dropped instead of being relayed verbatim to an upstream on a pull-through request; the wildcard forms clients actually send (`*/*`, `type/*`) are unaffected.
- An upload session whose stored state no longer decodes is now reaped by prune instead of kept forever, since re-reading it returns the same bytes; a session whose read failed transiently is still kept.

## 1.4.4

### Added

- The namespace listing reports a `tag_count` alongside the manifest and upload counts, which the web UI shows as a column.

### Fixed

- A completed upload is now promoted only when the assembled object is exactly as long as the bytes the session hashed, so an append that failed after durably writing bytes can no longer leave a resumed upload serving a blob whose content does not hash to its digest.
- A `[server.tls]` section that does not parse now fails startup instead of falling through to a plaintext listener, so a malformed TLS configuration can no longer silently downgrade the registry to HTTP.
- Repository names accept the full distribution-spec separator set, so a name using a double underscore or a run of dashes such as `a__b` or `a--b` is no longer rejected on push, pull, and pull-through mirroring.
- `request.reference` reaches CEL access policies as the documented string rather than a tagged object, so a rule such as `request.reference == "latest"` now matches instead of never firing.
- The web UI discards responses from a superseded load and guards `Load more` while a page is in flight, so navigating or paging quickly no longer shows stale data or appends a page twice.
- A failed action in the web UI is reported in a banner above the view it was taken on instead of replacing the page, and deleting the tag being viewed navigates to the digest rather than stranding the user on a 404.
- The web UI downloads a layer through a plain link, so the browser streams it to disk instead of holding a blob that is routinely multiple gigabytes in tab memory.
- Repository, namespace and manifest rows in the web UI carry a real link, so the primary drill-down is reachable by keyboard.
- The web UI resolves a browse path against the configured repository names, so a repository whose name contains a slash such as `team/website` opens its own listing instead of being read as the repository `team` and reporting 404.
- The web UI lists the namespaces nested under the path being browsed, so an intermediate level of a name such as `team/website/backend` shows what lives below it instead of an empty manifest list.
- A browse URL with a trailing or doubled slash now names the same path as one without, instead of resolving to an invalid namespace and reporting 404.
- Deleting a tag or cancelling an upload in the web UI no longer refetches the namespace listing, which costs a store walk and three backend listings per namespace, so an action on a repository holding many namespaces completes without that delay.
- A remote that omits the SHOULD-level `Docker-Content-Digest` header no longer breaks pull-through or replication: a manifest GET recovers the digest by hashing the body, and a HEAD that cannot report one refetches or pushes instead of failing.
- A `Docker-Content-Digest` that is present but unparseable is no longer read as an absent one, so a blob mount advertising garbage is rejected instead of being accepted as converged, and a manifest push whose echo cannot be parsed logs a warning rather than silently skipping the divergence check.

## 1.4.3

### Added

- `-c` is repeatable: configuration files are merged in order with later files winning, so credentials can live in their own file and rotate without a restart.

### Fixed

- The configuration watcher now matches events against every spelling of a watched path, so hot reload and certificate rotation keep working on Linux when the path is reached through a symlink, as it is under a Kubernetes Secret or ConfigMap mount.
- The `X-Forwarded-Proto` header sent to authorization webhooks now reports the scheme the listener served the request on, so a TLS listener reports `https` instead of always `http`.

## 1.4.2

### Changed

- The web UI's embedded assets are served straight from the binary instead of being copied into a fresh buffer on every request.

### Fixed

- A manifest push now checks reference ownership inside the link transaction (strict rejects, permissive drops the link), so a delete or prune reclaiming a referenced blob mid-push can no longer slip a manifest whose layer bytes are gone past the pre-write validation.
- The pull-through cache-fill grant path now checks byte presence inside the blob-data lock, so a reclaim racing the fill can no longer leave a dangling ownership grant on deleted bytes.
- A committed link delete no longer sweeps its directory prefix afterwards (the transaction removes the link and the FS backend prunes emptied directories itself), so the sweep can no longer erase a tag or revision a concurrent push re-created after its 201.
- Blob-index removals now re-check under the blob-data lock that the entry is still byteless or dangling before applying, and the delete path's reclaim decision now shares the commit-validated shard read, so a concurrent push, upload, or cache fill can no longer lose its just-written grant or have a referenced blob's bytes reclaimed.
- A link transaction now plans one mutation per repeated operation, so pushing a manifest that lists the same layer digest twice no longer erases that layer's other referrers and drops its link when the manifest is deleted.
- A lock heartbeat or release that has to re-read the lock object now checks the body's writer nonce first and gives the lease up one tick before the TTL expires, so a holder whose refresh failed can no longer reclaim or delete the lock a peer took over after expiry.
- A lock-coordinated transaction that aborts after one of its mutations applied now reports a non-retriable partial commit, so the caller's retry can no longer commit fresh state that the recovery loop later reverts by replaying the preserved intent.
- A manifest delete by digest now resolves and plans its tag cascade inside the blob-data lock, so a tag pushed to that digest while the delete waits is deleted with it instead of surviving as a tag that serves a revision the delete removed.
- The recovery loop now re-reads an intent after taking its lock, so a transaction its owner finished or advanced in that window is no longer replayed from a stale snapshot (which could delete an object a later transaction re-created) or rolled back after its first mutation committed.
- Scrub now repairs a manifest's child links and grants only for digests the namespace already references, so with `allow_missing_manifest_references` enabled it can no longer hand a namespace the cross-namespace read access the push deliberately withheld.
- A lock object whose body no longer parses is now reclaimed by the lock janitor once its object mtime ages past the longest permitted lock TTL, instead of blocking every acquire on that key until an operator deleted it by hand.
- A job claim whose dedup index cannot be retired is now left for the next scan instead of running with that index live (which coalesced every same-`lock_key` enqueue into the running job and dropped its write), and a rescheduled retry no longer overwrites an index a concurrent enqueue just pointed at its own fresh job.
- Error paths now reclaim what they staged instead of leaving it for scrub: an aborted transaction discards its staged bodies, a failed or rejected pull-through cache fill drops its upload session, and a completion whose bytes hash to the wrong digest aborts the session rather than keeping a full blob on disk.
- `HEAD /v2/` now answers the API version check instead of falling through to the web UI and serving `index.html` with a `200`.

## 1.4.1

### Fixed

- `angos migrate` now backfills a manifest link's `media_type` from the body, so a tag or revision served after migration advertises the `Content-Type` that go-containerregistry clients such as kaniko require and reject when absent.
- Contended CAS transaction retries (blob-index shard merges and the whole-transaction retry loop) now back off with jitter instead of retrying in lockstep, so concurrent writers on a shared shard converge in-band on S3 rather than stranding partial commits for the recovery loop.
- Scrub's repair-settle check is now budgeted by re-check attempts instead of a wall-clock deadline, so a slow intent-log scan on S3 no longer exhausts the budget before a single real re-check and defers every repair.

## 1.4.0

### Security

- Debug-formatting a registry client (pull-through upstream or replication downstream) no longer exposes the configured password; the client stores it in the redacting `Secret` wrapper.
- A pull-through cache fill now rejects a fetched blob whose bytes do not hash to the requested digest, so a compromised or man-in-the-middle upstream can no longer poison the cache with content stored under a trusted digest.
- A 5xx response body no longer includes the internal error string (which could carry a backend URL or connection detail); the full detail is logged server-side and the client receives only the error code and a request id to quote.
- The authorization-webhook decision cache is now keyed on the full set of forwarded request headers (host, URI, and operator-forwarded headers) rather than just identity and action, so a cached allow can no longer be replayed across a different forwarded context the webhook could have denied.

### Added

- New `angos migrate` command rewrites pre-JSON bare-digest link files as JSON, converting registries seeded from a raw Docker `distribution` on-disk layout.
- New `scrub --delete-unknown` flag deletes unrecognized keys outright instead of quarantining them under `_lost_and_found/`.
- New `prune --concurrency` option (default 25) checks namespaces, uploads, blobs, and shards concurrently within each sweep, like the scrub walk.
- New OIDC provider options `http_request_timeout_secs` (default 30) and `jwks_refresh_timeout_secs` (default 5) make the previously hardcoded JWKS/discovery fetch timeouts configurable per provider.
- Previously hardcoded operational constants are now configurable (the S3 circuit breaker, children-scan and namespace-walk fan-outs, presigned-URL TTL, durable-queue retry policy, S3 lock ambiguous-write retries, and shutdown drain grace), and the prune/replicate sweep fan-outs derive from their concurrency options.

### Changed

- Scrub is rewritten as a single concurrent walk that categorizes and validates every object key: all checks always run, unreadable objects are deleted, and unrecognized keys are quarantined under `_lost_and_found/`; run scrub from the same version as the server fleet.
- Prune now owns configuration-relative and age-gated reclamation: orphan namespaces and orphan jobs are always cleared, and a single `-u/--uploads` window (default 1h) gates upload sessions, orphan S3 multiparts, and byteless blob-index entries.
- Grant-only blob ownership (a blob uploaded whose manifest never landed) is now decided by the retention policies like any other untagged content, with the `-u` window shielding in-flight pushes.
- The transaction engine's garbage janitors (orphaned staging bodies, expired lock objects) no longer run as background loops in the server and worker; `angos scrub` sweeps them instead, while the recovery loop stays in serving processes.
- A `put-manifest` CEL policy input now exposes `request.digest` (by-digest push) and `request.tags` (the tags the push creates) instead of `request.reference`; update any access policy that gated a manifest push on `request.reference`.
- The `_ext` namespace walk now lists a directory's siblings concurrently, and a single-repository listing walks only that repository's subtree, so the repository and namespace listings are no longer bottlenecked by sequential whole-store scans on S3.
- `angos prune` now fails to start when a retention rule uses `last_pulled_at` or `top_pulled` while `update_pull_time` is disabled, instead of silently treating every image as never pulled.
- Every listing-driven operation now streams pages lazily and fans out its per-item reads with bounded concurrency, so maintenance sweeps, the `_ext` info endpoints, and the engine janitors are bound by backend latency instead of serialized round-trips; whole-store blob enumeration additionally walks its hash-shard prefixes in parallel.
- Complete child enumerations (notably the tags listing) now read the directory once on the filesystem backend and scan disjoint name ranges concurrently on S3, instead of chaining every page through one continuation token.
- `/readyz` now probes storage with one bounded listing instead of enumerating the whole namespace catalog on every poll, and the namespace walk keeps its scan fan-out saturated instead of pausing at every tree level.

### Fixed

- A failing required event webhook no longer skips that event's delivery to the remaining endpoints; the failure is surfaced after every matching webhook got its delivery.
- Async event-webhook deliveries no longer accumulate one finished task handle per event for the life of the server, and their delivery-duration metric now measures the actual delivery instead of the task spawn.
- A configuration reload now applies `query_timeout` and `query_timeout_grace_period` changes to the non-TLS listener; previously only the TLS listener picked them up without a restart.
- Delete and reclaim decisions no longer treat a failed blob-index read as an absent index: a corrupt shard fails the transaction and a prune revision whose index read errors is skipped, instead of either green-lighting deletion.
- A replication or cache-fill job whose downstream or upstream returns `403` is now dead-lettered immediately as a terminal denial instead of retrying until its budget is exhausted, since retrying cannot clear a permission failure.
- A transient backend failure while retiring an orphaned job dedup index is now surfaced instead of being swallowed as a miss, so the lingering index can no longer make an enqueue collide on its `PutIfAbsent` and silently drop a distinct job as a false duplicate.
- A job claim now re-reads the pending file after acquiring the execution lock, so a job completed by another worker in the gap between the first read and the lock is skipped instead of being re-run and possibly dead-lettered a second time.
- The S3 client's `User-Agent` now advertises the angos release version instead of the internal transport crate's `0.1.0`.
- A blob `HEAD` now returns `500` for a transient or internal storage fault instead of reporting the blob absent with a `404`, matching the `GET` path; only a genuine miss is a `404`.
- A `prune = true` reconcile no longer enqueues a delete for a downstream tag on the strength of a stale local snapshot; each prune candidate is re-checked against live local state, so a tag pushed mid-reconcile is not reaped.
- Retention no longer treats content with an unknown push time (a migrated legacy link carries none) as pushed at the Unix epoch; an age rule like `image.pushed_at > now() - days(30)` now keeps it instead of deleting it, while never-pulled content stays eligible for pull-age deletion.
- The web UI manifest tree no longer hides a manifest that lists itself as a parent or referrer; the self-reference is ignored so the manifest still renders.
- Navigating the web UI quickly between manifests or namespaces no longer lets a slow earlier response overwrite the current view; a superseded load is discarded.
- A repository access policy written as `default = "deny"` with no rules now denies as configured; it was previously indistinguishable from an absent policy and silently ignored.
- Concurrent pushes sharing a layer no longer strand a blob-index shard update as a permanent partial commit that the recovery loop logged "precondition failed" for on every sweep; shard updates are now idempotent merges that converge under contention.
- The recovery loop now abandons a committed transaction that stays unreconcilable past a one-hour grace instead of replaying it forever, clearing legacy stranded intents after upgrade; the blob index is reconciled by `angos scrub`.
- The web UI could not display a manifest or download a blob when redirects were enabled, because a browser cannot follow the cross-origin redirect to a pre-signed S3 URL; GET requests carrying the `X-Angos-No-Redirect` header are now served inline.
- The transaction engine's body janitor never actually reclaimed orphaned `.tx-bodies/` staging: it mishandled the listing's relative names and always concluded there was nothing to delete.
- On the filesystem backend, deleting a single object sometimes left its now-empty parent directories behind, and directory-based listings served them back (a deleted tag kept appearing in the tag list).
- Referrer listing now reads fallback manifest content through the blob store, so referrers resolve when the blob and metadata stores use separate backends.
- A multi-key lock acquisition hitting a hard storage error during stale-lock recovery no longer leaks its already-acquired lock objects until TTL expiry.

### Removed

- The deprecated `access_policy.default_allow` boolean is removed; set the typed `default = "allow"` or `default = "deny"` instead.
- The deprecated `cache_store` config section is removed; use `cache`.
- The deprecated `storage` config section is removed; use `blob_store`.
- The deprecated `global.enable_redirect` boolean is removed; use `global.enable_blob_redirect` and `global.enable_manifest_redirect`.
- The legacy single-file blob index (`index.json`) runtime fallback and its scrub migration are removed; run `angos scrub` on the prior version before upgrading, or references held only in an un-migrated `index.json` are lost.
- The pre-JSON bare-digest link-metadata runtime fallback is removed; such links no longer resolve until `angos migrate` rewrites them as JSON.
- The manifest `media_type` runtime fallback and its `scrub --media-types` backfill are removed; run `scrub --media-types` on the prior version before upgrading, or a manifest whose link lacks a `media_type` is served without a `Content-Type`.
- Scrub no longer prunes the dead pre-1.3 namespace-registry objects (`_registry/`); run `scrub` on the prior version before upgrading to have them removed automatically, otherwise the inert objects can be deleted manually.
- The deprecated `scrub --retention` and `scrub --replicate` flags are removed; use `angos prune` and `angos replicate`.
- All scrub selection flags (`--tags`, `--manifests`, `--blobs`, `--links`, `--reconcile-blob-index`, `--referrers`, `--replication-orphans`, `--cache-orphans`, `--uploads`, `--multipart`, `--orphan-grants`, `--orphan-namespaces`) are removed; every structural check always runs and the rest moved to `angos prune`.
- The deprecated `[metadata_store.s3.capabilities]` table is removed; set `conditional_operations` (a config still carrying `capabilities` now ignores it and probes at startup).
- The pre-JSON resumable-hash checkpoint fallback is removed; a chunked upload checkpointed by a pre-1.3 build restarts on resume across the upgrade instead of continuing.

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
