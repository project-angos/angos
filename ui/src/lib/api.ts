export interface Platform {
	os: string;
	architecture: string;
	variant?: string;
}

export interface Descriptor {
	mediaType: string;
	digest: string;
	size: number;
	artifactType?: string;
	platform?: Platform;
	annotations?: Record<string, string>;
}

export interface Manifest {
	mediaType?: string;
	artifactType?: string;
	subject?: Descriptor;
	config?: Descriptor;
	layers?: Descriptor[];
	manifests?: Descriptor[];
	annotations?: Record<string, string>;
}

export interface ParentRef {
	digest: string;
	tags: string[];
	platform?: Platform;
}

export interface ReferrerInfo {
	digest: string;
	artifactType?: string;
	annotations?: Record<string, string>;
}

export interface ManifestEntry {
	digest: string;
	tags: string[];
	parents?: ParentRef[];
	referrers?: ReferrerInfo[];
	referrers_next?: string;
	pushed_at?: string;
	last_pulled_at?: string;
}

export interface PullEntry {
	client: string;
	at: string;
}

/**
 * One target's recorded pulls, newest first and capped by the registry.
 * `window_secs` is the operator-configured retention: entries superseded by a
 * newer one are collected past it, so this is a window, not a full history.
 */
export interface PullHistory {
	target: string;
	window_secs: number;
	entries: PullEntry[];
}

export interface ReferrersPage {
	referrers: ReferrerInfo[];
	next?: string;
}

export interface UploadEntry {
	uuid: string;
	size: number;
	started_at: string;
}

export interface RepositoryInfo {
	name: string;
	namespace_count: number;
	pull_through_cache: boolean;
	immutable_tags: boolean;
}

export interface NamespaceInfo {
	name: string;
	tag_count: number;
	manifest_count: number;
	upload_count: number;
}

interface RepositoriesResponse {
	repositories: RepositoryInfo[];
}

interface NamespacesResponse {
	repository: string;
	namespaces: NamespaceInfo[];
	pull_through_cache: boolean;
	upstream_urls: string[];
	immutable_tags: boolean;
	immutable_tags_exclusions: string[];
}

interface RevisionsResponse {
	name: string;
	manifests: ManifestEntry[];
}

interface UploadsResponse {
	name: string;
	uploads: UploadEntry[];
}

export interface JobEntry {
	storage_key: string;
	id: string;
	kind: string;
	lock_key: string;
	attempts: number;
	max_attempts: number;
	created_at: string;
	not_before: string;
}

export interface FailedJobEntry {
	storage_key: string;
	id: string;
	kind: string;
	lock_key: string;
	attempts: number;
	max_attempts: number;
	created_at: string;
	failed_at: string;
	last_error: string;
}

interface JobsResponse {
	jobs: JobEntry[];
	next?: string;
}

interface FailedJobsResponse {
	failed: FailedJobEntry[];
	next?: string;
}

export type JobState = 'pending' | 'failed';

export type JobQueue = 'cache' | 'replication' | 'scan' | 'index';
export const JOB_QUEUES: JobQueue[] = ['cache', 'replication', 'scan', 'index'];

const MANIFEST_ACCEPT_HEADER = [
	'application/vnd.oci.image.manifest.v1+json',
	'application/vnd.docker.distribution.manifest.v2+json',
	'application/vnd.oci.image.index.v1+json',
	'application/vnd.docker.distribution.manifest.list.v2+json'
].join(', ');

interface FetchResult<T> {
	data: T | null;
	error: string | null;
}

async function fetchJson<T>(url: string, options?: RequestInit): Promise<FetchResult<T>> {
	try {
		const response = await fetch(url, options);
		if (!response.ok) {
			return { data: null, error: `HTTP ${response.status}` };
		}
		const data: T = await response.json();
		return { data, error: null };
	} catch (e) {
		return { data: null, error: e instanceof Error ? e.message : 'Request failed' };
	}
}

async function deleteResource(url: string): Promise<string | null> {
	try {
		const response = await fetch(url, { method: 'DELETE' });
		if (!response.ok) {
			return `HTTP ${response.status}`;
		}
		return null;
	} catch (e) {
		return e instanceof Error ? e.message : 'Delete failed';
	}
}

async function postAction(url: string): Promise<string | null> {
	try {
		const response = await fetch(url, { method: 'POST' });
		if (!response.ok) {
			return `HTTP ${response.status}`;
		}
		return null;
	} catch (e) {
		return e instanceof Error ? e.message : 'Request failed';
	}
}

export async function fetchRepositories(): Promise<FetchResult<RepositoriesResponse>> {
	return fetchJson<RepositoriesResponse>('/v2/_angos/repositories/list');
}

export async function fetchNamespaces(repository: string): Promise<FetchResult<NamespacesResponse>> {
	return fetchJson<NamespacesResponse>(
		`/v2/_angos/namespaces/list?repository=${encodeURIComponent(repository)}`
	);
}

export async function fetchRevisions(namespace: string): Promise<FetchResult<RevisionsResponse>> {
	return fetchJson<RevisionsResponse>(`/v2/${namespace}/_angos/revisions/list`);
}

// The registry keys pull records by the reference they were pulled through, so
// the target is queried as it was addressed. A tag cannot contain a colon,
// which is what tells the two apart; the endpoint takes exactly one of them.
export async function fetchPullHistory(
	namespace: string,
	target: string
): Promise<FetchResult<PullHistory>> {
	const param = target.includes(':') ? 'digest' : 'tag';
	return fetchJson<PullHistory>(
		`/v2/${namespace}/_angos/pulls/list?${param}=${encodeURIComponent(target)}`
	);
}

export async function fetchUploads(namespace: string): Promise<FetchResult<UploadsResponse>> {
	return fetchJson<UploadsResponse>(`/v2/${namespace}/_angos/uploads/list`);
}

function jobsQuery(queue: JobQueue, n: number, after?: string): string {
	const params = new URLSearchParams({ queue, n: String(n) });
	if (after) {
		params.set('after', after);
	}
	return params.toString();
}

export async function fetchJobs(
	queue: JobQueue,
	n = 100,
	after?: string
): Promise<FetchResult<JobsResponse>> {
	return fetchJson<JobsResponse>(`/v2/_angos/jobs/list?${jobsQuery(queue, n, after)}`);
}

export async function fetchFailedJobs(
	queue: JobQueue,
	n = 100,
	after?: string
): Promise<FetchResult<FailedJobsResponse>> {
	return fetchJson<FailedJobsResponse>(`/v2/_angos/jobs/failed?${jobsQuery(queue, n, after)}`);
}

export async function retryJob(queue: JobQueue, storageKey: string): Promise<string | null> {
	return postAction(`/v2/_angos/jobs/failed?queue=${queue}&key=${encodeURIComponent(storageKey)}`);
}

export async function deleteJob(
	queue: JobQueue,
	state: JobState,
	storageKey: string
): Promise<string | null> {
	return deleteResource(
		`/v2/_angos/jobs/${state}?queue=${queue}&key=${encodeURIComponent(storageKey)}`
	);
}

export interface ManifestResult {
	manifest: Manifest | null;
	digest: string | null;
	error: string | null;
}

export async function fetchManifest(namespace: string, reference: string): Promise<ManifestResult> {
	try {
		const response = await fetch(`/v2/${namespace}/manifests/${reference}`, {
			headers: { 'Accept': MANIFEST_ACCEPT_HEADER, 'X-Angos-No-Redirect': '1' }
		});
		if (!response.ok) {
			return { manifest: null, digest: null, error: `HTTP ${response.status}` };
		}
		const digest = response.headers.get('Docker-Content-Digest');
		const manifest: Manifest = await response.json();
		return { manifest, digest, error: null };
	} catch (e) {
		return { manifest: null, digest: null, error: e instanceof Error ? e.message : 'Request failed' };
	}
}

// The `last` cursor of a `Link: <...>; rel="next"` header, absent when the
// listing is exhausted and no such header is sent. The target is resolved
// against the request's own URL, since the registry advertises it as a path.
function nextCursor(response: Response): string | undefined {
	const target = response.headers.get('Link')?.match(/<([^>]*)>/)?.[1];
	if (!target) return undefined;
	return new URL(target, response.url).searchParams.get('last') ?? undefined;
}

// One page of a subject's referrers, resuming from `last` when given. The
// registry sizes the page; the endpoint takes no page-size parameter.
export async function fetchReferrers(
	namespace: string,
	digest: string,
	last?: string
): Promise<FetchResult<ReferrersPage>> {
	const params = new URLSearchParams(last ? { last } : {});
	try {
		const response = await fetch(`/v2/${namespace}/referrers/${digest}?${params}`);
		if (!response.ok) {
			return { data: null, error: `HTTP ${response.status}` };
		}
		const index: { manifests?: Descriptor[] } = await response.json();
		const referrers = (index.manifests ?? []).map(descriptor => ({
			digest: descriptor.digest,
			artifactType: descriptor.artifactType,
			annotations: descriptor.annotations
		}));
		return { data: { referrers, next: nextCursor(response) }, error: null };
	} catch (e) {
		return { data: null, error: e instanceof Error ? e.message : 'Request failed' };
	}
}

export async function deleteManifest(namespace: string, reference: string): Promise<string | null> {
	return deleteResource(`/v2/${namespace}/manifests/${reference}`);
}

export async function cancelUpload(namespace: string, uuid: string): Promise<string | null> {
	return deleteResource(`/v2/${namespace}/blobs/uploads/${uuid}`);
}

export function blobUrl(namespace: string, digest: string): string {
	return `/v2/${namespace}/blobs/${digest}`;
}

// ---- Layer filesystems ----

export type LayerEntryKind = 'file' | 'dir' | 'symlink' | 'hardlink' | 'whiteout' | 'opaque' | 'other';

export interface LayerEntry {
	path: string;
	kind: LayerEntryKind;
	size: number;
	mode: number;
	uid: number;
	gid: number;
	mtime: number;
	link?: string;
	offset: number;
}

export interface LayerListing {
	compressed: boolean;
	uncompressed_size: number;
	entries: LayerEntry[];
}

export interface LayerEntriesResult {
	listing: LayerListing | null;
	/** The registry is indexing the layer; ask again shortly. */
	pending: boolean;
	error: string | null;
}

export async function fetchLayerEntries(namespace: string, digest: string): Promise<LayerEntriesResult> {
	try {
		const response = await fetch(`/v2/${namespace}/_angos/layers/${digest}/entries`);
		if (response.status === 202) {
			return { listing: null, pending: true, error: null };
		}
		if (!response.ok) {
			return { listing: null, pending: false, error: `HTTP ${response.status}` };
		}
		return { listing: await response.json(), pending: false, error: null };
	} catch (e) {
		return { listing: null, pending: false, error: e instanceof Error ? e.message : 'Request failed' };
	}
}

export function layerFileUrl(namespace: string, digest: string, path: string, download = false): string {
	const query = new URLSearchParams({ path });
	if (download) query.set('download', '1');
	return `/v2/${namespace}/_angos/layers/${digest}/file?${query}`;
}

// A blob body as JSON, served by the registry itself rather than by a
// presigned redirect the browser could not read across origins.
export function fetchBlobJson<T>(namespace: string, digest: string): Promise<FetchResult<T>> {
	return fetchJson<T>(blobUrl(namespace, digest), { headers: { 'X-Angos-No-Redirect': '1' } });
}
