export interface Platform {
	os: string;
	architecture: string;
	variant?: string;
}

export interface Descriptor {
	mediaType: string;
	digest: string;
	size: number;
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
	pushed_at?: string;
	last_pulled_at?: string;
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

export type JobQueue = 'cache' | 'replication';

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
	return fetchJson<RepositoriesResponse>('/_ext/_repositories');
}

export async function fetchNamespaces(repository: string): Promise<FetchResult<NamespacesResponse>> {
	return fetchJson<NamespacesResponse>(`/_ext/${repository}/_namespaces`);
}

export async function fetchRevisions(namespace: string): Promise<FetchResult<RevisionsResponse>> {
	return fetchJson<RevisionsResponse>(`/_ext/${namespace}/_revisions`);
}

export async function fetchUploads(namespace: string): Promise<FetchResult<UploadsResponse>> {
	return fetchJson<UploadsResponse>(`/_ext/${namespace}/_uploads`);
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
	return fetchJson<JobsResponse>(`/_ext/_jobs?${jobsQuery(queue, n, after)}`);
}

export async function fetchFailedJobs(
	queue: JobQueue,
	n = 100,
	after?: string
): Promise<FetchResult<FailedJobsResponse>> {
	return fetchJson<FailedJobsResponse>(`/_ext/_jobs/failed?${jobsQuery(queue, n, after)}`);
}

export async function retryJob(queue: JobQueue, storageKey: string): Promise<string | null> {
	return postAction(`/_ext/_jobs/failed/${encodeURIComponent(storageKey)}/retry?queue=${queue}`);
}

export async function deleteJob(
	queue: JobQueue,
	state: JobState,
	storageKey: string
): Promise<string | null> {
	return deleteResource(`/_ext/_jobs/${state}/${encodeURIComponent(storageKey)}?queue=${queue}`);
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

export async function deleteManifest(namespace: string, reference: string): Promise<string | null> {
	return deleteResource(`/v2/${namespace}/manifests/${reference}`);
}

export async function cancelUpload(namespace: string, uuid: string): Promise<string | null> {
	return deleteResource(`/v2/${namespace}/blobs/uploads/${uuid}`);
}

export async function downloadBlob(namespace: string, digest: string, filename: string | null): Promise<string | null> {
	try {
		const response = await fetch(`/v2/${namespace}/blobs/${digest}`, {
			headers: { 'X-Angos-No-Redirect': '1' }
		});
		if (!response.ok) {
			return `HTTP ${response.status}`;
		}
		const blob = await response.blob();
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = filename ?? digest;
		document.body.appendChild(a);
		a.click();
		document.body.removeChild(a);
		URL.revokeObjectURL(url);
		return null;
	} catch (e) {
		return e instanceof Error ? e.message : 'Download failed';
	}
}
