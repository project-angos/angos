import { redirect } from '@sveltejs/kit';
import { resolve } from '$app/paths';
import {
	PAGE,
	fetchManifest,
	fetchNamespaces,
	fetchRepositoryNames,
	fetchRevision,
	fetchRevisions,
	fetchUploads,
	type FetchResult,
	type ManifestResult,
	type NamespacesResponse,
	type RevisionsResponse,
	type UploadsResponse
} from '#lib/api.js';
import { isFirstLoad, resolveRepository } from '#lib/utils.js';
import type { PageLoad } from './$types';

/** What the page first shows, fetched before the navigation completes. */
export type InitialView =
	| { kind: 'manifest'; manifest: ManifestResult; revision: FetchResult<RevisionsResponse> | null }
	| {
			kind: 'browse';
			revisions: FetchResult<RevisionsResponse>;
			uploads: FetchResult<UploadsResponse>;
			/** `null` above every repository, where only the names are listed. */
			namespaces: FetchResult<NamespacesResponse> | null;
	  };

export interface BrowseParams {
	/** Full registry path being browsed, never empty. */
	path: string;
	/** Repository owning `path`, or `null` above every configured one. */
	repository: string | null;
	/** Tag or digest when a single manifest is being viewed. */
	reference: string | null;
	/** Configured repository names, reused to list what sits above one. */
	repositoryNames: string[];
	/** Still pending on the first load, which renders before it arrives. */
	view: InitialView | Promise<InitialView>;
}

export const load: PageLoad = async ({ params }): Promise<BrowseParams> => {
	// Empty segments come from a trailing or doubled slash and name nothing.
	const segments = params.path.split('/').filter(Boolean);
	if (segments.length === 0) {
		redirect(307, resolve('/'));
	}

	const last = segments[segments.length - 1];
	const digestIndex = last.indexOf('@');
	const tagIndex = last.lastIndexOf(':');
	let reference: string | null = null;
	let tail = last;
	if (digestIndex !== -1) {
		tail = last.slice(0, digestIndex);
		reference = last.slice(digestIndex + 1);
	} else if (tagIndex !== -1) {
		tail = last.slice(0, tagIndex);
		reference = last.slice(tagIndex + 1);
	}

	const path = [...segments.slice(0, -1), tail].filter(Boolean).join('/');
	let names = (await fetchRepositoryNames()).data ?? [];
	let repository = resolveRepository(names, path);
	// A path the kept names place above every repository rereads them: its
	// repository may have gained content since they were read.
	if (repository === null) {
		names = (await fetchRepositoryNames(true)).data ?? [];
		repository = resolveRepository(names, path);
	}

	// Fetched here rather than on mount so a navigation swaps the whole view
	// at once instead of blanking it behind a spinner.
	const view = fetchView(path, repository, reference);
	return { path, repository, reference, repositoryNames: names, view: isFirstLoad() ? view : await view };
};

async function fetchView(path: string, repository: string | null, reference: string | null): Promise<InitialView> {
	if (reference !== null) {
		const manifest = await fetchManifest(path, reference);
		const revision = manifest.digest ? await fetchRevision(path, manifest.digest) : null;
		return { kind: 'manifest', manifest, revision };
	}
	const [revisions, uploads, namespaces] = await Promise.all([
		fetchRevisions(path, 'tag', 'asc', 0, PAGE),
		fetchUploads(path, 0, PAGE),
		repository === null ? Promise.resolve(null) : fetchNamespaces(repository, path, 'asc', 0, PAGE)
	]);
	return { kind: 'browse', revisions, uploads, namespaces };
}
