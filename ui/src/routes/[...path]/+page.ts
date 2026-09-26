import { redirect } from '@sveltejs/kit';
import { base } from '$app/paths';
import { fetchRepositoryNames } from '$lib/api';
import { resolveRepository } from '$lib/utils';
import type { PageLoad } from './$types';

export interface BrowseParams {
	/** Full registry path being browsed, never empty. */
	path: string;
	/** Repository owning `path`, or `null` above every configured one. */
	repository: string | null;
	/** Tag or digest when a single manifest is being viewed. */
	reference: string | null;
	/** Configured repository names, reused to list what sits above one. */
	repositoryNames: string[];
}

export const load: PageLoad = async ({ params }): Promise<BrowseParams> => {
	// Empty segments come from a trailing or doubled slash and name nothing.
	const segments = params.path.split('/').filter(Boolean);
	if (segments.length === 0) {
		redirect(307, `${base}/`);
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
	const names = (await fetchRepositoryNames()).data ?? [];

	return { path, repository: resolveRepository(names, path), reference, repositoryNames: names };
};
