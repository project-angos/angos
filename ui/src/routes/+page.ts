import { PAGE, fetchRepositories, type FetchResult, type RepositoriesResponse } from '#lib/api.js';
import { isFirstLoad } from '#lib/utils.js';
import type { PageLoad } from './$types';

export interface RepositoriesParams {
	/** Still pending on the first load, which renders before it arrives. */
	repositories: FetchResult<RepositoriesResponse> | Promise<FetchResult<RepositoriesResponse>>;
}

export const load: PageLoad = async (): Promise<RepositoriesParams> => {
	const repositories = fetchRepositories('asc', 0, PAGE);
	return { repositories: isFirstLoad() ? repositories : await repositories };
};
