import { error } from '@sveltejs/kit';
import {
	JOB_QUEUES,
	PAGE,
	fetchFailedJobs,
	fetchJobs,
	type FailedJobsResponse,
	type FetchResult,
	type JobQueue,
	type JobsResponse
} from '#lib/api.js';
import { isFirstLoad } from '#lib/utils.js';
import type { PageLoad } from './$types';

export interface JobLists {
	pending: FetchResult<JobsResponse>;
	failed: FetchResult<FailedJobsResponse>;
}

export interface JobsParams {
	queue: JobQueue;
	/** Still pending on the first load, which renders before it arrives. */
	lists: JobLists | Promise<JobLists>;
}

export const load: PageLoad = async ({ params }): Promise<JobsParams> => {
	const queue = JOB_QUEUES.find((name) => name === params.queue);
	if (!queue) {
		error(404, `No job queue is named '${params.queue}'`);
	}
	const lists = Promise.all([fetchJobs(queue, PAGE), fetchFailedJobs(queue, PAGE)]).then(
		([pending, failed]) => ({ pending, failed })
	);
	return { queue, lists: isFirstLoad() ? lists : await lists };
};
