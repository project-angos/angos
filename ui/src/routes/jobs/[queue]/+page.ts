import { error } from '@sveltejs/kit';
import { JOB_QUEUES, type JobQueue } from '#lib/api.js';
import type { PageLoad } from './$types';

export interface JobsParams {
	queue: JobQueue;
}

export const load: PageLoad = ({ params }): JobsParams => {
	const queue = JOB_QUEUES.find((name) => name === params.queue);
	if (!queue) {
		error(404, `No job queue is named '${params.queue}'`);
	}
	return { queue };
};
