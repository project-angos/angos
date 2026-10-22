import { redirect } from '@sveltejs/kit';
import { resolve } from '$app/paths';
import type { PageLoad } from './$types';

/** Each queue has its own page; the bare path opens the first. */
export const load: PageLoad = () => {
	redirect(307, resolve('/jobs/[queue]', { queue: 'cache' }));
};
