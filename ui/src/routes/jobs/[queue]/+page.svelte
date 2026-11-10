<script lang="ts">
	import { untrack } from 'svelte';
	import { resolve } from '$app/paths';
	import { getRegistryName } from '#lib/config.svelte.js';
	import {
		PAGE,
		fetchJobs,
		fetchFailedJobs,
		retryJob,
		deleteJob,
		JOB_QUEUES,
		type JobEntry,
		type FailedJobEntry,
		type FailedJobsResponse,
		type FetchResult,
		type JobsResponse
	} from '#lib/api.js';
	import { formatTimeAgo } from '#lib/utils.js';
	import Breadcrumb from '#lib/components/Breadcrumb.svelte';
	import Card from '#lib/components/Card.svelte';
	import LoadingState from '#lib/components/LoadingState.svelte';
	import ErrorState from '#lib/components/ErrorState.svelte';
	import DeleteButton from '#lib/components/DeleteButton.svelte';
	import type { JobLists, JobsParams } from './+page';

	let { data }: { data: JobsParams } = $props();

	const queue = $derived(data.queue);

	let pending: JobEntry[] = $state([]);
	let pendingNext: string | undefined = $state(undefined);
	let failed: FailedJobEntry[] = $state([]);
	let failedNext: string | undefined = $state(undefined);

	let loading = $state(false);
	// Only on the first load, which renders before its data arrives.
	let firstLoading = $state(false);
	let error: string | null = $state(null);
	let actionError: string | null = $state(null);

	let loadToken = 0;
	let pendingMore = $state(false);
	let failedMore = $state(false);

	let busyKey: string | null = $state(null);
	let confirmKey: string | null = $state(null);

	// A failed first page leaves nothing to show; a failed next page keeps the list.
	async function loadPending(reset: boolean, fetched?: FetchResult<JobsResponse>) {
		const token = loadToken;
		const result = fetched ?? (await fetchJobs(queue, PAGE, reset ? undefined : pendingNext));
		if (token !== loadToken) return;
		if (result.error && reset) {
			error = result.error;
		} else if (result.error) {
			actionError = `Could not load more pending jobs (${result.error}).`;
		} else if (result.data) {
			pending = reset ? result.data.jobs : [...pending, ...result.data.jobs];
			pendingNext = result.data.next;
		}
	}

	async function loadFailed(reset: boolean, fetched?: FetchResult<FailedJobsResponse>) {
		const token = loadToken;
		const result = fetched ?? (await fetchFailedJobs(queue, PAGE, reset ? undefined : failedNext));
		if (token !== loadToken) return;
		if (result.error && reset) {
			error = result.error;
		} else if (result.error) {
			actionError = `Could not load more failed jobs (${result.error}).`;
		} else if (result.data) {
			failed = reset ? result.data.failed : [...failed, ...result.data.failed];
			failedNext = result.data.next;
		}
	}

	// Refreshes in place: the lists stay on screen until the new ones arrive.
	async function refresh() {
		loadToken += 1;
		loading = true;
		error = null;
		confirmKey = null;
		await Promise.all([loadPending(true), loadFailed(true)]);
		loading = false;
	}

	// The route's load fetched the queue the URL names.
	$effect.pre(() => {
		const { lists } = data;
		untrack(() => {
			const token = ++loadToken;
			error = null;
			actionError = null;
			confirmKey = null;
			if (lists instanceof Promise) {
				firstLoading = true;
				lists.then((resolved) => {
					if (token === loadToken) show(resolved);
				});
			} else {
				show(lists);
			}
		});
	});

	function show(lists: JobLists) {
		firstLoading = false;
		loadPending(true, lists.pending);
		loadFailed(true, lists.failed);
	}

	async function act(key: string, verb: 'retry' | 'delete', request: () => Promise<string | null>) {
		busyKey = key;
		actionError = null;
		const err = await request();
		busyKey = null;
		if (err) {
			actionError = `Could not ${verb} the job (${err}).`;
		}
		await refresh();
	}

	async function loadMorePending() {
		pendingMore = true;
		await loadPending(false);
		pendingMore = false;
	}

	async function loadMoreFailed() {
		failedMore = true;
		await loadFailed(false);
		failedMore = false;
	}
</script>

<svelte:head>
	<title>{getRegistryName()} &gt; Jobs &gt; {queue}</title>
</svelte:head>

<Breadcrumb items={[{ label: 'Jobs', href: resolve('/jobs') }, { label: queue }]} />

<h1>Jobs</h1>
<p class="lede">Queued and failed background work, by queue.</p>

<div class="toolbar">
	<nav class="view-toggle" aria-label="Job queue">
		{#each JOB_QUEUES as name (name)}
			<a href={resolve('/jobs/[queue]', { queue: name })} aria-current={queue === name ? 'page' : undefined}>{name}</a>
		{/each}
	</nav>
	<button class="secondary" onclick={refresh} disabled={loading}>Refresh</button>
</div>

{#if actionError}
	<ErrorState message={actionError} />
{/if}

{#if firstLoading}
	<LoadingState message="Loading jobs" />
{:else if error}
	<ErrorState message="Could not load the jobs ({error})." />
{:else}
	<Card title="Pending & In-flight Jobs" count={pending.length}>
		<table>
			<thead>
				<tr>
					<th>Kind</th>
					<th>Lock key</th>
					<th class="col-small">Attempts</th>
					<th class="col-medium">Queued</th>
					<th class="col-actions"></th>
				</tr>
			</thead>
			<tbody>
				{#if pending.length === 0}
					<tr><td colspan="5" class="empty">No pending jobs</td></tr>
				{:else}
					{#each pending as job (job.storage_key)}
						<tr>
							<td>{job.kind}</td>
							<td class="mono">{job.lock_key}</td>
							<td>
								{job.attempts}/{job.max_attempts}
								{#if new Date(job.not_before).getTime() > Date.now()}
									<span class="badge backoff">backoff</span>
								{/if}
							</td>
							<td>{formatTimeAgo(job.created_at)}</td>
							<td class="col-actions">
								<DeleteButton
									isConfirming={confirmKey === job.storage_key}
									disabled={busyKey === job.storage_key}
									onrequestconfirm={() => (confirmKey = job.storage_key)}
									oncancel={() => (confirmKey = null)}
									onconfirm={() => act(job.storage_key, 'delete', () => deleteJob(queue, 'pending', job.storage_key))}
								/>
							</td>
						</tr>
					{/each}
				{/if}
			</tbody>
		</table>
		{#if pendingNext}
			<div class="load-more">
				<button class="secondary" onclick={loadMorePending} disabled={loading || pendingMore}>Load more</button>
			</div>
		{/if}
	</Card>

	<Card title="Failed Jobs" count={failed.length} variant="warning">
		<table>
			<thead>
				<tr>
					<th>Kind</th>
					<th>Lock key</th>
					<th class="col-small">Attempts</th>
					<th class="col-medium">Failed</th>
					<th>Last error</th>
					<th class="col-actions"></th>
				</tr>
			</thead>
			<tbody>
				{#if failed.length === 0}
					<tr><td colspan="6" class="empty">No failed jobs</td></tr>
				{:else}
					{#each failed as job (job.storage_key)}
						<tr>
							<td>{job.kind}</td>
							<td class="mono">{job.lock_key}</td>
							<td>{job.attempts}/{job.max_attempts}</td>
							<td>{formatTimeAgo(job.failed_at)}</td>
							<td class="error-cell" title={job.last_error}>{job.last_error}</td>
							<td class="col-actions">
								<div class="row-actions">
									<button
										class="secondary"
										onclick={() => act(job.storage_key, 'retry', () => retryJob(queue, job.storage_key))}
										disabled={busyKey === job.storage_key}
									>
										retry
									</button>
									<DeleteButton
										isConfirming={confirmKey === job.storage_key}
										disabled={busyKey === job.storage_key}
										onrequestconfirm={() => (confirmKey = job.storage_key)}
										oncancel={() => (confirmKey = null)}
										onconfirm={() => act(job.storage_key, 'delete', () => deleteJob(queue, 'failed', job.storage_key))}
									/>
								</div>
							</td>
						</tr>
					{/each}
				{/if}
			</tbody>
		</table>
		{#if failedNext}
			<div class="load-more">
				<button class="secondary" onclick={loadMoreFailed} disabled={loading || failedMore}>Load more</button>
			</div>
		{/if}
	</Card>
{/if}

<style>
	.toolbar {
		display: flex;
		justify-content: space-between;
		align-items: center;
		margin-bottom: 1rem;
	}

	.error-cell {
		max-width: 28rem;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		color: var(--muted);
		font-size: 0.8125rem;
	}

	.row-actions {
		display: flex;
		gap: 0.375rem;
		align-items: center;
		justify-content: flex-end;
	}
</style>
