<script lang="ts">
	import { untrack } from 'svelte';
	import { base } from '$app/paths';
	import { getRegistryName } from '$lib/config.svelte';
	import {
		fetchJobs,
		fetchFailedJobs,
		retryJob,
		deleteJob,
		JOB_QUEUES,
		type JobEntry,
		type FailedJobEntry,
		type JobState
	} from '$lib/api';
	import { formatTimeAgo } from '$lib/utils';
	import Breadcrumb from '$lib/components/Breadcrumb.svelte';
	import Card from '$lib/components/Card.svelte';
	import LoadingState from '$lib/components/LoadingState.svelte';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import DeleteButton from '$lib/components/DeleteButton.svelte';
	import type { JobsParams } from './+page';

	let { data }: { data: JobsParams } = $props();

	const PAGE = 100;

	const queue = $derived(data.queue);

	let pending: JobEntry[] = $state([]);
	let pendingNext: string | undefined = $state(undefined);
	let failed: FailedJobEntry[] = $state([]);
	let failedNext: string | undefined = $state(undefined);

	let loading = $state(true);
	let error: string | null = $state(null);
	let actionError: string | null = $state(null);

	let loadToken = 0;
	let pendingMore = $state(false);
	let failedMore = $state(false);

	// storage_key currently being mutated (disables its row controls).
	let busyKey: string | null = $state(null);
	// storage_key whose delete is awaiting confirmation.
	let confirmKey: string | null = $state(null);

	async function loadPending(reset: boolean): Promise<void> {
		const token = loadToken;
		const result = await fetchJobs(queue, PAGE, reset ? undefined : pendingNext);
		if (token !== loadToken) return;
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			pending = reset ? result.data.jobs : [...pending, ...result.data.jobs];
			pendingNext = result.data.next;
		}
	}

	async function loadFailed(reset: boolean): Promise<void> {
		const token = loadToken;
		const result = await fetchFailedJobs(queue, PAGE, reset ? undefined : failedNext);
		if (token !== loadToken) return;
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			failed = reset ? result.data.failed : [...failed, ...result.data.failed];
			failedNext = result.data.next;
		}
	}

	async function refresh(): Promise<void> {
		loadToken += 1;
		loading = true;
		error = null;
		actionError = null;
		confirmKey = null;
		await Promise.all([loadPending(true), loadFailed(true)]);
		loading = false;
	}

	// Loads the queue the URL names, again whenever it changes.
	$effect(() => {
		void data.queue;
		untrack(() => void refresh());
	});

	async function onRetry(key: string): Promise<void> {
		busyKey = key;
		actionError = null;
		const err = await retryJob(queue, key);
		busyKey = null;
		if (err) {
			actionError = `Retry failed (${err}); list refreshed.`;
		}
		await refresh();
	}

	async function onDelete(state: JobState, key: string): Promise<void> {
		busyKey = key;
		actionError = null;
		const err = await deleteJob(queue, state, key);
		busyKey = null;
		if (err) {
			actionError = `Delete failed (${err}); list refreshed.`;
		}
		await refresh();
	}

	async function loadMorePending(): Promise<void> {
		pendingMore = true;
		await loadPending(false);
		pendingMore = false;
	}

	async function loadMoreFailed(): Promise<void> {
		failedMore = true;
		await loadFailed(false);
		failedMore = false;
	}

	function backoffPending(job: JobEntry): boolean {
		return new Date(job.not_before).getTime() > Date.now();
	}
</script>

<svelte:head>
	<title>{getRegistryName()} &gt; Jobs &gt; {queue}</title>
</svelte:head>

<Breadcrumb items={[{ label: 'Jobs', href: `${base}/jobs` }, { label: queue }]} />

<h1>Jobs</h1>
<p class="lede">Queued and failed background work, by queue.</p>

<div class="toolbar">
	<nav class="view-toggle" aria-label="Job queue">
		{#each JOB_QUEUES as name (name)}
			<a href="{base}/jobs/{name}" aria-current={queue === name ? 'page' : undefined}>{name}</a>
		{/each}
	</nav>
	<button class="secondary" onclick={refresh} disabled={loading}>
		{loading ? 'Refreshing...' : 'Refresh'}
	</button>
</div>

{#if actionError}
	<div class="action-error">{actionError}</div>
{/if}

{#if loading && pending.length === 0 && failed.length === 0}
	<LoadingState message="Loading jobs" />
{:else if error}
	<ErrorState message={error} />
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
								{#if backoffPending(job)}
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
									onconfirm={() => onDelete('pending', job.storage_key)}
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
										onclick={() => onRetry(job.storage_key)}
										disabled={busyKey === job.storage_key}
									>
										retry
									</button>
									<DeleteButton
										isConfirming={confirmKey === job.storage_key}
										disabled={busyKey === job.storage_key}
										onrequestconfirm={() => (confirmKey = job.storage_key)}
										oncancel={() => (confirmKey = null)}
										onconfirm={() => onDelete('failed', job.storage_key)}
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

	.load-more {
		display: flex;
		justify-content: center;
		padding: 0.625rem;
		border-top: 1px solid var(--border);
	}
</style>
