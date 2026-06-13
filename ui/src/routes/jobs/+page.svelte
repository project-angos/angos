<script lang="ts">
	import { onMount } from 'svelte';
	import { base } from '$app/paths';
	import { getRegistryName } from '$lib/config.svelte';
	import {
		fetchJobs,
		fetchFailedJobs,
		retryJob,
		deleteJob,
		type JobEntry,
		type FailedJobEntry,
		type JobState,
		type JobQueue
	} from '$lib/api';
	import { formatTimeAgo } from '$lib/utils';
	import Breadcrumb from '$lib/components/Breadcrumb.svelte';
	import Card from '$lib/components/Card.svelte';
	import LoadingState from '$lib/components/LoadingState.svelte';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import DeleteButton from '$lib/components/DeleteButton.svelte';

	const PAGE = 100;

	let queue: JobQueue = $state('cache');

	let pending: JobEntry[] = $state([]);
	let pendingNext: string | undefined = $state(undefined);
	let failed: FailedJobEntry[] = $state([]);
	let failedNext: string | undefined = $state(undefined);

	let loading = $state(true);
	let error: string | null = $state(null);
	let actionError: string | null = $state(null);

	// storage_key currently being mutated (disables its row controls).
	let busyKey: string | null = $state(null);
	// storage_key whose delete is awaiting confirmation.
	let confirmKey: string | null = $state(null);

	async function loadPending(reset: boolean): Promise<void> {
		const result = await fetchJobs(queue, PAGE, reset ? undefined : pendingNext);
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			pending = reset ? result.data.jobs : [...pending, ...result.data.jobs];
			pendingNext = result.data.next;
		}
	}

	async function loadFailed(reset: boolean): Promise<void> {
		const result = await fetchFailedJobs(queue, PAGE, reset ? undefined : failedNext);
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			failed = reset ? result.data.failed : [...failed, ...result.data.failed];
			failedNext = result.data.next;
		}
	}

	async function refresh(): Promise<void> {
		loading = true;
		error = null;
		actionError = null;
		confirmKey = null;
		await Promise.all([loadPending(true), loadFailed(true)]);
		loading = false;
	}

	onMount(refresh);

	function selectQueue(next: JobQueue): void {
		if (next === queue) {
			return;
		}
		queue = next;
		void refresh();
	}

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

	function backoffPending(job: JobEntry): boolean {
		return new Date(job.not_before).getTime() > Date.now();
	}
</script>

<svelte:head>
	<title>{getRegistryName()} &gt; Jobs</title>
</svelte:head>

<Breadcrumb items={[{ label: 'Jobs', href: `${base}/jobs` }]} />

<div class="toolbar">
	<div class="view-toggle" role="group" aria-label="Job queue">
		<button
			class:active={queue === 'cache'}
			onclick={() => selectQueue('cache')}
			disabled={loading}
		>
			cache
		</button>
		<button
			class:active={queue === 'replication'}
			onclick={() => selectQueue('replication')}
			disabled={loading}
		>
			replication
		</button>
	</div>
	<button class="refresh" onclick={refresh} disabled={loading}>
		{loading ? 'Refreshing…' : 'Refresh'}
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
				<button onclick={() => loadPending(false)} disabled={loading}>Load more</button>
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
										class="retry"
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
				<button onclick={() => loadFailed(false)} disabled={loading}>Load more</button>
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

	.refresh {
		padding: 0.4rem 0.9rem;
		border: 1px solid var(--color-border);
		border-radius: 6px;
		background: var(--color-surface);
		color: inherit;
		cursor: pointer;
	}

	.refresh:hover:not(:disabled) {
		background: var(--color-surface-hover, var(--color-surface));
	}

	.action-error {
		margin-bottom: 1rem;
		padding: 0.6rem 0.9rem;
		border: 1px solid var(--color-warning, #b45309);
		border-radius: 6px;
		color: var(--color-warning, #b45309);
		font-size: 0.9rem;
	}

	.mono {
		font-family: var(--font-mono, monospace);
		font-size: 0.85rem;
	}

	.error-cell {
		max-width: 28rem;
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		color: var(--color-text-muted, inherit);
		font-size: 0.85rem;
	}

	.row-actions {
		display: flex;
		gap: 0.5rem;
		align-items: center;
		justify-content: flex-end;
	}

	.col-actions {
		text-align: right;
		width: 1%;
		white-space: nowrap;
	}

	.retry {
		padding: 0.25rem 0.6rem;
		border: 1px solid var(--color-border);
		border-radius: 4px;
		background: var(--color-surface);
		color: inherit;
		cursor: pointer;
	}

	.retry:hover:not(:disabled) {
		background: var(--color-surface-hover, var(--color-surface));
	}

	.retry:disabled {
		opacity: 0.5;
		cursor: default;
	}

	.load-more {
		display: flex;
		justify-content: center;
		margin-top: 0.75rem;
	}

	.load-more button {
		padding: 0.35rem 1rem;
		border: 1px solid var(--color-border);
		border-radius: 6px;
		background: var(--color-surface);
		color: inherit;
		cursor: pointer;
	}

	.badge.backoff {
		margin-left: 0.4rem;
		background: var(--color-warning-bg, #fef3c7);
		color: var(--color-warning, #b45309);
	}
</style>
