<script lang="ts">
	import { onMount } from 'svelte';
	// Aliased: the component's own generated type owns the `PullHistory` name here.
	import { fetchPullHistory, type PullHistory as PullHistoryBody } from '#lib/api.js';
	import { formatRetention, formatTimeAgo } from '#lib/utils.js';
	import Card from './Card.svelte';
	import LoadingState from './LoadingState.svelte';
	import ErrorState from './ErrorState.svelte';

	interface Props {
		namespace: string;
		/** The reference this view was addressed by, which is how pulls are keyed. */
		target: string;
	}

	let { namespace, target }: Props = $props();

	let loading = $state(true);
	let error: string | null = $state(null);
	let history: PullHistoryBody | null = $state(null);
	let loadingMore = $state(false);

	onMount(async () => {
		const result = await fetchPullHistory(namespace, target);
		loading = false;
		if (result.error) {
			error = result.error;
		} else {
			history = result.data;
		}
	});

	async function loadMore() {
		if (!history?.next) return;
		loadingMore = true;
		const result = await fetchPullHistory(namespace, target, history.next);
		loadingMore = false;
		if (result.error || !result.data) {
			error = result.error;
		} else {
			history = { ...result.data, entries: [...history.entries, ...result.data.entries] };
		}
	}

	// The retention is only known once the registry has answered, so the label
	// states it from the response rather than from a compiled-in assumption.
	const title = $derived.by(() => {
		if (!history) return 'Pull history';
		const age = history.max_age_secs === undefined ? '' : `, up to ${formatRetention(history.max_age_secs)}`;
		return `Pull history (last ${history.max_pulls} pulls${age})`;
	});
</script>

<Card {title}>
	{#if loading}
		<LoadingState message="Loading pull history" />
	{:else if error}
		<ErrorState message="Could not load pull history ({error})." />
	{:else if history}
		<table>
			<thead>
				<tr>
					<th>Client</th>
					<th class="col-medium">IP address</th>
					<th class="col-medium">Pulled</th>
				</tr>
			</thead>
			<tbody>
				{#each history.entries as entry}
					<tr>
						<td>
							{#if entry.method}
								<span class="badge method {entry.method}">{entry.method}</span>
							{/if}
							{#if entry.method !== 'anonymous'}{entry.client}{/if}
						</td>
						<td>{entry.client_ip ?? '—'}</td>
						<td title={entry.at}>{formatTimeAgo(entry.at)}</td>
					</tr>
				{:else}
					<tr>
						<!-- Not "never pulled": recording is off unless the
						     operator enables it. -->
						<td colspan="3" class="empty">
							No pulls recorded. Pull recording requires
							<code>update_pull_time</code> to be enabled.
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
		{#if history.next}
			<div class="load-more">
				<button class="secondary" onclick={loadMore} disabled={loadingMore}>Load more</button>
			</div>
		{/if}
	{/if}
</Card>

<style>
	.method {
		margin-right: 0.4rem;
	}

	.method.kubernetes {
		background: var(--chip-blue-bg);
		color: var(--chip-blue-fg);
	}

	.method.oidc {
		background: var(--chip-cyan-bg);
		color: var(--chip-cyan-fg);
	}

	.method.mtls {
		background: var(--chip-green-bg);
		color: var(--chip-green-fg);
	}

	.method.token {
		background: var(--chip-purple-bg);
		color: var(--chip-purple-fg);
	}

	.method.anonymous {
		background: var(--chip-orange-bg);
		color: var(--chip-orange-fg);
	}
</style>
