<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { getRegistryName } from '$lib/config.svelte';
	import { PAGE, fetchRepositories, type RepositoryInfo, type SortOrder } from '$lib/api';
	import { isInteractiveTarget, pathUrl } from '$lib/utils';
	import Card from '$lib/components/Card.svelte';
	import LoadingState from '$lib/components/LoadingState.svelte';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import Breadcrumb from '$lib/components/Breadcrumb.svelte';
	import SortHeader from '$lib/components/SortHeader.svelte';

	let repositories: RepositoryInfo[] = $state([]);
	let loading = $state(true);
	let error: string | null = $state(null);
	let order: SortOrder = $state('asc');
	let sorting = $state(false);
	let next: number | undefined = $state(undefined);
	let loadingMore = $state(false);
	// Monotonic token: a re-sort's response supersedes any still in flight.
	let loadToken = 0;

	// A re-sort keeps as many rows as are on screen.
	async function load() {
		const token = ++loadToken;
		const result = await fetchRepositories(order, 0, Math.max(repositories.length, PAGE));
		if (token !== loadToken) return;
		sorting = false;
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			repositories = result.data.repositories;
			next = result.data.next;
		}
		loading = false;
	}

	async function loadMore() {
		if (next === undefined) return;
		const token = loadToken;
		loadingMore = true;
		const result = await fetchRepositories(order, next, PAGE);
		loadingMore = false;
		if (token !== loadToken) return;
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			repositories = [...repositories, ...result.data.repositories];
			next = result.data.next;
		}
	}

	onMount(load);

	function sort() {
		order = order === 'asc' ? 'desc' : 'asc';
		sorting = true;
		load();
	}

	const hosted = $derived(repositories.filter((repo) => !repo.pull_through_cache));
	const caches = $derived(repositories.filter((repo) => repo.pull_through_cache));

	function open(event: MouseEvent, name: string) {
		if (!isInteractiveTarget(event)) goto(pathUrl(name));
	}
</script>

<!-- Pages mix both kinds, so the last card shown carries the control. -->
{#snippet more()}
	{#if next !== undefined}
		<div class="load-more">
			<button class="secondary" onclick={loadMore} disabled={loadingMore}>Load more</button>
		</div>
	{/if}
{/snippet}

<svelte:head>
	<title>{getRegistryName()} &gt; Repositories</title>
</svelte:head>

<Breadcrumb items={[{ label: 'Repositories', href: `${base}/` }]} />

<h1>Repositories</h1>
<p class="lede">Every repository this registry serves.</p>

{#if loading}
	<LoadingState message="Loading repositories" />
{:else if error}
	<ErrorState message={error} />
{:else}
	<Card title="Hosted" count={hosted.length}>
		<table>
			<thead>
				<tr>
					<SortHeader label="Name" {order} busy={sorting} onsort={sort} />
					<th>Features</th>
					<th class="col-medium">Namespaces</th>
				</tr>
			</thead>
			<tbody>
				{#each hosted as repo (repo.name)}
					<tr class="clickable" onclick={(event) => open(event, repo.name)}>
						<td><a class="row-link" href={pathUrl(repo.name)}>{repo.name}</a></td>
						<td>
							{#if repo.immutable_tags}
								<span class="badge immutable">Immutable</span>
							{:else}
								<span class="no-features">-</span>
							{/if}
						</td>
						<td>{repo.namespace_count > 0 ? repo.namespace_count : '-'}</td>
					</tr>
				{:else}
					<tr>
						<td colspan="3" class="empty">No hosted repositories</td>
					</tr>
				{/each}
			</tbody>
		</table>
		{#if caches.length === 0}
			{@render more()}
		{/if}
	</Card>

	{#if caches.length > 0}
		<Card title="Pull-through caches" count={caches.length}>
			<table>
				<thead>
					<tr>
						<SortHeader label="Name" {order} busy={sorting} onsort={sort} />
						<th>Upstream</th>
						<th>Features</th>
						<th class="col-medium">Namespaces</th>
					</tr>
				</thead>
				<tbody>
					{#each caches as repo (repo.name)}
						<tr class="clickable" onclick={(event) => open(event, repo.name)}>
							<td><a class="row-link" href={pathUrl(repo.name)}>{repo.name}</a></td>
							<td class="mono">{repo.upstream_urls.join(', ')}</td>
							<td>
								{#if repo.immutable_tags}
									<span class="badge immutable">Immutable</span>
								{:else}
									<span class="no-features">-</span>
								{/if}
							</td>
							<td>{repo.namespace_count > 0 ? repo.namespace_count : '-'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
			{@render more()}
		</Card>
	{/if}
{/if}
