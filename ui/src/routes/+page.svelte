<script lang="ts">
	import { untrack } from 'svelte';
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import { getRegistryName } from '#lib/config.svelte.js';
	import { PAGE, fetchRepositories, type FetchResult, type RepositoriesResponse, type RepositoryInfo, type SortOrder } from '#lib/api.js';
	import { isInteractiveTarget } from '#lib/utils.js';
	import Card from '#lib/components/Card.svelte';
	import LoadingState from '#lib/components/LoadingState.svelte';
	import ErrorState from '#lib/components/ErrorState.svelte';
	import Breadcrumb from '#lib/components/Breadcrumb.svelte';
	import SortHeader from '#lib/components/SortHeader.svelte';
	import type { RepositoriesParams } from './+page';

	let { data }: { data: RepositoriesParams } = $props();

	let repositories: RepositoryInfo[] = $state([]);
	// Only on the first load, which renders before its data arrives.
	let loading = $state(false);
	let error: string | null = $state(null);
	let order: SortOrder = $state('asc');
	let sorting = $state(false);
	let next: number | undefined = $state(undefined);
	let loadingMore = $state(false);
	// Monotonic token: a re-sort's response supersedes any still in flight.
	let loadToken = 0;

	// The route's load fetched the first page, in the default order.
	$effect.pre(() => {
		const result = data.repositories;
		untrack(() => {
			const token = ++loadToken;
			order = 'asc';
			if (result instanceof Promise) {
				loading = true;
				result.then((resolved) => {
					if (token === loadToken) apply(resolved);
				});
			} else {
				apply(result);
			}
		});
	});

	function apply(result: FetchResult<RepositoriesResponse>) {
		loading = false;
		sorting = false;
		error = result.error && `Could not load the repositories (${result.error}).`;
		if (result.data) {
			repositories = result.data.repositories;
			next = result.data.next;
		}
	}

	async function loadMore() {
		if (next === undefined) return;
		const token = loadToken;
		loadingMore = true;
		const result = await fetchRepositories(order, next, PAGE);
		loadingMore = false;
		if (token !== loadToken) return;
		if (result.error) {
			error = `Could not load more repositories (${result.error}).`;
		} else if (result.data) {
			repositories = [...repositories, ...result.data.repositories];
			next = result.data.next;
		}
	}

	// A re-sort keeps as many rows as are on screen.
	async function sort() {
		order = order === 'asc' ? 'desc' : 'asc';
		sorting = true;
		const token = ++loadToken;
		const result = await fetchRepositories(order, 0, Math.max(repositories.length, PAGE));
		if (token === loadToken) apply(result);
	}

	const hosted = $derived(repositories.filter((repo) => !repo.pull_through_cache));
	const caches = $derived(repositories.filter((repo) => repo.pull_through_cache));

	function open(event: MouseEvent, name: string) {
		if (!isInteractiveTarget(event)) goto(resolve(name));
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

{#snippet features(repo: RepositoryInfo)}
	{#if repo.immutable_tags}
		<span class="badge immutable">Immutable</span>
	{:else}
		<span class="no-features">-</span>
	{/if}
{/snippet}

<svelte:head>
	<title>{getRegistryName()} &gt; Repositories</title>
</svelte:head>

<Breadcrumb items={[{ label: 'Repositories', href: resolve('/') }]} />

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
						<td><a class="row-link" href={resolve(repo.name)}>{repo.name}</a></td>
						<td>{@render features(repo)}</td>
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
							<td><a class="row-link" href={resolve(repo.name)}>{repo.name}</a></td>
							<td class="mono">{repo.upstream_urls.join(', ')}</td>
							<td>{@render features(repo)}</td>
							<td>{repo.namespace_count > 0 ? repo.namespace_count : '-'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
			{@render more()}
		</Card>
	{/if}
{/if}
