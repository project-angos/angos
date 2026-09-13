<script lang="ts">
	import { onMount } from 'svelte';
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { getRegistryName } from '$lib/config.svelte';
	import { fetchRepositories, type RepositoryInfo } from '$lib/api';
	import { isInteractiveTarget, pathUrl } from '$lib/utils';
	import Card from '$lib/components/Card.svelte';
	import LoadingState from '$lib/components/LoadingState.svelte';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import Breadcrumb from '$lib/components/Breadcrumb.svelte';

	let repositories: RepositoryInfo[] = $state([]);
	let loading = $state(true);
	let error: string | null = $state(null);

	onMount(async () => {
		const result = await fetchRepositories();
		if (result.error) {
			error = result.error;
		} else if (result.data) {
			repositories = result.data.repositories;
		}
		loading = false;
	});

	const hosted = $derived(repositories.filter((repo) => !repo.pull_through_cache));
	const caches = $derived(repositories.filter((repo) => repo.pull_through_cache));

	function open(event: MouseEvent, name: string) {
		if (!isInteractiveTarget(event)) goto(pathUrl(name));
	}
</script>

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
					<th>Name</th>
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
	</Card>

	{#if caches.length > 0}
		<Card title="Pull-through caches" count={caches.length}>
			<table>
				<thead>
					<tr>
						<th>Name</th>
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
		</Card>
	{/if}
{/if}
