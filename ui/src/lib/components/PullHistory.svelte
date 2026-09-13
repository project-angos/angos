<script lang="ts">
	// Aliased: the component's own generated type owns the `PullHistory` name here.
	import { untrack } from 'svelte';
	import { fetchPullHistory, type PullHistory as PullHistoryBody } from '$lib/api';
	import { formatRetention, formatTimeAgo } from '$lib/utils';
	import Card from './Card.svelte';
	import AnnotationToggle from './AnnotationToggle.svelte';
	import LoadingState from './LoadingState.svelte';
	import ErrorState from './ErrorState.svelte';

	interface Props {
		namespace: string;
		/** The reference this view was addressed by, which is how pulls are keyed. */
		target: string;
		/** Load at once and drop the toggle, for a dedicated tab. */
		open?: boolean;
	}

	let { namespace, target, open = false }: Props = $props();

	// The registry caps the listing; at exactly the cap, older pulls exist that
	// the response does not carry.
	const CAP = 100;

	let expanded = $state(untrack(() => open));
	let loading = $state(false);
	let error: string | null = $state(null);
	let history: PullHistoryBody | null = $state(null);

	// Listing a namespace renders one of these per manifest, so the request is
	// held until it is asked for, rather than fanning out on load. The result
	// is then kept: the section is an audit view, not a live one.
	async function load() {
		if (history || loading) return;
		loading = true;
		error = null;
		const result = await fetchPullHistory(namespace, target);
		loading = false;
		if (result.error) {
			error = result.error;
		} else {
			history = result.data;
		}
	}

	function toggle() {
		expanded = !expanded;
		if (expanded) load();
	}

	// In its own tab it is the reason the tab was opened, so it loads at once.
	$effect(() => {
		if (open) load();
	});

	// The retention is only known once the registry has answered, so the label
	// states it from the response rather than from a compiled-in assumption.
	const title = $derived.by(() =>
		history ? `Pull history (retained ${formatRetention(history.window_secs)})` : 'Pull history'
	);
</script>

{#snippet toggleAction()}
	<AnnotationToggle {expanded} label="pull history" ontoggle={toggle} />
{/snippet}

<Card {title} headerActions={open ? undefined : toggleAction}>
	{#if expanded}
		{#if loading}
			<LoadingState message="Loading pull history" />
		{:else if error}
			<ErrorState message="Could not load pull history ({error})." />
		{:else if history}
			<table>
				<thead>
					<tr>
						<th>Client</th>
						<th class="col-medium">Pulled</th>
					</tr>
				</thead>
				<tbody>
					{#each history.entries as entry}
						<tr>
							<td>{entry.client}</td>
							<td title={entry.at}>{formatTimeAgo(entry.at)}</td>
						</tr>
					{:else}
						<tr>
							<!-- Not "never pulled": recording is off unless the
							     operator enables it, and only the newest entry
							     outlives the window. -->
							<td colspan="2" class="empty">
								No pulls recorded in the retention window (last
								{formatRetention(history.window_secs)}). Pull recording requires
								<code>update_pull_time</code> to be enabled.
							</td>
						</tr>
					{/each}
				</tbody>
				{#if history.entries.length >= CAP}
					<tfoot>
						<tr>
							<td colspan="2">
								Showing the newest {CAP} pulls; older ones are not listed.
							</td>
						</tr>
					</tfoot>
				{/if}
			</table>
		{/if}
	{/if}
</Card>
