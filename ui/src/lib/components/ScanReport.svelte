<script lang="ts">
	import type { Manifest } from '#lib/api.js';
	import { fetchBlobJson } from '#lib/api.js';
	import { parseSarif, SEVERITIES, type ParsedReport, type Severity } from '#lib/utils.js';
	import Card from './Card.svelte';
	import LoadingState from './LoadingState.svelte';
	import ErrorState from './ErrorState.svelte';

	interface Props {
		namespace: string;
		manifest: Manifest;
	}

	let { namespace, manifest }: Props = $props();
	let report = $state<ParsedReport | null>(null);
	let error = $state<string | null>(null);
	let loading = $state(true);
	let hidden = $state<Set<Severity>>(new Set());

	const layer = $derived(
		manifest.layers?.find((l) => l.mediaType === 'application/sarif+json') ?? manifest.layers?.[0]
	);

	$effect(() => {
		const digest = layer?.digest;
		if (!digest) {
			error = 'The report has no SARIF layer';
			loading = false;
			return;
		}
		loading = true;
		fetchBlobJson<unknown>(namespace, digest).then((result) => {
			if (result.data) {
				report = parseSarif(result.data);
			} else {
				error = result.error ?? 'Failed to load the report';
			}
			loading = false;
		});
	});

	function toggle(severity: Severity) {
		const next = new Set(hidden);
		if (next.has(severity)) next.delete(severity);
		else next.add(severity);
		hidden = next;
	}

	const shown = $derived(report?.findings.filter((f) => !hidden.has(f.severity)) ?? []);
</script>

<Card title="Vulnerability report" count={report?.findings.length}>
	{#if loading}
		<LoadingState message="Loading the report" />
	{:else if error}
		<ErrorState message={error} />
	{:else if report}
		<div class="toolbar">
			<span class="scan-summary">
				{#each SEVERITIES as severity}
					{#if report.summary.counts[severity] > 0}
						<button
							class="severity severity-{severity}"
							class:muted={hidden.has(severity)}
							onclick={() => toggle(severity)}
							title={hidden.has(severity) ? 'Show' : 'Hide'}
						>
							{report.summary.counts[severity]} {severity}
						</button>
					{/if}
				{/each}
				{#if report.findings.length === 0}
					<span class="severity severity-clean">no findings</span>
				{/if}
			</span>
			{#if report.summary.scanner}
				<span class="scanner">{report.summary.scanner}</span>
			{/if}
		</div>
		{#if shown.length > 0}
			<table>
				<thead>
					<tr>
						<th>Severity</th>
						<th>Vulnerability</th>
						<th>Package</th>
						<th>Installed</th>
						<th>Fixed</th>
						<th>Description</th>
					</tr>
				</thead>
				<tbody>
					{#each shown as finding}
						<tr>
							<td><span class="severity severity-{finding.severity}">{finding.severity}</span></td>
							<td>
								{#if finding.url}
									<a href={finding.url} target="_blank" rel="noreferrer"><code>{finding.id}</code></a>
								{:else}
									<code>{finding.id}</code>
								{/if}
							</td>
							<td>{finding.pkg ?? '-'}</td>
							<td>{finding.installed ?? '-'}</td>
							<td>{finding.fixed ?? '-'}</td>
							<td class="description">{finding.description}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/if}
</Card>

<style>
	.toolbar {
		display: flex;
		justify-content: space-between;
		align-items: center;
		gap: 0.5rem;
		padding: 0.625rem 0.875rem;
		border-bottom: 1px solid var(--border);
	}
	.scanner {
		color: var(--muted);
		font-size: 0.8125rem;
	}
	.description {
		max-width: 32rem;
	}
	button.severity {
		box-shadow: none;
	}
	button.severity.muted {
		opacity: 0.35;
	}
</style>
