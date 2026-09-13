<script lang="ts">
	import { fetchManifest, type Manifest } from '$lib/api';
	import { formatTimeAgo, manifestUrl } from '$lib/utils';
	import DigestLink from './DigestLink.svelte';
	import ScanReport from './ScanReport.svelte';
	import LoadingState from './LoadingState.svelte';
	import ErrorState from './ErrorState.svelte';

	interface Report {
		/** The platform the report covers, when the subject is a multi-platform index. */
		label?: string;
		digest: string;
		annotations?: Record<string, string>;
	}

	interface Props {
		namespace: string;
		reports: Report[];
		/** The platform label the URL asks for, the first report when it names none. */
		platform?: string;
		/** The report manifest already loaded, when the page is itself a report. */
		ownManifest?: Manifest | null;
		ownDigest?: string | null;
	}

	let { namespace, reports, platform = '', ownManifest = null, ownDigest = null }: Props = $props();

	const selected = $derived(
		Math.max(
			0,
			reports.findIndex((report) => report.label === platform)
		)
	);
	const current = $derived(reports[selected]);
	const scannedAt = $derived(current?.annotations?.['org.opencontainers.image.created']);
	const scanner = $derived(current?.annotations?.['io.angos.scan.scanner']);
	const scanned = $derived(
		scannedAt ? `scanned ${formatTimeAgo(scannedAt)}${scanner ? ` by ${scanner}` : ''}` : ''
	);

	let manifest = $state<Manifest | null>(null);
	let error = $state<string | null>(null);
	let loading = $state(true);

	// The report manifest holds the SARIF layer the findings are read from; the
	// page's own report is already in hand, a referrer's is fetched by digest.
	$effect(() => {
		const digest = current?.digest;
		manifest = null;
		error = null;
		loading = true;
		if (!digest) {
			loading = false;
			return;
		}
		if (ownManifest && ownDigest === digest) {
			manifest = ownManifest;
			loading = false;
			return;
		}
		fetchManifest(namespace, digest).then((result) => {
			manifest = result.manifest;
			error = result.error;
			loading = false;
		});
	});
</script>

{#if reports.length > 1}
	<nav class="view-toggle platforms" aria-label="Platform">
		{#each reports as report, i (report.digest)}
			<a href="#vulnerabilities/{report.label}" aria-current={i === selected ? 'page' : undefined}
				>{report.label ?? `report ${i + 1}`}</a
			>
		{/each}
	</nav>
{/if}

{#if current}
	<p class="report-of">
		Report artifact <DigestLink digest={current.digest} href={manifestUrl(namespace, current.digest)} />
		{#if scanned}<span>· {scanned}</span>{/if}
	</p>
{/if}

{#if error}
	<ErrorState message={error} />
{:else if loading}
	<LoadingState message="Loading the report" />
{:else if manifest}
	<ScanReport {namespace} {manifest} />
{/if}

<style>
	.platforms {
		margin-bottom: 0.75rem;
	}
	.report-of {
		display: flex;
		align-items: center;
		gap: 0.375rem;
		margin: 0 0 0.75rem;
		color: var(--muted);
		font-size: 0.875rem;
	}
</style>
