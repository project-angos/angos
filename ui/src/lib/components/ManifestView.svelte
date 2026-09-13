<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import type { ParentRef, Manifest, ReferrerInfo } from '$lib/api';
	import {
		formatPlatform,
		formatSize,
		getTagConfirm,
		isInteractiveTarget,
		manifestUrl,
		tagConfirmKey,
		getAttestationType,
		latestScanReport,
		parseScanSummary,
		isOrasArtifact,
		getFileName,
		isFilesystemLayer
	} from '$lib/utils';
	import Card from './Card.svelte';
	import DeleteButton from './DeleteButton.svelte';
	import TagList from './TagList.svelte';
	import PlatformBadge from './PlatformBadge.svelte';
	import AttestationBadge from './AttestationBadge.svelte';
	import AnnotationToggle from './AnnotationToggle.svelte';
	import AnnotationList from './AnnotationList.svelte';
	import DigestLink from './DigestLink.svelte';
	import PullHistory from './PullHistory.svelte';
	import ScanSummary from './ScanSummary.svelte';
	import LayerBrowser from './LayerBrowser.svelte';
	import VulnTab from './VulnTab.svelte';

	interface Props {
		path: string;
		/** The tag or digest this view was addressed by; pulls are keyed by it. */
		reference: string;
		manifest: Manifest;
		digest: string | null;
		tags: string[];
		referencedBy: ParentRef[];
		childReferrers: Map<string, ReferrerInfo[]>;
		childReferrersNext: Map<string, string>;
		loadingReferrers: string | null;
		onloadmorereferrers: (childDigest: string) => void;
		deleteConfirm: string | null;
		deleting: boolean;
		ondeletetag: (tag: string) => void;
		ondeletebyhash: () => void;
		onconfirmchange: (value: string | null) => void;
		getbloburl: (blobDigest: string) => string;
	}

	let {
		path,
		reference,
		manifest,
		digest,
		tags,
		referencedBy,
		childReferrers,
		childReferrersNext,
		loadingReferrers,
		onloadmorereferrers,
		deleteConfirm,
		deleting,
		ondeletetag,
		ondeletebyhash,
		onconfirmchange,
		getbloburl
	}: Props = $props();

	let expandedAnnotations: Set<string> = $state(new Set());

	// The reports the Vulnerabilities tab shows: the manifest itself when it is
	// a report, an image's own report, or one per platform for an index.
	type Report = { label?: string; digest: string; annotations?: Record<string, string> };
	const isReport = $derived(manifest.artifactType === 'application/sarif+json');
	const latestReport = $derived(digest ? latestScanReport(childReferrers.get(digest) ?? []) : null);
	const reports = $derived.by((): Report[] => {
		if (isReport && digest) {
			return [{ digest, annotations: manifest.annotations }];
		}
		if (latestReport) {
			return [{ digest: latestReport.digest, annotations: latestReport.annotations }];
		}
		return (manifest.manifests ?? [])
			.filter((m) => !m.annotations?.['vnd.docker.reference.digest'])
			.flatMap((m) => {
				const report = latestScanReport(childReferrers.get(m.digest) ?? []);
				return report
					? [{ label: formatPlatform(m.platform), digest: report.digest, annotations: report.annotations }]
					: [];
			});
	});
	const vulnTotal = $derived(
		reports.reduce((sum, report) => sum + (parseScanSummary(report.annotations)?.total ?? 0), 0)
	);

	// A plain image's tar layers, the ones the filesystem view is built from.
	const fsLayers = $derived(
		!manifest.subject && !manifest.artifactType
			? (manifest.layers ?? []).filter((layer) => isFilesystemLayer(layer.mediaType))
			: []
	);

	// Each tab is addressed by the URL's anchor, OCI by its absence.
	type Tab = { id: string; label: string; count?: number };
	const tabs = $derived<Tab[]>([
		{ id: 'oci', label: 'OCI' },
		{ id: 'history', label: 'Pull History' },
		...(reports.length > 0 ? [{ id: 'vulnerabilities', label: 'Vulnerabilities', count: vulnTotal }] : []),
		...(fsLayers.length > 0 ? [{ id: 'filesystem', label: 'Filesystem' }] : [])
	]);
	// The anchor is the tab's id, followed for Vulnerabilities by the platform shown.
	const anchor = $derived(page.url.hash.slice(1));
	const active = $derived(anchor.split('/', 1)[0]);
	const platform = $derived(anchor.slice(active.length + 1));
	// An anchor naming a tab this manifest lacks falls back to OCI.
	const current = $derived(tabs.some((tab) => tab.id === active) ? active : 'oci');

	type LayersViewMode = 'auto' | 'files' | 'layers';
	let layersViewMode: LayersViewMode = $state('auto');

	const showFilesView = $derived(
		layersViewMode === 'auto' ? (manifest ? isOrasArtifact(manifest) : false) : layersViewMode === 'files'
	);

	function toggleAnnotations(key: string) {
		if (expandedAnnotations.has(key)) {
			expandedAnnotations.delete(key);
		} else {
			expandedAnnotations.add(key);
		}
		expandedAnnotations = new Set(expandedAnnotations);
	}

	function handleRowClick(event: MouseEvent, targetDigest: string) {
		if (isInteractiveTarget(event)) return;
		goto(manifestUrl(path, targetDigest));
	}
</script>

<nav class="tabs" aria-label="Manifest sections">
	{#each tabs as tab (tab.id)}
		<a
			href={tab.id === 'oci' ? page.url.pathname : `#${tab.id}`}
			class:active={current === tab.id}
			aria-current={current === tab.id ? 'page' : undefined}
		>
			{tab.label}{#if tab.count !== undefined}<span class="tab-badge">{tab.count}</span>{/if}
		</a>
	{/each}
</nav>

{#if current === 'oci'}
	<Card title="Manifest">
		<table>
			<tbody>
				<tr>
					<td class="label">Digest</td>
					<td>
						<DigestLink digest={digest ?? ''} href={digest ? manifestUrl(path, digest) : undefined} />
					</td>
				</tr>
				<tr>
					<td class="label">Tags</td>
					<td>
						<TagList
							{tags}
							deleteConfirm={getTagConfirm(deleteConfirm)}
							disabled={deleting}
							ondelete={ondeletetag}
							onconfirmchange={(tag) => onconfirmchange(tag ? tagConfirmKey(tag) : null)}
							getHref={(tag) => manifestUrl(path, tag)}
						/>
					</td>
				</tr>
				<tr>
					<td class="label">Media type</td>
					<td>
						{manifest.mediaType ?? 'unknown'}
						{#if manifest.annotations}
							<AnnotationToggle expanded={expandedAnnotations.has('root')} ontoggle={() => toggleAnnotations('root')} />
						{/if}
					</td>
				</tr>
				{#if manifest.annotations && expandedAnnotations.has('root')}
					<AnnotationList annotations={manifest.annotations} />
				{/if}
				{#if manifest.artifactType}
					<tr>
						<td class="label">Artifact type</td>
						<td>{manifest.artifactType}</td>
					</tr>
				{/if}
				{#if manifest.subject}
					<tr>
						<td class="label">Subject</td>
						<td>
							<DigestLink digest={manifest.subject.digest} href={manifestUrl(path, manifest.subject.digest)} />
							<span class="subject-meta">({manifest.subject.mediaType}, {formatSize(manifest.subject.size)})</span>
						</td>
					</tr>
				{/if}
				<tr>
					<td class="label">Actions</td>
					<td>
						<DeleteButton
							isConfirming={deleteConfirm === 'digest'}
							disabled={deleting}
							onconfirm={ondeletebyhash}
							oncancel={() => onconfirmchange(null)}
							onrequestconfirm={() => onconfirmchange('digest')}
						/>
					</td>
				</tr>
			</tbody>
		</table>
	</Card>

	{#if manifest.config}
		<Card title="Config">
			<table>
				<tbody>
					<tr>
						<td class="label">Digest</td>
						<td>
							<DigestLink
								digest={manifest.config.digest}
								annotations={manifest.config.annotations}
								expanded={expandedAnnotations.has('config')}
								ontoggle={() => toggleAnnotations('config')}
							/>
						</td>
					</tr>
					<tr>
						<td class="label">Media type</td>
						<td>{manifest.config.mediaType}</td>
					</tr>
					<tr>
						<td class="label">Size</td>
						<td class="nowrap">{formatSize(manifest.config.size)}</td>
					</tr>
					{#if manifest.config.annotations && expandedAnnotations.has('config')}
						<AnnotationList annotations={manifest.config.annotations} />
					{/if}
				</tbody>
			</table>
		</Card>
	{/if}

	{#if manifest.layers && manifest.layers.length > 0}
		{#snippet viewToggle()}
			<div class="view-toggle">
				<button class:active={layersViewMode === 'auto'} onclick={() => (layersViewMode = 'auto')}>auto</button>
				<button class:active={layersViewMode === 'files'} onclick={() => (layersViewMode = 'files')}>files</button>
				<button class:active={layersViewMode === 'layers'} onclick={() => (layersViewMode = 'layers')}>layers</button>
			</div>
		{/snippet}
		{#if showFilesView}
			<Card title="Files" count={manifest.layers.length} headerActions={viewToggle}>
				<table>
					<thead>
						<tr>
							<th>Name</th>
							<th>Type</th>
							<th class="col-narrow">Size</th>
							<th class="col-narrow"></th>
						</tr>
					</thead>
					<tbody>
						{#each manifest.layers as layer}
							<tr>
								<td class="filename">{getFileName(layer) ?? layer.digest}</td>
								<td>{layer.mediaType}</td>
								<td class="nowrap">{formatSize(layer.size)}</td>
								<td>
									<a class="download-link" href={getbloburl(layer.digest)} download={getFileName(layer) ?? layer.digest}
										>Download</a
									>
								</td>
							</tr>
						{/each}
					</tbody>
					<tfoot>
						<tr>
							<td colspan="2" class="total-label">Total</td>
							<td class="nowrap">{formatSize(manifest.layers.reduce((sum, l) => sum + l.size, 0))}</td>
							<td></td>
						</tr>
					</tfoot>
				</table>
			</Card>
		{:else}
			<Card title="Layers" count={manifest.layers.length} headerActions={viewToggle}>
				<table>
					<thead>
						<tr>
							<th>Digest</th>
							<th>Media type</th>
							<th class="col-narrow">Size</th>
						</tr>
					</thead>
					<tbody>
						{#each manifest.layers as layer}
							<tr>
								<td>
									<DigestLink
										digest={layer.digest}
										annotations={layer.annotations}
										expanded={expandedAnnotations.has(`layer:${layer.digest}`)}
										ontoggle={() => toggleAnnotations(`layer:${layer.digest}`)}
									/>
								</td>
								<td>{layer.mediaType}</td>
								<td class="nowrap">{formatSize(layer.size)}</td>
							</tr>
							{#if layer.annotations && expandedAnnotations.has(`layer:${layer.digest}`)}
								<tr class="annotations-row">
									<td colspan="3">
										<AnnotationList annotations={layer.annotations} format="inline" />
									</td>
								</tr>
							{/if}
						{/each}
					</tbody>
					<tfoot>
						<tr>
							<td colspan="2" class="total-label">Total</td>
							<td class="nowrap">{formatSize(manifest.layers.reduce((sum, l) => sum + l.size, 0))}</td>
						</tr>
					</tfoot>
				</table>
			</Card>
		{/if}
	{/if}

	{#if manifest.manifests && manifest.manifests.length > 0}
		{@const platformManifests = manifest.manifests.filter((m) => !m.annotations?.['vnd.docker.reference.digest'])}
		{#if platformManifests.length > 0}
			<Card title="Manifests" count={platformManifests.length}>
				<table>
					<thead>
						<tr>
							<th>Digest</th>
							<th>Platform</th>
							<th>Media type</th>
							<th class="col-narrow">Size</th>
						</tr>
					</thead>
					<tbody>
						{#each platformManifests as m}
							{@const refs = childReferrers.get(m.digest) ?? []}
							<tr class="child-row clickable" onclick={(e) => handleRowClick(e, m.digest)}>
								<td class="has-children" class:expanded={refs.length > 0}>
									<span class="tree-toggle leaf"></span>
									<DigestLink
										digest={m.digest}
										href={manifestUrl(path, m.digest)}
										annotations={m.annotations}
										expanded={expandedAnnotations.has(`manifest:${m.digest}`)}
										ontoggle={() => toggleAnnotations(`manifest:${m.digest}`)}
									/>
								</td>
								<td>
									<PlatformBadge platform={m.platform} />
								</td>
								<td>{m.mediaType}</td>
								<td class="nowrap">{formatSize(m.size)}</td>
							</tr>
							{#if m.annotations && expandedAnnotations.has(`manifest:${m.digest}`)}
								<tr class="annotations-row">
									<td colspan="4">
										<AnnotationList annotations={m.annotations} format="inline" />
									</td>
								</tr>
							{/if}
							{@const moreRefs = childReferrersNext.has(m.digest)}
							{#each refs as ref, ridx}
								{@const isLastRef = ridx === refs.length - 1 && !moreRefs}
								<tr class="child-row clickable" onclick={(e) => handleRowClick(e, ref.digest)}>
									<td class="tree-branch" class:has-next={!isLastRef}>
										<span class="tree-toggle leaf"></span>
										<DigestLink digest={ref.digest} href={manifestUrl(path, ref.digest)} />
									</td>
									<td class="nowrap">
										<AttestationBadge type={getAttestationType(ref)} />
										<ScanSummary annotations={ref.annotations} compact />
									</td>
									<td></td>
									<td></td>
								</tr>
							{/each}
							{#if moreRefs}
								<tr class="child-row">
									<td class="tree-branch">
										<span class="tree-toggle leaf"></span>
										<button
											class="secondary"
											onclick={() => onloadmorereferrers(m.digest)}
											disabled={loadingReferrers === m.digest}
										>
											Load more referrers
										</button>
									</td>
									<td></td>
									<td></td>
									<td></td>
								</tr>
							{/if}
						{/each}
					</tbody>
				</table>
			</Card>
		{/if}
	{/if}

	{#if digest && (childReferrers.get(digest) ?? []).length > 0}
		{@const ownReferrers = childReferrers.get(digest) ?? []}
		<Card title="Referrers" count={ownReferrers.length}>
			<table>
				<thead>
					<tr>
						<th>Digest</th>
						<th>Type</th>
					</tr>
				</thead>
				<tbody>
					{#each ownReferrers as ref}
						<tr class="clickable" onclick={(e) => handleRowClick(e, ref.digest)}>
							<td>
								<DigestLink digest={ref.digest} href={manifestUrl(path, ref.digest)} />
							</td>
							<td class="nowrap">
								<AttestationBadge type={getAttestationType(ref)} />
								<ScanSummary annotations={ref.annotations} compact />
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Card>
	{/if}

	{#if referencedBy.length > 0}
		<Card title="Referenced by" count={referencedBy.length}>
			<table>
				<thead>
					<tr>
						<th>Digest</th>
						<th>Tags</th>
						<th>Platform</th>
					</tr>
				</thead>
				<tbody>
					{#each referencedBy as parent}
						<tr class="clickable" onclick={(e) => handleRowClick(e, parent.digest)}>
							<td>
								<DigestLink digest={parent.digest} href={manifestUrl(path, parent.digest)} />
							</td>
							<td>
								<TagList tags={parent.tags} />
							</td>
							<td>
								<PlatformBadge platform={parent.platform} />
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Card>
	{/if}
{:else if current === 'history'}
	<!-- Keyed on the reference so navigating to another manifest drops the
	     history fetched for the previous one. -->
	{#key reference}
		<PullHistory namespace={path} target={reference} open />
	{/key}
{:else if current === 'vulnerabilities'}
	<VulnTab namespace={path} {reports} {platform} ownManifest={isReport ? manifest : null} ownDigest={digest} />
{:else if current === 'filesystem'}
	<LayerBrowser namespace={path} layers={fsLayers} />
{/if}
