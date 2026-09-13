<script lang="ts">
	import type { Descriptor, LayerListing } from '$lib/api';
	import { fetchLayerEntries, layerFileUrl } from '$lib/api';
	import {
		formatSize,
		fsMatches,
		fsNodeAt,
		fsParent,
		fsResolve,
		fsVisible,
		mergeLayers,
		sortedChildren,
		type FsMatcher,
		type FsNode,
		type FsTree
	} from '$lib/utils';
	import Card from './Card.svelte';
	import FsIcon from './FsIcon.svelte';
	import FsRow from './FsRow.svelte';
	import LoadingState from './LoadingState.svelte';
	import ErrorState from './ErrorState.svelte';

	interface Props {
		namespace: string;
		/** The image's walkable layers, in order. */
		layers: Descriptor[];
	}

	let { namespace, layers }: Props = $props();

	/** Files up to this size open inline; larger ones only download. */
	const INLINE_LIMIT = 512 * 1024;

	let listings = $state<(LayerListing | null)[]>([]);
	let pending = $state(0);
	let error = $state<string | null>(null);
	let timer: ReturnType<typeof setTimeout> | undefined;

	let view = $state<'list' | 'icons'>('list');
	let layerFilter = $state<Set<number>>(new Set());
	let layerMenu = $state<HTMLDetailsElement>();
	let search = $state('');
	let expanded = $state<Set<string>>(new Set());
	/** The folder the icon view is in. */
	let cwd = $state('');
	let selected = $state<FsNode | null>(null);
	let content = $state<{ text: string | null; binary: boolean; tooBig: boolean } | null>(null);
	let contentError = $state<string | null>(null);

	// Every layer is asked at once; the registry indexes the ones it never
	// saw and answers 202 until it has, so the load asks again shortly.
	async function load(pass: number) {
		const results = await Promise.all(
			layers.map((layer) => fetchLayerEntries(namespace, layer.digest))
		);
		listings = results.map((result) => result.listing);
		pending = results.filter((result) => result.pending).length;
		error = results.find((result) => result.error)?.error ?? null;
		if (pending > 0 && !error) timer = setTimeout(() => load(pass + 1), Math.min(2000 * (pass + 1), 10000));
	}

	$effect(() => {
		void namespace;
		void layers;
		listings = [];
		pending = 0;
		error = null;
		selected = null;
		content = null;
		expanded = new Set();
		cwd = '';
		layerFilter = new Set();
		search = '';
		clearTimeout(timer);
		load(0);
		return () => clearTimeout(timer);
	});

	const tree = $derived.by((): FsTree | null =>
		listings.length === layers.length && listings.every(Boolean)
			? mergeLayers(listings as LayerListing[])
			: null
	);
	const matcher = $derived<FsMatcher>({ layers: layerFilter, text: search.trim().toLowerCase() });
	const filtering = $derived(layerFilter.size > 0 || matcher.text !== '');
	const deleted = $derived(tree ? tree.deletions.filter((d) => layerFilter.has(d.layer)) : []);
	const total = $derived(listings.reduce((sum, listing) => sum + (listing?.entries.length ?? 0), 0));
	const layerlabel = (layer: number) => `L${layer + 1}`;
	const layerSummary = $derived(
		layerFilter.size === 0
			? 'All layers'
			: layerFilter.size > 4
				? `${layerFilter.size} layers`
				: [...layerFilter].sort((a, b) => a - b).map(layerlabel).join(', ')
	);
	/** The icon view's folder and its entries, the narrowed ones only while filtering. */
	const folder = $derived(tree ? fsNodeAt(tree.root, cwd) : null);
	const tiles = $derived(
		folder ? sortedChildren(folder).filter((n) => !filtering || fsVisible(matcher, n)) : []
	);
	const crumbs = $derived(cwd.split('/').filter(Boolean));
	/** Deletions the icon view shows: the focused layers' whiteouts directly in the folder. */
	const deletedHere = $derived(
		deleted.filter((d) => d.path.slice(0, d.path.lastIndexOf('/') + 1) === (cwd ? cwd + '/' : ''))
	);
	/** The selected node when it has bytes to show. */
	const file = $derived(
		selected && (selected.kind === 'file' || selected.kind === 'hardlink') ? selected : null
	);
	/** A selected symlink is one that led nowhere. */
	const dangling = $derived(selected?.kind === 'symlink' ? selected : null);
	const downloadUrl = $derived(
		file ? layerFileUrl(namespace, layers[file.layer].digest, file.path, true) : ''
	);

	function toggle(path: string) {
		const next = new Set(expanded);
		if (!next.delete(path)) next.add(path);
		expanded = next;
	}

	function toggleLayer(layer: number) {
		const next = new Set(layerFilter);
		if (!next.delete(layer)) next.add(layer);
		layerFilter = next;
	}

	function enter(node: FsNode) {
		if (node.kind === 'dir') cwd = node.path;
		else open(node);
	}

	/** Opens a folder's ancestors in the tree and moves the icon view into it. */
	function reveal(node: FsNode) {
		const next = new Set(expanded);
		for (let dir = node.kind === 'dir' ? node.path : fsParent(node.path); dir; dir = fsParent(dir)) {
			next.add(dir);
		}
		expanded = next;
		cwd = node.kind === 'dir' ? node.path : fsParent(node.path);
	}

	/** A symlink is followed to where it leads, and shown there. */
	async function open(link: FsNode) {
		const node = tree ? fsResolve(tree.root, link) : link;
		selected = node ?? link;
		content = null;
		contentError = null;
		if (!node) return;
		if (node !== link) reveal(node);
		if (node.kind !== 'file' && node.kind !== 'hardlink') return;
		if ((node.entry?.size ?? 0) > INLINE_LIMIT) {
			content = { text: null, binary: false, tooBig: true };
			return;
		}
		try {
			const response = await fetch(layerFileUrl(namespace, layers[node.layer].digest, node.path));
			if (!response.ok) {
				contentError = `HTTP ${response.status}`;
				return;
			}
			const bytes = new Uint8Array(await response.arrayBuffer());
			const binary = bytes.subarray(0, 8192).some((byte) => byte === 0);
			content = { text: binary ? null : new TextDecoder().decode(bytes), binary, tooBig: false };
		} catch (e) {
			contentError = e instanceof Error ? e.message : 'Request failed';
		}
	}
</script>

<svelte:window
	onclick={(e) => {
		if (layerMenu?.open && !layerMenu.contains(e.target as Node)) layerMenu.open = false;
	}}
/>

<Card title="Filesystem" count={tree ? total : undefined}>
	{#if error}
		<ErrorState message="Could not load the layer listings ({error})." />
	{:else if !tree}
		<LoadingState
			message={pending > 0
				? `Indexing layer ${layers.length - pending + 1} of ${layers.length}, this takes a moment the first time`
				: 'Loading the layer listings'}
		/>
	{:else}
		<div class="fs-toolbar">
			<div class="view-toggle" role="group" aria-label="View">
				<button type="button" class:active={view === 'list'} onclick={() => (view = 'list')}>List</button>
				<button type="button" class:active={view === 'icons'} onclick={() => (view = 'icons')}>Icons</button>
			</div>
			<details class="menu" bind:this={layerMenu}>
				<summary class="btn secondary">{layerSummary} <span aria-hidden="true">▾</span></summary>
				<div class="menu-panel">
					{#each layers as layer, i}
						<label>
							<input type="checkbox" checked={layerFilter.has(i)} onchange={() => toggleLayer(i)} />
							<span class="badge">{layerlabel(i)}</span>
							{formatSize(layer.size)}
							<span class="mono muted">{layer.digest.slice(7, 19)}</span>
						</label>
					{/each}
					{#if layerFilter.size > 0}
						<button type="button" class="menu-clear" onclick={() => (layerFilter = new Set())}>All layers</button>
					{/if}
				</div>
			</details>
			<input type="search" placeholder="Filter paths" bind:value={search} aria-label="Filter paths" />
		</div>
		{#if view === 'list'}
			<table>
				<thead>
					<tr>
						<th>Name</th>
						<th class="col-narrow">Size</th>
						<th class="col-medium">Mode</th>
						<th class="col-medium">Modified</th>
						<th class="col-narrow">Layer</th>
					</tr>
				</thead>
				<tbody>
					{#each sortedChildren(tree.root) as node, i (node.path)}
						<FsRow
							{node}
							depth={0}
							guides={[]}
							hasNext={i < tree.root.children.size - 1}
							{expanded}
							{matcher}
							selected={selected?.path ?? null}
							ontoggle={toggle}
							onopen={open}
							{layerlabel}
						/>
					{/each}
					{#each deleted as gone}
						<tr>
							<td class="fs-name fs-deleted" colspan="4">{gone.path}</td>
							<td><span class="badge">{layerlabel(gone.layer)}</span></td>
						</tr>
					{/each}
				</tbody>
			</table>
		{:else}
			<nav class="fs-crumbs" aria-label="Folder">
				<button type="button" onclick={() => (cwd = '')} disabled={cwd === ''}>/</button>
				{#each crumbs as crumb, i}
					{#if i > 0}<span aria-hidden="true">/</span>{/if}
					<button
						type="button"
						onclick={() => (cwd = crumbs.slice(0, i + 1).join('/'))}
						disabled={i === crumbs.length - 1}>{crumb}</button
					>
				{/each}
			</nav>
			<div class="fs-grid">
				{#each tiles as node (node.path)}
					<button
						type="button"
						class="fs-tile"
						class:dim={filtering && !fsMatches(matcher, node)}
						class:selected={selected?.path === node.path}
						title={node.entry?.link ? `${node.name} → ${node.entry.link}` : node.name}
						onclick={() => enter(node)}
					>
						<FsIcon kind={node.kind} />
						<span class="fs-tile-name">{node.name}</span>
						<span class="fs-tile-meta">
							{node.kind === 'file' ? formatSize(node.entry?.size ?? 0) : layerlabel(node.layer)}
						</span>
					</button>
				{:else}
					<p class="muted">Nothing here{filtering ? ' matches' : ''}.</p>
				{/each}
				{#each deletedHere as gone}
					<div class="fs-tile fs-deleted">
						<FsIcon kind="file" />
						<span class="fs-tile-name">{gone.path.split('/').pop()}</span>
						<span class="fs-tile-meta">{layerlabel(gone.layer)}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</Card>

{#if dangling}
	<Card title={dangling.path}>
		<div class="fs-content">
			<p class="muted">Points to {dangling.entry?.link}, which is not in the image.</p>
		</div>
	</Card>
{:else if file}
	{#snippet actions()}
		<a class="btn secondary" href={downloadUrl} download={file.name}>Download</a>
	{/snippet}
	<Card title={file.path} headerActions={actions}>
		<div class="fs-content">
			{#if contentError}
				<ErrorState message="Could not read the file ({contentError})." />
			{:else if !content}
				<LoadingState message="Reading the file" />
			{:else if content.tooBig}
				<p class="muted">{formatSize(file.entry?.size ?? 0)}, too large to show here.</p>
			{:else if content.binary}
				<p class="muted">Binary file, {formatSize(file.entry?.size ?? 0)}.</p>
			{:else}
				<pre>{content.text}</pre>
			{/if}
		</div>
	</Card>
{/if}
