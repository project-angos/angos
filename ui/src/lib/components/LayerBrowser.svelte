<script lang="ts">
	import type { Descriptor, LayerEntry, LayerListing } from '$lib/api';
	import { fetchLayerEntries } from '$lib/api';
	import { goto } from '$app/navigation';
	import { tick, untrack } from 'svelte';
	import {
		SECRET_LABELS,
		formatMode,
		formatSize,
		fsCopies,
		fsDuplicates,
		fsMatches,
		fsNodeAt,
		fsParent,
		fsResolve,
		fsRisks,
		fsRows,
		fsSame,
		fsVisible,
		mergeLayers,
		secretKinds,
		sortedChildren,
		type FsMatcher,
		type FsNode,
		type FsTree
	} from '$lib/utils';
	import Card from './Card.svelte';
	import FilePreview from './FilePreview.svelte';
	import FsIcon from './FsIcon.svelte';
	import FsRow from './FsRow.svelte';
	import LoadingState from './LoadingState.svelte';
	import ErrorState from './ErrorState.svelte';

	interface Props {
		namespace: string;
		/** The image's walkable layers, in order. */
		layers: Descriptor[];
		/** The URL anchor past `filesystem/`: the open file's path, `@L<n>` naming another layer's version. */
		anchor: string;
	}

	let { namespace, layers, anchor }: Props = $props();

	/** Rows a wasted-space list shows, the largest. */
	const WASTE_ROWS = 100;

	// Replaced whole, never edited, so neither needs a deep proxy.
	let listings = $state.raw<(LayerListing | null)[]>([]);
	let cursor = $state.raw<FsNode | null>(null);
	let pending = $state(0);
	/** Layers an older version indexed, which the registry is indexing again. */
	let refreshing = $state(0);
	let error = $state<string | null>(null);
	let timer: ReturnType<typeof setTimeout> | undefined;

	let view = $state<'tree' | 'icons' | 'secrets' | 'permissions' | 'waste'>('tree');
	let wasteView = $state<'overwritten' | 'duplicates'>('overwritten');
	let layerFilter = $state<Set<number>>(new Set());
	let layerMenu = $state<HTMLDetailsElement>();
	let pane = $state<HTMLElement>();
	let grid = $state<HTMLElement>();
	let search = $state('');
	let expanded = $state<Set<string>>(new Set());
	/** The folder the icon view is in. */
	let cwd = $state('');

	// Every layer is asked at once; the registry indexes the ones it never
	// saw and answers 202 until it has, so the load asks again shortly. It
	// serves an older version's listing while it indexes the layer again, so
	// those are asked for again too, until the new one replaces it.
	async function load(pass: number) {
		const results = await Promise.all(
			layers.map((layer, i) => {
				const known = listings[i];
				return known && !known.refreshing
					? { listing: known, pending: false, error: null }
					: fetchLayerEntries(namespace, layer.digest);
			})
		);
		// A listing still refreshing is the one shown: keeping it spares rebuilding the tree.
		const next = results.map((result, i) => (result.listing?.refreshing && listings[i]) || result.listing);
		if (next.some((listing, i) => listing !== listings[i])) listings = next;
		pending = results.filter((result) => result.pending).length;
		refreshing = next.filter((listing) => listing?.refreshing).length;
		error = results.find((result) => result.error)?.error ?? null;
		if ((pending > 0 || refreshing > 0) && !error) {
			timer = setTimeout(() => load(pass + 1), Math.min(2000 * (pass + 1), 10000));
		}
	}

	$effect(() => {
		void namespace;
		void layers;
		listings = [];
		pending = 0;
		refreshing = 0;
		error = null;
		cursor = null;
		expanded = new Set();
		cwd = '';
		layerFilter = new Set();
		search = '';
		clearTimeout(timer);
		// The load reads the listings it may keep; they are not this effect's to track.
		untrack(() => load(0));
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
	const wastedBytes = $derived(tree ? tree.wasted.reduce((sum, w) => sum + w.size, 0) : 0);
	const duplicates = $derived(tree ? fsDuplicates(tree.root) : []);
	const duplicateBytes = $derived(duplicates.reduce((sum, d) => sum + fsCopies(d), 0));
	const imageBytes = $derived(listings.reduce((sum, listing) => sum + (listing?.uncompressed_size ?? 0), 0));
	/** Share of the image's uncompressed bytes, as a CSS width. */
	const share = (bytes: number) => `${((100 * bytes) / Math.max(imageBytes, 1)).toFixed(1)}%`;
	const wasteList = $derived(
		wasteView === 'overwritten' && tree?.wasted.length ? 'overwritten' : duplicates.length ? 'duplicates' : 'overwritten'
	);
	/** Every layer's secrets: one a later layer removed still ships in its own. */
	const secrets = $derived.by(() => {
		if (!tree) return [];
		const root = tree.root;
		return (listings as LayerListing[]).flatMap((listing, layer) =>
			listing.entries
				.filter((entry) => entry.kind === 'file' && entry.content?.secrets?.length)
				.map((entry) => {
					const shown = fsNodeAt(root, entry.path);
					return shown.entry === entry ? shown : versionNode(entry, layer);
				})
		);
	});

	const risky = $derived(tree ? fsRisks(tree.root) : []);

	/** The nodes the current view lists, in order, which the keyboard walks. */
	const items = $derived.by((): FsNode[] => {
		if (!tree) return [];
		if (view === 'tree') return fsRows(tree.root, expanded, matcher);
		if (view === 'icons') return tiles;
		if (view === 'secrets') return secrets;
		if (view === 'permissions') return risky.map((risk) => risk.node);
		return wasteList === 'overwritten'
			? tree.wasted.slice(0, WASTE_ROWS).map((w) => w.node)
			: duplicates.slice(0, WASTE_ROWS).flatMap((d) => d.nodes);
	});
	/** The one item the list's focus rests on: the cursor, or the first until there is one. */
	const rover = $derived(items.find((node) => fsSame(node, cursor)) ?? items[0] ?? null);

	const decode = (part: string) => {
		try {
			return decodeURIComponent(part);
		} catch {
			return part;
		}
	};
	/** The file the URL names; a path's own `@` stays escaped, so only the version suffix is bare. */
	const wanted = $derived.by(() => {
		const version = /^(.+)@L(\d+)$/.exec(anchor);
		const layer = version ? Number(version[2]) - 1 : -1;
		const versioned = version !== null && layer >= 0 && layer < layers.length;
		const path = (versioned ? version[1] : anchor).split('/').map(decode).join('/');
		return path ? { path, layer: versioned ? layer : null } : null;
	});
	const selected = $derived.by((): FsNode | null => {
		if (!tree || !wanted) return null;
		const live = fsNodeAt(tree.root, wanted.path);
		const found = live.path === wanted.path ? live : null;
		if (wanted.layer === null || found?.layer === wanted.layer) return found;
		const version = versionsOf(wanted.path).find((v) => v.layer === wanted.layer);
		return version ? versionNode(version.entry, version.layer) : null;
	});
	/** What the preview shows: a symlink followed, null where it leads nowhere. */
	const target = $derived(selected && tree ? fsResolve(tree.root, selected) : null);
	const file = $derived(target && (target.kind === 'file' || target.kind === 'hardlink') ? target : null);
	const dangling = $derived(selected?.kind === 'symlink' && !target ? selected : null);
	const versions = $derived(file ? versionsOf(file.path).map((v) => v.layer) : []);
	/** The layer of the version the image shows, null when the path is gone from it. */
	const live = $derived.by(() => {
		if (!file || !tree) return null;
		const shown = fsNodeAt(tree.root, file.path);
		return shown.path === file.path && (shown.kind === 'file' || shown.kind === 'hardlink') ? shown.layer : null;
	});
	/** The entry holding the file's bytes: a hard link's is its target in the same layer, as the registry reads it. */
	const bytes = $derived.by((): LayerEntry | null => {
		if (file?.kind !== 'hardlink') return file?.entry ?? null;
		const link = file.entry?.link?.replace(/^(\.\/)+/, '').replace(/^\/+|\/+$/g, '');
		return listings[file.layer]?.entries.findLast((e) => e.path === link && e.kind === 'file') ?? null;
	});
	const inTree = (node: FsNode | null) => !!node && !!tree && fsNodeAt(tree.root, node.path) === node;

	// The tree opens down to what the URL names, and the keyboard carries on from
	// there where the view lists it: another layer's version stays off the tree.
	$effect(() => {
		const node = target ?? selected;
		if (!node) return;
		untrack(() => {
			if (inTree(node)) reveal(node);
			if (items.some((item) => fsSame(item, node))) cursor = node;
		});
	});

	// Focus follows the cursor unless it is elsewhere on the page, falling back on the
	// list itself when the view has no item, or when the focused one went away.
	$effect(() => {
		void rover;
		tick().then(() => {
			const item = pane?.querySelector<HTMLElement>('.cursor');
			const active = document.activeElement;
			if (pane?.contains(active) || active === document.body) (item ?? pane)?.focus();
			else item?.scrollIntoView({ block: 'nearest' });
		});
	});

	/** Each layer's last byte-holding entry at a path. */
	function versionsOf(path: string): { layer: number; entry: LayerEntry }[] {
		return (listings as LayerListing[]).flatMap((listing, layer) => {
			const entry = listing.entries.findLast(
				(e) => e.path === path && (e.kind === 'file' || e.kind === 'hardlink')
			);
			return entry ? [{ layer, entry }] : [];
		});
	}

	function versionNode(entry: LayerEntry, layer: number): FsNode {
		const name = entry.path.split('/').pop() ?? entry.path;
		return { name, path: entry.path, kind: entry.kind, entry, layer, children: new Map() };
	}

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

	/** Opens a folder's ancestors in the tree and moves the icon view into it. */
	function reveal(node: FsNode) {
		const next = new Set(expanded);
		for (let dir = node.kind === 'dir' ? node.path : fsParent(node.path); dir; dir = fsParent(dir)) {
			next.add(dir);
		}
		expanded = next;
		cwd = node.kind === 'dir' ? node.path : fsParent(node.path);
	}

	/** Puts a file in the URL, `layer` naming a version the image no longer shows. */
	function show(path: string, layer: number | null) {
		const encoded = path.split('/').map(encodeURIComponent).join('/');
		goto(`#filesystem/${encoded}${layer === null ? '' : `@L${layer + 1}`}`, { noScroll: true, keepFocus: true });
	}

	function open(node: FsNode) {
		cursor = node;
		show(node.path, inTree(node) ? null : node.layer);
	}

	function close() {
		goto('#filesystem', { noScroll: true, keepFocus: true });
	}

	/** What a click or Enter does: a folder opens in place, or in the tree from a list, anything else in the preview. */
	function activate(node: FsNode) {
		cursor = node;
		if (node.kind !== 'dir') open(node);
		else if (view === 'icons') cwd = node.path;
		else if (view === 'tree') toggle(node.path);
		else {
			reveal(node);
			view = 'tree';
		}
	}

	function navigate(e: KeyboardEvent) {
		const at = items.findIndex((node) => fsSame(node, rover));
		const current = at >= 0 ? items[at] : null;
		const move = (to: number) => {
			cursor = items[Math.max(0, Math.min(items.length - 1, at < 0 ? 0 : to))] ?? null;
		};
		// The icon view's arrows go by row up and down, by tile across.
		const columns = view === 'icons' && grid ? getComputedStyle(grid).gridTemplateColumns.split(' ').length : 1;
		const folder = view === 'tree' && current?.kind === 'dir' ? current : null;
		switch (e.key) {
			case 'ArrowDown':
				move(at + columns);
				break;
			case 'ArrowUp':
				move(at - columns);
				break;
			case 'Home':
				move(0);
				break;
			case 'End':
				move(items.length - 1);
				break;
			case 'ArrowRight':
				if (view === 'icons') move(at + 1);
				else if (folder && !expanded.has(folder.path)) toggle(folder.path);
				else if (folder) move(at + 1);
				else return;
				break;
			case 'ArrowLeft':
				if (view === 'icons') move(at - 1);
				else if (folder && expanded.has(folder.path)) toggle(folder.path);
				else if (view === 'tree' && current) {
					const parent = items.findIndex((node) => node.path === fsParent(current.path));
					if (parent < 0) return;
					move(parent);
				} else return;
				break;
			case 'Enter':
				// A focused button opens itself.
				if (!current || e.target instanceof HTMLButtonElement) return;
				activate(current);
				break;
			case 'Backspace':
				if (view !== 'icons' || !cwd) return;
				cwd = fsParent(cwd);
				break;
			default:
				return;
		}
		e.preventDefault();
	}
</script>

<svelte:window
	onclick={(e) => {
		if (layerMenu?.open && !layerMenu.contains(e.target as Node)) layerMenu.open = false;
	}}
	onkeydown={(e) => {
		if (e.key === 'Escape' && (file || dangling) && !(e.target instanceof HTMLInputElement)) close();
		else if (pane?.contains(e.target as Node)) navigate(e);
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
		{#if refreshing > 0}
			<p class="notice fs-notice" role="status">
				An older version indexed {refreshing === 1 ? 'a layer' : `${refreshing} layers`} of this image, so some
				file types and secrets may be missing. The registry is indexing {refreshing === 1 ? 'it' : 'them'} again,
				and this view updates when it is done.
			</p>
		{/if}
		<div class="fs-toolbar">
			<div class="view-toggle" role="group" aria-label="View">
				<button type="button" class:active={view === 'tree'} onclick={() => (view = 'tree')}>Tree</button>
				<button type="button" class:active={view === 'icons'} onclick={() => (view = 'icons')}>Icons</button>
				{#if secrets.length > 0}
					<button type="button" class:active={view === 'secrets'} onclick={() => (view = 'secrets')}>
						Secrets <span class="severity severity-critical">{secrets.length}</span>
					</button>
				{/if}
				{#if risky.length > 0}
					<button type="button" class:active={view === 'permissions'} onclick={() => (view = 'permissions')}>
						Permissions <span class="fs-toggle-meta">{risky.length}</span>
					</button>
				{/if}
				{#if tree.wasted.length > 0 || duplicates.length > 0}
					<button type="button" class:active={view === 'waste'} onclick={() => (view = 'waste')}>
						Waste <span class="fs-toggle-meta">{formatSize(wastedBytes + duplicateBytes)}</span>
					</button>
				{/if}
			</div>
			{#if view === 'tree' || view === 'icons'}
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
			{/if}
		</div>
		<div class="fs-split" class:open={file || dangling}>
			<div class="fs-pane" tabindex="-1" bind:this={pane}>
				{#if view === 'tree'}
					<table class="fs-tree">
						<thead>
							<tr>
								<th>Name</th>
								<th class="col-narrow fs-extra">Size</th>
								<th class="col-medium fs-extra">Mode</th>
								<th class="col-medium fs-extra">Modified</th>
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
									selected={inTree(target) ? (target?.path ?? null) : null}
									cursor={rover?.path ?? null}
									ontoggle={(path) => {
										cursor = fsNodeAt(tree.root, path);
										toggle(path);
									}}
									onopen={activate}
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
				{:else if view === 'icons'}
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
					<div class="fs-grid" bind:this={grid}>
						{#each tiles as node (node.path)}
							<button
								type="button"
								tabindex={fsSame(node, rover) ? 0 : -1}
								class="fs-tile"
								class:dim={filtering && !fsMatches(matcher, node)}
								class:selected={fsSame(node, target)}
								class:cursor={fsSame(node, rover)}
								title={node.entry?.link ? `${node.name} → ${node.entry.link}` : node.name}
								onclick={() => activate(node)}
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
				{:else if view === 'secrets'}
					<table>
						<thead>
							<tr>
								<th>Path</th>
								<th class="col-wide">Looks like</th>
								<th class="col-narrow">Layer</th>
							</tr>
						</thead>
						<tbody>
							{#each secrets as node (`${node.layer}:${node.path}`)}
								<tr class:selected={fsSame(node, file)}>
									<td>
										<button
											type="button"
											tabindex={fsSame(node, rover) ? 0 : -1}
											class="link fs-path"
											class:cursor={fsSame(node, rover)}
											onclick={() => activate(node)}>{node.path}</button
										>
										{#if !inTree(node)}<span class="badge" title="Gone from the image, still in its layer">removed</span>{/if}
									</td>
									<td>
										{#each secretKinds(node.entry?.content?.secrets ?? []) as [kind, lines]}
											<div>
												<span class="severity severity-critical secret">{SECRET_LABELS[kind]}</span>
												<span class="muted nowrap">line{lines.length > 1 ? 's' : ''} {lines.join(', ')}</span>
											</div>
										{/each}
									</td>
									<td><span class="badge">{layerlabel(node.layer)}</span></td>
								</tr>
							{/each}
						</tbody>
					</table>
				{:else if view === 'permissions'}
					<table>
						<thead>
							<tr>
								<th>Path</th>
								<th class="col-medium">Risk</th>
								<th class="col-medium fs-extra">Mode</th>
								<th class="col-narrow fs-extra">Owner</th>
								<th class="col-narrow">Layer</th>
							</tr>
						</thead>
						<tbody>
							{#each risky as { node, risks } (node.path)}
								<tr class:selected={fsSame(node, file)}>
									<td>
										<button
											type="button"
											tabindex={fsSame(node, rover) ? 0 : -1}
											class="link fs-path"
											class:cursor={fsSame(node, rover)}
											onclick={() => activate(node)}>{node.path}{node.kind === 'dir' ? '/' : ''}</button
										>
									</td>
									<td>
										{#each risks as risk}
											<span class="badge">
												{risk === 'capabilities' ? node.entry?.capabilities?.join(', ') : risk}
											</span>
										{/each}
									</td>
									<td class="mono fs-extra">{formatMode(node.entry?.mode ?? 0)}</td>
									<td class="fs-extra">{node.entry?.uid}:{node.entry?.gid}</td>
									<td><span class="badge">{layerlabel(node.layer)}</span></td>
								</tr>
							{/each}
						</tbody>
					</table>
				{:else}
					<div class="waste-summary">
						<div class="waste-meter" aria-hidden="true">
							<span class="waste-overwritten" style:width={share(wastedBytes)}></span>
							<span class="waste-duplicates" style:width={share(duplicateBytes)}></span>
						</div>
						<div class="waste-head">
							<p>
								<strong>{formatSize(wastedBytes + duplicateBytes)}</strong>
								<span class="muted">of {formatSize(imageBytes)} uncompressed, {share(wastedBytes + duplicateBytes)}</span>
							</p>
							<div class="view-toggle" role="group" aria-label="Wasted space">
								<button
									type="button"
									class:active={wasteList === 'overwritten'}
									disabled={tree.wasted.length === 0}
									onclick={() => (wasteView = 'overwritten')}>Overwritten {tree.wasted.length}</button
								>
								<button
									type="button"
									class:active={wasteList === 'duplicates'}
									disabled={duplicates.length === 0}
									onclick={() => (wasteView = 'duplicates')}>Duplicates {duplicates.length}</button
								>
							</div>
						</div>
						<p class="waste-legend muted">
							<span><i class="waste-swatch waste-overwritten"></i>{formatSize(wastedBytes)} overwritten or removed by a later layer</span>
							<span><i class="waste-swatch waste-duplicates"></i>{formatSize(duplicateBytes)} in copies of identical files</span>
						</p>
					</div>
					{#if wasteList === 'overwritten'}
						{@const largest = tree.wasted[0]?.size ?? 1}
						<table>
							<thead>
								<tr>
									<th>Path</th>
									<th class="col-medium">Change</th>
									<th class="col-wide">Size</th>
									<th class="col-medium fs-extra">Layers</th>
								</tr>
							</thead>
							<tbody>
								{#each tree.wasted.slice(0, WASTE_ROWS) as waste}
									<tr class:selected={fsSame(waste.node, file)}>
										<td>
											<button
												type="button"
												tabindex={fsSame(waste.node, rover) ? 0 : -1}
												class="link fs-path"
												class:cursor={fsSame(waste.node, rover)}
												onclick={() => activate(waste.node)}
											>
												{waste.node.path}
											</button>
										</td>
										<td><span class="badge waste-{waste.change}">{waste.change}</span></td>
										<td class="nowrap">
											<span class="waste-bar"><span style:width="{(100 * waste.size) / largest}%"></span></span>
											{formatSize(waste.size)}
										</td>
										<td class="nowrap fs-extra">
											<span class="badge">{layerlabel(waste.node.layer)}</span>
											<span class="muted">→</span>
											<span class="badge">{layerlabel(waste.by)}</span>
										</td>
									</tr>
								{/each}
								{#if tree.wasted.length > WASTE_ROWS}
									<tr>
										<td class="muted" colspan="4">And {tree.wasted.length - WASTE_ROWS} smaller files</td>
									</tr>
								{/if}
							</tbody>
						</table>
					{:else}
						{@const largest = fsCopies(duplicates[0])}
						<table>
							<thead>
								<tr>
									<th>Copies</th>
									<th class="col-medium fs-extra">Each</th>
									<th class="col-wide">Wasted</th>
								</tr>
							</thead>
							<tbody>
								{#each duplicates.slice(0, WASTE_ROWS) as duplicate}
									<tr>
										<td>
											{#each duplicate.nodes as node}
												<div class="waste-copy">
													<button
														type="button"
														tabindex={fsSame(node, rover) ? 0 : -1}
														class="link fs-path"
														class:cursor={fsSame(node, rover)}
														onclick={() => activate(node)}
													>
														{node.path}
													</button>
													<span class="badge">{layerlabel(node.layer)}</span>
												</div>
											{/each}
										</td>
										<td class="nowrap fs-extra">{formatSize(duplicate.size)}</td>
										<td class="nowrap">
											<span class="waste-bar duplicates"><span style:width="{(100 * fsCopies(duplicate)) / largest}%"></span></span>
											{formatSize(fsCopies(duplicate))}
										</td>
									</tr>
								{/each}
								{#if duplicates.length > WASTE_ROWS}
									<tr>
										<td class="muted" colspan="3">And {duplicates.length - WASTE_ROWS} smaller sets</td>
									</tr>
								{/if}
							</tbody>
						</table>
					{/if}
				{/if}
			</div>
			{#if file || dangling}
				{@const node = (file ?? dangling) as FsNode}
				<FilePreview
					{namespace}
					digests={layers.map((layer) => layer.digest)}
					{node}
					{bytes}
					{versions}
					{live}
					{layerlabel}
					onversion={(layer) => show(node.path, layer === live ? null : layer)}
					onclose={close}
				/>
			{/if}
		</div>
	{/if}
</Card>
