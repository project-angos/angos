<script lang="ts">
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { getRegistryName } from '$lib/config.svelte';
	import { fetchRevisions, fetchUploads, fetchManifest, fetchNamespaces, fetchReferrers, deleteManifest as apiDeleteManifest, cancelUpload as apiCancelUpload, blobUrl, type UploadEntry, type ParentRef, type Manifest, type ManifestEntry, type ReferrerInfo } from '$lib/api';
	import { buildTree, buildTreeRows, cascadeSummary, deleteCascade, descendantNamespaces, isInteractiveTarget, pathUrl, manifestUrl, selectedManifestsConfirmKey, type NamespaceDescendant, type TreeRowNode } from '$lib/utils';
	import LoadingState from '$lib/components/LoadingState.svelte';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import Breadcrumb from '$lib/components/Breadcrumb.svelte';
	import ManifestView from '$lib/components/ManifestView.svelte';
	import RepositoryTree from '$lib/components/RepositoryTree.svelte';
	import Card from '$lib/components/Card.svelte';
	import CopyButton from '$lib/components/CopyButton.svelte';
	import DeleteButton from '$lib/components/DeleteButton.svelte';
	import type { BrowseParams } from './+page';

	let { data }: { data: BrowseParams } = $props();

	const isManifestView = $derived(data.reference !== null);
	// The page's name as it is pulled: a tag after a colon, a digest after an at.
	const fullName = $derived(
		data.reference === null
			? data.path
			: `${data.path}${data.reference.startsWith('sha256:') ? '@' : ':'}${data.reference}`
	);
	// The path below the owning repository, which is what the breadcrumb splits on.
	const relativePath = $derived(
		data.repository !== null && data.path.length > data.repository.length
			? data.path.slice(data.repository.length + 1)
			: ''
	);

	let rows: TreeRowNode[] = $state([]);
	// The listing the rows were built from; a delete reads what it orphans here.
	let manifests: ManifestEntry[] = $state([]);
	// Namespaces nested under this path, named relative to it. Counts are absent
	// above a repository, where only the repository names are known.
	let children: NamespaceDescendant[] = $state([]);
	let pullThroughCache = $state(false);
	let upstreamUrls: string[] = $state([]);
	let immutableTags = $state(false);
	let immutableTagsExclusions: string[] = $state([]);
	let uploads: UploadEntry[] = $state([]);
	let selectedUploads: Set<string> = $state(new Set());
	// Select mode over the manifest table, for deleting several at once.
	let selectingManifests = $state(false);
	let selectedManifests: Set<string> = $state(new Set());
	// Rows unticked out of a cascade: kept, along with what they in turn hold.
	let sparedManifests: Set<string> = $state(new Set());

	let manifest: Manifest | null = $state(null);
	let digest: string | null = $state(null);
	let tags: string[] = $state([]);
	let referencedBy: ParentRef[] = $state([]);
	let childReferrers: Map<string, ReferrerInfo[]> = $state(new Map());
	// Where each manifest's referrer listing continues; a manifest absent from
	// the map has none left to load.
	let childReferrersNext: Map<string, string> = $state(new Map());
	let loadingReferrers: string | null = $state(null);

	let loading = $state(true);
	let error: string | null = $state(null);
	// Failures of an action taken on the current view, shown as a banner so the
	// view itself survives; `error` stays reserved for a load that produced no
	// view at all.
	let actionError: string | null = $state(null);
	let deleteConfirm: string | null = $state(null);
	let deleting = $state(false);
	let expanded: Set<string> = $state(new Set());

	// Monotonic token: each load claims the next value, so a slow response from
	// a superseded load is discarded instead of overwriting the current view.
	let loadToken = 0;

	// What the path holds, for the lede: a prefix has namespaces, a leaf has
	// manifests, and a nested name may have both.
	const summary = $derived.by(() => {
		const parts = [];
		if (children.length > 0) parts.push(`${children.length} namespace${children.length === 1 ? '' : 's'}`);
		if (rows.length > 0) parts.push(`${rows.length} manifest${rows.length === 1 ? '' : 's'}`);
		return parts.join(', ') || 'Nothing here yet';
	});

	function toggleExpand(digest: string, event: MouseEvent) {
		event.stopPropagation();
		const newExpanded = new Set(expanded);
		if (newExpanded.has(digest)) {
			newExpanded.delete(digest);
		} else {
			newExpanded.add(digest);
		}
		expanded = newExpanded;
	}

	$effect(() => {
		actionError = null;
		if (data.reference !== null) {
			loadManifest(data.path, data.reference);
		} else {
			loadBrowse(data.path);
		}
	});

	// `background` refreshes without blanking the view, which is what an action
	// taken on that view wants: swapping it for a spinner reads as the whole
	// page reloading when only one row changed.
	async function loadBrowse(namespace: string, background = false) {
		const token = ++loadToken;
		loading = !background;
		error = null;
		// Clearing keeps a navigation from showing the previous path's content
		// behind the spinner. A background refresh has no spinner, so it holds
		// what is on screen until the new data replaces it.
		if (!background) {
			children = [];
			pullThroughCache = false;
			immutableTags = false;
		}
		// Above every repository there is nothing to list but the repository
		// names, which the route already resolved. A background refresh skips the
		// listing entirely: it is by far the most expensive of the three calls, at
		// one store walk plus three backend listings per namespace, and the action
		// that triggered it touched this namespace, whose descendants it lists are
		// all unaffected.
		const [revisionsResult, uploadsResult, namespacesResult] = await Promise.all([
			fetchRevisions(namespace),
			fetchUploads(namespace),
			background || data.repository === null
				? Promise.resolve(null)
				: fetchNamespaces(data.repository)
		]);
		if (token !== loadToken) return;
		if (revisionsResult.error) {
			error = revisionsResult.error;
		} else if (revisionsResult.data) {
			manifests = revisionsResult.data.manifests ?? [];
			rows = buildTreeRows(buildTree(manifests));
		}
		uploads = uploadsResult.data?.uploads ?? [];
		if (uploadsResult.error) {
			actionError = `Could not list uploads (${uploadsResult.error}).`;
		}
		if (!background) {
			children = descendantNamespaces(
				namespacesResult
					? (namespacesResult.data?.namespaces ?? [])
					: data.repositoryNames.map((name) => ({ name })),
				namespace
			);
		}
		if (namespacesResult?.data) {
			pullThroughCache = namespacesResult.data.pull_through_cache;
			upstreamUrls = namespacesResult.data.upstream_urls;
			immutableTags = namespacesResult.data.immutable_tags;
			immutableTagsExclusions = namespacesResult.data.immutable_tags_exclusions;
		}
		selectedUploads = new Set();
		// A new path is a new view: select mode does not follow across it.
		selectedManifests = new Set();
		sparedManifests = new Set();
		selectingManifests = false;
		loading = false;
	}

	async function loadManifest(namespace: string, reference: string, background = false) {
		const token = ++loadToken;
		loading = !background;
		error = null;
		if (!background) {
			tags = [];
			referencedBy = [];
			childReferrers = new Map();
			childReferrersNext = new Map();
		}

		const result = await fetchManifest(namespace, reference);
		if (token !== loadToken) return;
		if (result.error) {
			error = result.error;
			loading = false;
			return;
		}

		manifest = result.manifest;
		digest = result.digest;

		if (digest) {
			const revisionsResult = await fetchRevisions(namespace);
			if (token !== loadToken) return;
			if (revisionsResult.data) {
				manifests = revisionsResult.data.manifests;
				const entry = revisionsResult.data.manifests.find(m => m.digest === digest);
				if (entry) {
					tags = entry.tags;
					referencedBy = entry.parents ?? [];
				}
				const newChildReferrers = new Map<string, ReferrerInfo[]>();
				const newChildReferrersNext = new Map<string, string>();
				for (const m of revisionsResult.data.manifests) {
					if (m.referrers && m.referrers.length > 0) {
						newChildReferrers.set(m.digest, m.referrers);
					}
					if (m.referrers_next) {
						newChildReferrersNext.set(m.digest, m.referrers_next);
					}
				}
				childReferrers = newChildReferrers;
				childReferrersNext = newChildReferrersNext;
			}
		}
		loading = false;
	}

	// Append the next page of a manifest's referrers, keeping the cursor the
	// server hands back so the control disappears once the listing is exhausted.
	async function loadMoreReferrers(childDigest: string) {
		const last = childReferrersNext.get(childDigest);
		if (!last) return;

		loadingReferrers = childDigest;
		actionError = null;
		const result = await fetchReferrers(data.path, childDigest, last);
		loadingReferrers = null;
		if (result.error || !result.data) {
			actionError = `Loading more referrers failed (${result.error}).`;
			return;
		}

		const merged = new Map(childReferrers);
		merged.set(childDigest, [...(merged.get(childDigest) ?? []), ...result.data.referrers]);
		childReferrers = merged;

		const cursors = new Map(childReferrersNext);
		if (result.data.next) {
			cursors.set(childDigest, result.data.next);
		} else {
			cursors.delete(childDigest);
		}
		childReferrersNext = cursors;
	}

	// Refresh the view a delete was taken from. Reloading by a reference that
	// was just deleted would 404 and strand the user on an error page although
	// the delete succeeded, so leave for the digest when it still resolves and
	// for the namespace otherwise.
	async function reloadAfterDelete(deletedReference: string) {
		if (data.reference === null) {
			await loadBrowse(data.path, true);
		} else if (data.reference !== deletedReference) {
			await loadManifest(data.path, data.reference, true);
		} else if (digest && digest !== deletedReference) {
			await goto(manifestUrl(data.path, digest));
		} else {
			await goto(pathUrl(data.path));
		}
	}

	// What the current selection would take along: shown ticked and locked on
	// the rows themselves, so an index's children read as going with it while a
	// child another index still names visibly stays.
	const impliedDeletes = $derived.by(() => {
		if (selectedManifests.size === 0) return new Set<string>();
		const cascade = deleteCascade(manifests, [...selectedManifests], sparedManifests);
		return new Set([...cascade.platforms, ...cascade.referrers]);
	});

	// One checkbox, three meanings: a row the cascade holds is spared or taken
	// back, anything else is picked or dropped from the selection itself.
	function toggleManifestSelection(digest: string) {
		if (sparedManifests.has(digest)) {
			const spared = new Set(sparedManifests);
			spared.delete(digest);
			sparedManifests = spared;
			return;
		}
		if (impliedDeletes.has(digest)) {
			sparedManifests = new Set(sparedManifests).add(digest);
			return;
		}
		const selected = new Set(selectedManifests);
		if (selected.has(digest)) {
			selected.delete(digest);
		} else {
			selected.add(digest);
		}
		selectedManifests = selected;
	}

	// The confirm label of a digest delete names what it takes along.
	const deleteConfirmLabel = (digests: string[]) =>
		`confirm${cascadeSummary(deleteCascade(manifests, digests, sparedManifests))}`;

	// Deletes `digests` and what their going orphans, the roots first so a
	// child is only ever removed once nothing names it. One request each: the
	// registry has no bulk delete, and deleting stays per manifest there.
	async function deleteWithCascade(digests: string[]) {
		const rootResults = await Promise.all(
			digests.map((digest) => apiDeleteManifest(data.path, digest))
		);
		const rootsGone = digests.filter((_, i) => rootResults[i] === null);
		const cascade = deleteCascade(manifests, rootsGone, sparedManifests);
		const extra = [...cascade.platforms, ...cascade.referrers];
		const extraResults = await Promise.all(
			extra.map((digest) => apiDeleteManifest(data.path, digest))
		);
		const gone = new Set([...rootsGone, ...extra.filter((_, i) => extraResults[i] === null)]);
		return { gone, failed: digests.length + extra.length - gone.size, total: digests.length + extra.length };
	}

	async function deleteByReference(reference: string) {
		// A tag cannot hold a colon; a digest always does. A tag delete keeps
		// the manifest, so nothing cascades from it.
		if (reference.includes(':')) {
			deleting = true;
			actionError = null;
			const { gone, failed, total } = await deleteWithCascade([reference]);
			rows = rows.filter((row) => !gone.has(row.digest));
			deleteConfirm = null;
			deleting = false;
			if (failed > 0) {
				actionError = `Failed to delete ${failed} of ${total} manifests.`;
			}
			if (gone.has(reference)) {
				await reloadAfterDelete(reference);
			}
			return;
		}
		deleting = true;
		actionError = null;
		const err = await apiDeleteManifest(data.path, reference);
		// The refresh below re-lists the whole namespace, which costs far more
		// than the delete it reflects. Holding every delete control disabled for
		// it is what made deleting a tag feel slow, so the controls come back as
		// soon as the registry has answered.
		deleting = false;
		if (err) {
			actionError = `Delete failed (${err}).`;
			return;
		}
		deleteConfirm = null;
		// Drop the tag from its row at once, so the click shows straight away;
		// the refresh reconciles whatever else the delete reclaimed.
		rows = rows.map((row) =>
			row.tags.includes(reference)
				? { ...row, tags: row.tags.filter((tag) => tag !== reference) }
				: row
		);
		await reloadAfterDelete(reference);
	}

	// Deleting by digest removes the manifest and every tag pointing at it, so
	// there is nothing left to reload here and the view always leaves for the
	// namespace.
	async function deleteByHash() {
		if (!digest) return;
		deleting = true;
		actionError = null;
		const { gone, failed, total } = await deleteWithCascade([digest]);
		deleting = false;
		if (!gone.has(digest)) {
			actionError = `Delete failed.`;
			return;
		}
		// The view leaves for the namespace; a cascade failure is reported there.
		if (failed > 0) {
			actionError = `Failed to delete ${failed} of ${total} manifests.`;
		}
		await goto(pathUrl(data.path));
	}

	async function cancelUpload(uuid: string) {
		deleting = true;
		actionError = null;
		const err = await apiCancelUpload(data.path, uuid);
		if (err) {
			actionError = `Cancel failed (${err}).`;
		} else {
			deleteConfirm = null;
			await loadBrowse(data.path, true);
		}
		deleting = false;
	}

	// One request per manifest: the registry has no bulk delete. Rows that
	// went are dropped at once and the controls come back before the refresh,
	// which reconciles what the deletes cascaded to.
	async function deleteSelectedManifests() {
		deleting = true;
		actionError = null;
		const { gone, failed, total } = await deleteWithCascade([...selectedManifests]);
		rows = rows.filter((row) => !gone.has(row.digest));
		selectedManifests = new Set();
		sparedManifests = new Set();
		selectingManifests = false;
		deleteConfirm = null;
		deleting = false;
		if (failed > 0) {
			actionError = `Failed to delete ${failed} of ${total} manifests.`;
		}
		await loadBrowse(data.path, true);
	}

	async function cancelSelectedUploads() {
		deleting = true;
		actionError = null;
		const uuids = [...selectedUploads];
		const results = await Promise.all(
			uuids.map((uuid) => apiCancelUpload(data.path, uuid))
		);
		deleteConfirm = null;
		await loadBrowse(data.path, true);
		const failed = results.filter((err) => err !== null).length;
		if (failed > 0) {
			actionError = `Failed to cancel ${failed} of ${uuids.length} uploads.`;
		}
		deleting = false;
	}
</script>

<svelte:head>
	<title>{getRegistryName()} &gt; {data.path}{isManifestView ? ` > ${data.reference}` : ''}</title>
</svelte:head>

<Breadcrumb items={[
	{ label: 'Repositories', href: `${base}/` },
	...(data.repository === null
		? [{ label: data.path, href: pathUrl(data.path) }]
		: [
			{ label: data.repository, href: pathUrl(data.repository) },
			...(relativePath === '' ? [] : [{ label: relativePath, href: pathUrl(data.path) }])
		]),
	...(isManifestView ? [{ label: data.reference ?? '' }] : [])
]} />

<div class="title" class:digest={data.reference?.startsWith('sha256:')}>
	<h1>{data.path}{#if isManifestView}<span class="reference">{fullName.slice(data.path.length)}</span>{/if}</h1>
	<CopyButton text={fullName} label={isManifestView ? 'Copy the reference' : 'Copy the namespace'} />
</div>
{#if !isManifestView}
	<!-- The count and the manifest-table actions share a line. -->
	<div class="lede-row">
		<p class="lede">{loading ? '\u00a0' : summary}</p>
		{#if rows.length > 0}
			<div class="table-actions">
				{#if selectingManifests && selectedManifests.size > 0}
					<DeleteButton
						label={`delete selected (${selectedManifests.size + impliedDeletes.size})`}
						confirmLabel={deleteConfirm === selectedManifestsConfirmKey
							? deleteConfirmLabel([...selectedManifests])
							: 'confirm'}
						isConfirming={deleteConfirm === selectedManifestsConfirmKey}
						disabled={deleting}
						onconfirm={deleteSelectedManifests}
						oncancel={() => (deleteConfirm = null)}
						onrequestconfirm={() => (deleteConfirm = selectedManifestsConfirmKey)}
					/>
				{/if}
				<button
					class="secondary"
					onclick={() => {
						selectingManifests = !selectingManifests;
						selectedManifests = new Set();
						sparedManifests = new Set();
						deleteConfirm = null;
					}}
					disabled={deleting}
				>
					{selectingManifests ? 'Done' : 'Select'}
				</button>
			</div>
		{/if}
	</div>
{/if}

{#if actionError}
	<div class="action-error">{actionError}</div>
{/if}

{#if loading}
	<LoadingState message={isManifestView ? 'Loading manifest' : 'Loading'} />
{:else if error}
	<ErrorState message={error} />
{:else if data.reference !== null && manifest}
	<ManifestView
		path={data.path}
		reference={data.reference}
		{manifest}
		{digest}
		{tags}
		{referencedBy}
		{childReferrers}
		{childReferrersNext}
		{loadingReferrers}
		onloadmorereferrers={loadMoreReferrers}
		{deleteConfirm}
		{deleting}
		ondeletetag={deleteByReference}
		ondeletebyhash={deleteByHash}
		deleteconfirmlabel={digest ? deleteConfirmLabel([digest]) : 'confirm'}
		onconfirmchange={(value) => deleteConfirm = value}
		getbloburl={(blobDigest) => blobUrl(data.path, blobDigest)}
	/>
{:else}
	{#if pullThroughCache || immutableTags}
		<div class="config-panel">
			{#if pullThroughCache}
				<div class="config-item">
					<span class="config-label">Upstream</span>
					<span class="config-value">{upstreamUrls.join(', ')}</span>
				</div>
			{/if}
			{#if immutableTags}
				<div class="config-item">
					<span class="config-label">Immutable tags</span>
					{#if immutableTagsExclusions.length > 0}
						<span class="config-value">except: {immutableTagsExclusions.join(', ')}</span>
					{:else}
						<span class="config-value enabled">Enabled</span>
					{/if}
				</div>
			{/if}
		</div>
	{/if}

	{#if children.length > 0}
		<Card title="Namespaces" count={children.length}>
			<table>
				<thead>
					<tr>
						<th>Namespace</th>
						<th class="col-medium">Tags</th>
						<th class="col-medium">Manifests</th>
						<th class="col-medium">Uploads</th>
					</tr>
				</thead>
				<tbody>
					{#each children as child (child.path)}
						{@const href = pathUrl(child.path)}
						<tr class="clickable" onclick={(event) => { if (!isInteractiveTarget(event)) goto(href); }}>
							<td><a class="row-link" {href}>{child.label}</a></td>
							<td>{child.tag_count ? child.tag_count : '-'}</td>
							<td>{child.manifest_count ? child.manifest_count : '-'}</td>
							<td>{child.upload_count ? child.upload_count : '-'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</Card>
	{/if}

	<!-- A pure prefix lists its namespaces alone; the manifest table appears
	     when the path holds manifests or uploads, or nothing at all. -->
	{#if rows.length > 0 || uploads.length > 0 || children.length === 0}
	<RepositoryTree
		path={data.path}
		{rows}
		{uploads}
		{selectedUploads}
		{selectingManifests}
		{selectedManifests}
		{impliedDeletes}
		{deleteConfirm}
		{deleting}
		{expanded}
		ontoggleexpand={toggleExpand}
		onconfirmchange={(value) => deleteConfirm = value}
		ondeletemanifest={deleteByReference}
		ondeletetag={deleteByReference}
		oncancelupload={cancelUpload}
		onuploadselectionchange={(selected) => selectedUploads = selected}
		oncancelselecteduploads={cancelSelectedUploads}
		onmanifestselectionchange={(selected) => selectedManifests = selected}
		{toggleManifestSelection}
		getdeleteconfirmlabel={deleteConfirmLabel}
	/>
	{/if}
{/if}

<style>
	/* The count and the table's actions on one line, the actions to the right. */
	.lede-row {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
		margin-bottom: 1.25rem;
	}
	.lede-row .lede {
		margin: 0;
	}
	.table-actions {
		display: flex;
		align-items: center;
		gap: 0.5rem;
	}
	.title {
		display: flex;
		align-items: center;
		gap: 0.375rem;
		margin-bottom: 1.25rem;
	}
	.title h1 {
		margin: 0;
		min-width: 0;
	}
	/* The reference reads lighter than the name it follows; a digest is long
	   enough that the whole line steps down a size to stay one line. */
	.reference {
		font-weight: 400;
	}
	.title.digest h1 {
		font-size: 1.25rem;
	}
</style>
