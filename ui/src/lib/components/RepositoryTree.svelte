<script lang="ts">
	import type { RevisionSort, SortOrder, UploadEntry } from '#lib/api.js';
	import { formatSize, formatTimeAgo, selectedUploadsConfirmKey, type TreeRowNode } from '#lib/utils.js';
	import Card from './Card.svelte';
	import DeleteButton from './DeleteButton.svelte';
	import SortHeader from './SortHeader.svelte';
	import TreeRow from './TreeRow.svelte';

	interface Props {
		path: string;
		rows: TreeRowNode[];
		uploads: UploadEntry[];
		selectedUploads: Set<string>;
		selectingManifests: boolean;
		selectedManifests: Set<string>;
		/** Digests the selection would delete as orphans, ticked and locked. */
		impliedDeletes: Set<string>;
		deleteConfirm: string | null;
		deleting: boolean;
		expanded: Set<string>;
		ontoggleexpand: (digest: string, event: MouseEvent) => void;
		onconfirmchange: (value: string | null) => void;
		/** Deletes a manifest by digest or a tag by name. */
		ondelete: (reference: string) => void;
		oncancelupload: (uuid: string) => void;
		onuploadselectionchange: (selected: Set<string>) => void;
		oncancelselecteduploads: () => void;
		onmanifestselectionchange: (selected: Set<string>) => void;
		/** One row's checkbox: picks, drops, spares or takes back. */
		toggleManifestSelection: (digest: string) => void;
		getdeleteconfirmlabel: (digests: string[]) => string;
		manifestSort: RevisionSort;
		manifestOrder: SortOrder;
		/** A re-sort is loading: the sorted column's header spins. */
		sortingManifests: boolean;
		onsortmanifests: (sort: RevisionSort) => void;
		/** Uploads across every page; `uploads` holds those loaded. */
		uploadsTotal: number;
		/** Whether a listing holds a page past those loaded. */
		moreUploads: boolean;
		moreManifests: boolean;
		/** The listing whose next page is loading. */
		loadingMore: string | null;
		onloadmoreuploads: () => void;
		onloadmoremanifests: () => void;
	}

	let {
		path,
		rows,
		uploads,
		selectedUploads,
		selectingManifests,
		selectedManifests,
		impliedDeletes,
		deleteConfirm,
		deleting,
		expanded,
		ontoggleexpand,
		onconfirmchange,
		ondelete,
		oncancelupload,
		onuploadselectionchange,
		oncancelselecteduploads,
		onmanifestselectionchange,
		toggleManifestSelection,
		getdeleteconfirmlabel,
		manifestSort,
		manifestOrder,
		sortingManifests,
		onsortmanifests,
		uploadsTotal,
		moreUploads,
		moreManifests,
		loadingMore,
		onloadmoreuploads,
		onloadmoremanifests
	}: Props = $props();

	// Select-all covers the rows on screen at depth zero; a nested row is
	// picked on its own once its parent is expanded.
	const allManifestsSelected = $derived(
		rows.length > 0 && rows.every((row) => selectedManifests.has(row.digest))
	);

	function toggleAllManifests() {
		onmanifestselectionchange(
			allManifestsSelected ? new Set() : new Set(rows.map((row) => row.digest))
		);
	}

	const allUploadsSelected = $derived(
		uploads.length > 0 && uploads.every((upload) => selectedUploads.has(upload.uuid))
	);

	function toggleUploadSelection(uuid: string) {
		const selected = new Set(selectedUploads);
		if (!selected.delete(uuid)) selected.add(uuid);
		onuploadselectionchange(selected);
	}

	function toggleAllUploads() {
		onuploadselectionchange(
			allUploadsSelected ? new Set() : new Set(uploads.map((upload) => upload.uuid))
		);
	}
</script>

{#if uploads.length > 0}
	<Card title="Uploads in progress" count={uploadsTotal} variant="warning">
		{#snippet headerActions()}
			{#if selectedUploads.size > 0}
				<DeleteButton
					label={`cancel selected (${selectedUploads.size})`}
					isConfirming={deleteConfirm === selectedUploadsConfirmKey}
					disabled={deleting}
					onconfirm={oncancelselecteduploads}
					oncancel={() => onconfirmchange(null)}
					onrequestconfirm={() => onconfirmchange(selectedUploadsConfirmKey)}
				/>
			{/if}
		{/snippet}
		<table>
			<thead>
				<tr>
					<th class="col-select">
						<label>
							<input
								type="checkbox"
								aria-label="Select all uploads"
								checked={allUploadsSelected}
								disabled={deleting}
								onchange={toggleAllUploads}
							/>
						</label>
					</th>
					<th>UUID</th>
					<th>Size</th>
					<th>Started</th>
					<th class="col-medium">Actions</th>
				</tr>
			</thead>
			<tbody>
				{#each uploads as upload}
					<tr>
						<td class="col-select">
							<label>
								<input
									type="checkbox"
									aria-label={`Select upload ${upload.uuid}`}
									checked={selectedUploads.has(upload.uuid)}
									disabled={deleting}
									onchange={() => toggleUploadSelection(upload.uuid)}
								/>
							</label>
						</td>
						<td><code class="uuid">{upload.uuid}</code></td>
						<td class="nowrap">{formatSize(upload.size)}</td>
						<td>{formatTimeAgo(upload.started_at)}</td>
						<td>
							<DeleteButton
								label="cancel"
								isConfirming={deleteConfirm === `upload:${upload.uuid}`}
								disabled={deleting}
								onconfirm={() => oncancelupload(upload.uuid)}
								oncancel={() => onconfirmchange(null)}
								onrequestconfirm={() => onconfirmchange(`upload:${upload.uuid}`)}
							/>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
		{#if moreUploads}
			<div class="load-more">
				<button class="secondary" onclick={onloadmoreuploads} disabled={loadingMore === 'uploads'}>Load more</button>
			</div>
		{/if}
	</Card>
{/if}

<table>
	<thead>
		<tr>
			{#if selectingManifests}
				<th class="col-select">
					<label>
						<input
							type="checkbox"
							aria-label="Select all manifests"
							checked={allManifestsSelected}
							disabled={deleting}
							onchange={toggleAllManifests}
						/>
					</label>
				</th>
			{/if}
			<SortHeader
				label="Digest"
				order={manifestSort === 'digest' ? manifestOrder : null}
				busy={sortingManifests && manifestSort === 'digest'}
				onsort={() => onsortmanifests('digest')}
			/>
			<SortHeader
				label="Tags / Platform"
				order={manifestSort === 'tag' ? manifestOrder : null}
				busy={sortingManifests && manifestSort === 'tag'}
				onsort={() => onsortmanifests('tag')}
			/>
			<th>Pushed</th>
			<th>Pulled</th>
			<th class="col-actions">Actions</th>
		</tr>
	</thead>
	<tbody>
		{#if rows.length === 0}
			<tr>
				<td colspan={selectingManifests ? 6 : 5} class="empty">No manifests found</td>
			</tr>
		{:else}
			{#each rows as node (`${node.kind}:${node.digest}`)}
				<TreeRow
					{path}
					{node}
					depth={0}
					{expanded}
					{deleteConfirm}
					{deleting}
					selecting={selectingManifests}
					selected={selectedManifests}
					implied={impliedDeletes}
					ontoggleselect={toggleManifestSelection}
					{getdeleteconfirmlabel}
					{ontoggleexpand}
					{ondelete}
					{onconfirmchange}
				/>
			{/each}
		{/if}
	</tbody>
</table>
{#if moreManifests}
	<div class="load-more">
		<button class="secondary" onclick={onloadmoremanifests} disabled={loadingMore === 'manifests'}>Load more</button>
	</div>
{/if}
