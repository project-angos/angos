<script lang="ts">
	import { goto } from '$app/navigation';
	import type { UploadEntry } from '$lib/api';
	import {
		formatSize,
		formatTimeAgo,
		isInteractiveTarget,
		manifestUrl,
		selectedUploadsConfirmKey,
		uploadConfirmKey,
		type TreeRowNode
	} from '$lib/utils';
	import Card from './Card.svelte';
	import DeleteButton from './DeleteButton.svelte';
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
		ondeletemanifest: (digest: string) => void;
		ondeletetag: (tag: string) => void;
		oncancelupload: (uuid: string) => void;
		onuploadselectionchange: (selected: Set<string>) => void;
		oncancelselecteduploads: () => void;
		onmanifestselectionchange: (selected: Set<string>) => void;
		/** One row's checkbox: picks, drops, spares or takes back. */
		toggleManifestSelection: (digest: string) => void;
		getdeleteconfirmlabel: (digests: string[]) => string;
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
		ondeletemanifest,
		ondeletetag,
		oncancelupload,
		onuploadselectionchange,
		oncancelselecteduploads,
		onmanifestselectionchange,
		toggleManifestSelection,
		getdeleteconfirmlabel
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
		if (selected.has(uuid)) {
			selected.delete(uuid);
		} else {
			selected.add(uuid);
		}
		onuploadselectionchange(selected);
	}

	function toggleAllUploads() {
		onuploadselectionchange(
			allUploadsSelected ? new Set() : new Set(uploads.map((upload) => upload.uuid))
		);
	}

	function handleRowClick(event: MouseEvent, targetDigest: string) {
		if (isInteractiveTarget(event)) return;
		goto(manifestUrl(path, targetDigest));
	}
</script>

{#if uploads.length > 0}
	<Card title="Uploads in progress" count={uploads.length} variant="warning">
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
								isConfirming={deleteConfirm === uploadConfirmKey(upload.uuid)}
								disabled={deleting}
								onconfirm={() => oncancelupload(upload.uuid)}
								oncancel={() => onconfirmchange(null)}
								onrequestconfirm={() => onconfirmchange(uploadConfirmKey(upload.uuid))}
							/>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
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
			<th>Digest</th>
			<th>Tags / Platform</th>
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
					onrowclick={handleRowClick}
					{ondeletemanifest}
					{ondeletetag}
					{onconfirmchange}
					gettaghref={(tag) => manifestUrl(path, tag)}
					getdigesthref={(rowDigest) => manifestUrl(path, rowDigest)}
				/>
			{/each}
		{/if}
	</tbody>
</table>

<style>
</style>
