<script lang="ts">
	import { goto } from '$app/navigation';
	import {
		formatTimeAgo,
		isInteractiveTarget,
		manifestUrl,
		type TreeRowNode
	} from '#lib/utils.js';
	import TagList from './TagList.svelte';
	import PlatformBadge from './PlatformBadge.svelte';
	import AttestationBadge from './AttestationBadge.svelte';
	import ScanSummary from './ScanSummary.svelte';
	import DeleteButton from './DeleteButton.svelte';
	import TreeRow from './TreeRow.svelte';

	interface Props {
		path: string;
		node: TreeRowNode;
		depth: number;
		parentHasNext?: boolean;
		expanded: Set<string>;
		deleteConfirm: string | null;
		deleting: boolean;
		selecting: boolean;
		selected: Set<string>;
		implied: Set<string>;
		ontoggleselect: (digest: string) => void;
		getdeleteconfirmlabel: (digests: string[]) => string;
		ontoggleexpand: (digest: string, event: MouseEvent) => void;
		ondelete: (reference: string) => void;
		onconfirmchange: (value: string | null) => void;
	}

	let {
		path,
		node,
		depth,
		parentHasNext = false,
		expanded,
		deleteConfirm,
		deleting,
		selecting,
		selected,
		implied,
		ontoggleselect,
		getdeleteconfirmlabel,
		ontoggleexpand,
		ondelete,
		onconfirmchange
	}: Props = $props();

	const isExpanded = $derived(expanded.has(node.digest));
	const cellClass = $derived(
		depth === 0 ? 'has-children' : depth === 1 ? 'tree-branch' : 'nested-tree-branch'
	);
	const rowClass = $derived(depth === 0 ? 'root-row clickable' : 'child-row clickable');
	const confirmKey = $derived(`digest:${node.digest}`);
</script>

<tr class={rowClass} onclick={(e) => { if (!isInteractiveTarget(e)) goto(manifestUrl(path, node.digest)); }}>
	{#if selecting}
		{@const isImplied = implied.has(node.digest)}
		<td class="col-select">
			<label
				title={isImplied
					? 'Deleted with the manifest that refers to it; untick to keep it'
					: undefined}
			>
				<input
					type="checkbox"
					aria-label={`Select manifest ${node.digest}`}
					checked={selected.has(node.digest) || isImplied}
					disabled={deleting}
					onchange={() => ontoggleselect(node.digest)}
				/>
			</label>
		</td>
	{/if}
	<td
		class={cellClass}
		class:expanded={depth === 0 && isExpanded}
		class:has-next={depth > 0 && node.hasNext}
		class:has-attestations={depth === 1 && node.canExpand && isExpanded}
	>{#if depth === 2 && parentHasNext}<span class="parent-line"></span>{/if}{#if node.canExpand}<button class="tree-toggle" onclick={(e) => ontoggleexpand(node.digest, e)}><span class="toggle-icon">{isExpanded ? '−' : '+'}</span></button>{#if depth === 1 && isExpanded}<span class="branch-line"></span>{/if}{:else}<span class="tree-toggle leaf"></span>{/if}<a class="row-link" href={manifestUrl(path, node.digest)}><code>{node.digest}</code></a></td>
	<td>
		{#if node.kind === 'root'}
			<TagList
				tags={node.tags}
				{deleteConfirm}
				disabled={deleting}
				{ondelete}
				{onconfirmchange}
				getHref={(tag) => manifestUrl(path, tag)}
			/>
		{:else if node.kind === 'child'}
			<PlatformBadge platform={node.platform} />
		{:else if node.attestationType}
			<span class="nowrap">
				<AttestationBadge type={node.attestationType} />
				<ScanSummary annotations={node.annotations} compact />
			</span>
		{/if}
	</td>
	<td>{#if node.showDates}{node.pushed_at ? formatTimeAgo(node.pushed_at) : '-'}{/if}</td>
	<td>{#if node.showDates}{node.last_pulled_at ? formatTimeAgo(node.last_pulled_at) : '-'}{/if}</td>
	<td>
		<DeleteButton
			isConfirming={deleteConfirm === confirmKey}
			confirmLabel={deleteConfirm === confirmKey ? getdeleteconfirmlabel([node.digest]) : 'confirm'}
			disabled={deleting}
			onconfirm={() => ondelete(node.digest)}
			oncancel={() => onconfirmchange(null)}
			onrequestconfirm={() => onconfirmchange(confirmKey)}
		/>
	</td>
</tr>
{#if isExpanded}
	{#each node.children as child (`${child.kind}:${child.digest}`)}
		<TreeRow
			{path}
			node={child}
			depth={depth + 1}
			parentHasNext={node.hasNext}
			{expanded}
			{deleteConfirm}
			{deleting}
			{selecting}
			{selected}
			{implied}
			{ontoggleselect}
			{getdeleteconfirmlabel}
			{ontoggleexpand}
			{ondelete}
			{onconfirmchange}
		/>
	{/each}
{/if}
