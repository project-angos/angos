<script lang="ts">
	import { formatTimeAgo, digestConfirmKey, getTagConfirm, tagConfirmKey, type TreeRowNode } from '$lib/utils';
	import TagList from './TagList.svelte';
	import PlatformBadge from './PlatformBadge.svelte';
	import AttestationBadge from './AttestationBadge.svelte';
	import DeleteButton from './DeleteButton.svelte';
	import TreeRow from './TreeRow.svelte';

	interface Props {
		node: TreeRowNode;
		depth: number;
		/** Whether the parent row has a following sibling (drives the nested parent line). */
		parentHasNext?: boolean;
		expanded: Set<string>;
		deleteConfirm: string | null;
		deleting: boolean;
		ontoggleexpand: (digest: string, event: MouseEvent) => void;
		onrowclick: (event: MouseEvent, digest: string) => void;
		ondeletemanifest: (digest: string) => void;
		ondeletetag: (tag: string) => void;
		onconfirmchange: (value: string | null) => void;
		gettaghref: (tag: string) => string;
	}

	let {
		node,
		depth,
		parentHasNext = false,
		expanded,
		deleteConfirm,
		deleting,
		ontoggleexpand,
		onrowclick,
		ondeletemanifest,
		ondeletetag,
		onconfirmchange,
		gettaghref
	}: Props = $props();

	const isExpanded = $derived(expanded.has(node.digest));
	const cellClass = $derived(
		depth === 0 ? 'has-children' : depth === 1 ? 'tree-branch' : 'nested-tree-branch'
	);
	const rowClass = $derived(depth === 0 ? 'root-row clickable' : 'child-row clickable');
</script>

<tr class={rowClass} onclick={(e) => onrowclick(e, node.digest)}>
	<td
		class={cellClass}
		class:expanded={depth === 0 && isExpanded}
		class:has-next={depth > 0 && node.hasNext}
		class:has-attestations={depth === 1 && node.canExpand && isExpanded}
	>{#if depth === 2 && parentHasNext}<span class="parent-line"></span>{/if}{#if node.canExpand}<button class="tree-toggle" onclick={(e) => ontoggleexpand(node.digest, e)}><span class="toggle-icon">{isExpanded ? '−' : '+'}</span></button>{#if depth === 1 && isExpanded}<span class="branch-line"></span>{/if}{:else}<span class="tree-toggle leaf"></span>{/if}<code>{node.digest}</code></td>
	<td>
		{#if node.kind === 'root'}
			<TagList
				tags={node.tags}
				deleteConfirm={getTagConfirm(deleteConfirm)}
				disabled={deleting}
				ondelete={ondeletetag}
				onconfirmchange={(tag) => onconfirmchange(tag ? tagConfirmKey(tag) : null)}
				getHref={gettaghref}
			/>
		{:else if node.kind === 'child'}
			<PlatformBadge platform={node.platform} />
		{:else if node.attestationType}
			<AttestationBadge type={node.attestationType} />
		{/if}
	</td>
	<td>{#if node.showDates}{node.pushed_at ? formatTimeAgo(node.pushed_at) : '-'}{/if}</td>
	<td>{#if node.showDates}{node.last_pulled_at ? formatTimeAgo(node.last_pulled_at) : '-'}{/if}</td>
	<td>
		<DeleteButton
			isConfirming={deleteConfirm === digestConfirmKey(node.digest)}
			disabled={deleting}
			onconfirm={() => ondeletemanifest(node.digest)}
			oncancel={() => onconfirmchange(null)}
			onrequestconfirm={() => onconfirmchange(digestConfirmKey(node.digest))}
		/>
	</td>
</tr>
{#if isExpanded}
	{#each node.children as child (`${child.kind}:${child.digest}`)}
		<TreeRow
			node={child}
			depth={depth + 1}
			parentHasNext={node.hasNext}
			{expanded}
			{deleteConfirm}
			{deleting}
			{ontoggleexpand}
			{onrowclick}
			{ondeletemanifest}
			{ondeletetag}
			{onconfirmchange}
			{gettaghref}
		/>
	{/each}
{/if}
