<script lang="ts">
	import {
		formatMode,
		formatSize,
		fsMatches,
		fsVisible,
		sortedChildren,
		type FsMatcher,
		type FsNode
	} from '$lib/utils';
	import FsIcon from './FsIcon.svelte';
	import FsRow from './FsRow.svelte';

	interface Props {
		node: FsNode;
		depth: number;
		/** Whether each ancestor column carries a continuing guide line. */
		guides: boolean[];
		/** Whether a sibling follows this node, so its own guide line continues. */
		hasNext: boolean;
		expanded: Set<string>;
		matcher: FsMatcher;
		selected: string | null;
		/** The keyboard's row, the one the list's focus rests on. */
		cursor: string | null;
		ontoggle: (path: string) => void;
		onopen: (node: FsNode) => void;
		layerlabel: (layer: number) => string;
	}

	let { node, depth, guides, hasNext, expanded, matcher, selected, cursor, ontoggle, onopen, layerlabel }: Props =
		$props();

	const filtering = $derived(matcher.layers.size > 0 || matcher.text !== '');
	const shown = $derived(!filtering || fsVisible(matcher, node));
	// A narrowed tree opens itself: the matches are what the reader asked for.
	const open = $derived(node.kind === 'dir' && (matcher.text !== '' || expanded.has(node.path)));
	const kids = $derived(node.kind === 'dir' ? sortedChildren(node) : []);
	const modified = $derived(node.entry ? new Date(node.entry.mtime * 1000).toISOString().slice(0, 10) : '');
</script>

{#if shown}
	<tr
		class="clickable"
		class:dim={filtering && !fsMatches(matcher, node)}
		class:selected={selected === node.path}
		class:cursor={cursor === node.path}
		tabindex={cursor === node.path ? 0 : -1}
		onclick={() => (node.kind === 'dir' ? ontoggle(node.path) : onopen(node))}
	>
		<td class="fs-name" style="--depth: {depth}" title={node.path}>
			{#each guides as guide, i}
				{#if guide}<span class="fs-guide" style="--col: {i}"></span>{/if}
			{/each}
			{#if depth > 0}<span class="fs-elbow" class:last={!hasNext} style="--col: {depth - 1}"></span>{/if}
			{#if open && kids.length > 0}<span class="fs-spine" style="--col: {depth}"></span>{/if}
			<FsIcon kind={node.kind} {open} />
			<span class="fs-label">{node.name}</span>
			{#if node.entry?.link}<span class="fs-link">→ {node.entry.link}</span>{/if}
		</td>
		<td class="nowrap fs-extra">{node.kind === 'file' ? formatSize(node.entry?.size ?? 0) : ''}</td>
		<td class="mono fs-extra">{node.entry ? formatMode(node.entry.mode) : ''}</td>
		<td class="nowrap fs-extra">{modified}</td>
		<td><span class="badge">{layerlabel(node.layer)}</span></td>
	</tr>
	{#if open}
		{#each kids as child, i (child.path)}
			<FsRow
				node={child}
				depth={depth + 1}
				guides={[...guides, hasNext]}
				hasNext={i < kids.length - 1}
				{expanded}
				{matcher}
				{selected}
				{cursor}
				{ontoggle}
				{onopen}
				{layerlabel}
			/>
		{/each}
	{/if}
{/if}

<style>
	.dim {
		color: var(--muted);
	}
</style>
