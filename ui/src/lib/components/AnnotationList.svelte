<script lang="ts">
	import { getAnnotationLabel, isUrl } from '#lib/utils.js';

	interface Props {
		annotations: Record<string, string>;
		format?: 'rows' | 'inline';
	}

	let { annotations, format = 'rows' }: Props = $props();
</script>

{#snippet text(value: string)}
	{#if isUrl(value)}
		<a href={value} target="_blank" rel="noopener">{value}</a>
	{:else}
		{value}
	{/if}
{/snippet}

{#each Object.entries(annotations) as [key, value]}
	{#if format === 'rows'}
		<tr class="annotations-row">
			<td class="label">{getAnnotationLabel(key)}</td>
			<td>{@render text(value)}</td>
		</tr>
	{:else}
		<span class="annotation">
			<span class="annotation-key">{getAnnotationLabel(key)}:</span>
			{@render text(value)}
		</span>
	{/if}
{/each}
