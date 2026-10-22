<script lang="ts">
	import { getAnnotationLabel, isUrl } from '#lib/utils.js';

	interface Props {
		annotations: Record<string, string>;
		format?: 'rows' | 'inline';
	}

	let { annotations, format = 'rows' }: Props = $props();
</script>

{#if format === 'rows'}
	{#each Object.entries(annotations) as [key, value]}
		<tr class="annotations-row">
			<td class="label">{getAnnotationLabel(key)}</td>
			<td>
				{#if isUrl(value)}
					<a href={value} target="_blank" rel="noopener">{value}</a>
				{:else}
					{value}
				{/if}
			</td>
		</tr>
	{/each}
{:else}
	{#each Object.entries(annotations) as [key, value]}
		<span class="annotation">
			<span class="annotation-key">{getAnnotationLabel(key)}:</span>
			{#if isUrl(value)}
				<a href={value} target="_blank" rel="noopener">{value}</a>
			{:else}
				{value}
			{/if}
		</span>
	{/each}
{/if}
