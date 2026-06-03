<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Props {
		title?: string;
		count?: number;
		variant?: 'default' | 'warning';
		headerActions?: Snippet;
		children: Snippet;
	}

	let { title, count, variant = 'default', headerActions, children }: Props = $props();
</script>

<div class="card" class:warning={variant === 'warning'}>
	{#if title || headerActions}
		<div class="card-header">
			{#if title}
				<h3>{title}{#if count !== undefined} &nbsp ({count}){/if}</h3>
			{/if}
			{#if headerActions}
				<div class="header-actions">
					{@render headerActions()}
				</div>
			{/if}
		</div>
	{/if}
	{@render children()}
</div>
