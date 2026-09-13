<script lang="ts">
	import type { LayerEntryKind } from '$lib/api';

	let { kind, open = false }: { kind: LayerEntryKind; open?: boolean } = $props();
</script>

{#if kind === 'dir' || kind === 'opaque'}
	{#if open}
		<svg class="fs-icon dir" viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true">
			<path d="M3 7.5A1.5 1.5 0 0 1 4.5 6h4l1.7 1.7a1 1 0 0 0 .7.3h7.6A1.5 1.5 0 0 1 19.5 9.5V11" />
			<path d="M3 17.5V7.5M3.5 19h14.9a1.5 1.5 0 0 0 1.4-1L22 12.5a1 1 0 0 0-.95-1.3H7.4a1.5 1.5 0 0 0-1.4 1L3.5 19Z" />
		</svg>
	{:else}
		<svg class="fs-icon dir" viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true">
			<path d="M3 7.5A1.5 1.5 0 0 1 4.5 6h4l1.7 1.7a1 1 0 0 0 .7.3h8.6A1.5 1.5 0 0 1 21 9.5v8A1.5 1.5 0 0 1 19.5 19h-15A1.5 1.5 0 0 1 3 17.5Z" />
		</svg>
	{/if}
{:else if kind === 'symlink'}
	<svg class="fs-icon symlink" viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true">
		<path d="M14 3v4a1 1 0 0 0 1 1h4" />
		<path d="M18 21H6a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h8l5 5v12a1 1 0 0 1-1 1Z" />
		<path d="m9.5 16 4-4M13.5 16v-4h-4" />
	</svg>
{:else}
	<svg class="fs-icon file" viewBox="0 0 24 24" fill="none" stroke="currentColor" aria-hidden="true">
		<path d="M14 3v4a1 1 0 0 0 1 1h4" />
		<path d="M18 21H6a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h8l5 5v12a1 1 0 0 1-1 1Z" />
	</svg>
{/if}
