<script lang="ts">
	import type { SortOrder } from '$lib/api';

	interface Props {
		label: string;
		/** The direction the listing is sorted in by this column, `null` when by another. */
		order: SortOrder | null;
		onsort: () => void;
		/** A re-sort is loading: a spinner stands in for the arrow. */
		busy?: boolean;
		disabled?: boolean;
	}

	let { label, order, onsort, busy = false, disabled = false }: Props = $props();
</script>

<th aria-sort={order === null ? undefined : order === 'asc' ? 'ascending' : 'descending'}>
	<button class="sort" class:busy onclick={onsort} {disabled} aria-busy={busy}>
		{label}{#if order !== null && !busy}<span aria-hidden="true">{order === 'asc' ? ' ↑' : ' ↓'}</span>{/if}
	</button>
</th>
