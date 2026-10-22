<script lang="ts">
	import { trail, type TrailItem } from '#lib/trail.svelte.js';

	interface Props {
		items: TrailItem[];
	}

	let { items }: Props = $props();

	// The trail renders in the layout's top bar; the page only names its steps.
	$effect(() => {
		trail.items = items;
	});
</script>
