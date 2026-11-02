<script lang="ts">
	import { page } from '$app/state';
	import ErrorState from '#lib/components/ErrorState.svelte';
</script>

<ErrorState message={page.error?.message ?? `HTTP ${page.status}`} />
