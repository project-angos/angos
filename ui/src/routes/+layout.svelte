<script lang="ts">
	import '../app.css';
	import { base } from '$app/paths';
	import { onMount } from 'svelte';
	import ThemeSwitcher from '$lib/components/ThemeSwitcher.svelte';
	import { loadConfig, getRegistryName } from '$lib/config.svelte';

	let { children } = $props();
	let registryName = $state('Angos');

	onMount(async () => {
		await loadConfig();
		registryName = getRegistryName();
	});
</script>

<header>
	<div class="container header-content">
		<div class="header-left">
			<a href="{base}/" class="header-brand">
				<svg class="header-logo" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
					<rect x="10" y="18" width="50" height="10" rx="3" fill="currentColor"/>
					<rect x="10" y="36" width="60" height="10" rx="3" fill="currentColor" opacity="0.7"/>
					<rect x="10" y="54" width="70" height="10" rx="3" fill="currentColor" opacity="0.5"/>
					<rect x="10" y="72" width="80" height="10" rx="3" fill="currentColor" opacity="0.3"/>
				</svg>
				<h1>{registryName}</h1>
			</a>
			<nav>
				<a href="{base}/">Repositories</a>
				<a href="{base}/jobs">Jobs</a>
			</nav>
		</div>
		<ThemeSwitcher />
	</div>
</header>

<main class="container">
	{@render children()}
</main>

<style>
	.header-brand {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		text-decoration: none;
		color: inherit;
	}

	.header-brand:hover {
		text-decoration: none;
	}

	.header-logo {
		width: 32px;
		height: 32px;
		color: var(--color-primary);
	}
</style>
