<script lang="ts">
	import '../app.css';
	import { base } from '$app/paths';
	import { page } from '$app/state';
	import { onMount } from 'svelte';
	import ThemeSwitcher from '$lib/components/ThemeSwitcher.svelte';
	import SignIn from '$lib/components/SignIn.svelte';
	import ErrorState from '$lib/components/ErrorState.svelte';
	import { loadConfig, getRegistryName } from '$lib/config.svelte';
	import { completeSignIn } from '$lib/auth.svelte';
	import { trail } from '$lib/trail.svelte';

	let { children } = $props();
	let registryName = $state('Angos');
	let signInError: string | null = $state(null);

	onMount(async () => {
		await loadConfig();
		registryName = getRegistryName();
		signInError = await completeSignIn();
	});

	const pathname = $derived(page.url.pathname.slice(base.length));
	const here = (path: string) => (pathname === path ? 'page' : undefined);
</script>

<header>
	<a class="brand" href="{base}/">
		<svg viewBox="0 0 100 100" aria-hidden="true">
			<rect x="10" y="18" width="50" height="10" rx="3" fill="currentColor" />
			<rect x="10" y="36" width="60" height="10" rx="3" fill="currentColor" opacity="0.7" />
			<rect x="10" y="54" width="70" height="10" rx="3" fill="currentColor" opacity="0.5" />
			<rect x="10" y="72" width="80" height="10" rx="3" fill="currentColor" opacity="0.3" />
		</svg>
		{registryName}
	</a>
	<nav class="crumbs" aria-label="Breadcrumb">
		{#each trail.items as item, i (i)}
			<span class="sep" aria-hidden="true">/</span>
			{#if item.href && i < trail.items.length - 1}
				<a class="step" href={item.href}>{item.label}</a>
			{:else}
				<span class="here" aria-current="page">{item.label}</span>
			{/if}
		{/each}
	</nav>
	<nav class="pages">
		<a href="{base}/" aria-current={here('/')}>Repositories</a>
		<a href="{base}/jobs" aria-current={pathname.startsWith('/jobs') ? 'page' : undefined}>Jobs</a>
	</nav>
	<div class="theme">
		<SignIn />
		<ThemeSwitcher />
	</div>
</header>

<main>
	{#if signInError}
		<ErrorState message={signInError} />
	{/if}
	{@render children()}
</main>

<style>
	/* One bar over a centered page; the bar stays put while the page scrolls. */
	header {
		position: sticky;
		top: 0;
		z-index: 10;
		height: 3rem;
		display: flex;
		align-items: center;
		gap: 0.5rem;
		padding: 0 0.875rem;
		background: var(--surface);
		border-bottom: 1px solid var(--border);
	}
	main {
		max-width: 75rem;
		margin: 0 auto;
		padding: 1.5rem;
	}

	.brand {
		flex: none;
		display: flex;
		align-items: center;
		gap: 0.5rem;
		padding: 0.25rem 0.5rem 0.25rem 0;
		color: var(--text);
		font-size: 0.9375rem;
		font-weight: 600;
		letter-spacing: -0.01em;
		white-space: nowrap;
		text-decoration: none;
	}
	.brand svg {
		width: 1.25rem;
		height: 1.25rem;
		flex: none;
		color: var(--accent);
	}

	.crumbs {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		min-width: 0;
		margin-right: auto;
	}
	.sep {
		color: var(--border-strong);
	}
	.here,
	.step {
		font-size: 0.875rem;
		font-weight: 500;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	/* A long current crumb, a digest, ellipsizes before the steps give way. */
	.step {
		flex: none;
		color: var(--muted);
		text-decoration: none;
	}
	.here {
		min-width: 6rem;
	}
	.step:hover {
		color: var(--text);
		text-decoration: none;
	}

	.pages {
		flex: none;
		display: flex;
		gap: 0.125rem;
	}
	.theme {
		flex: none;
		display: flex;
		align-items: center;
		gap: 0.5rem;
	}
	.pages a {
		padding: 0.25rem 0.5rem;
		border-radius: var(--radius-sm);
		color: var(--muted);
		font-size: 0.875rem;
		font-weight: 500;
		text-decoration: none;
		white-space: nowrap;
		transition: background 0.1s;
	}
	.pages a:hover {
		background: var(--hover);
		color: var(--text);
	}
	.pages a[aria-current='page'] {
		background: var(--accent-soft);
		color: var(--accent-hover);
	}

	@media (max-width: 48rem) {
		/* The trail moves under the bar so the brand and the pages keep their line. */
		header {
			height: auto;
			flex-wrap: wrap;
			padding: 0.5rem 0.875rem;
		}
		.brand {
			margin-right: auto;
		}
		.crumbs {
			order: 1;
			flex: 1 1 60%;
			margin: 0;
		}
		.crumbs .sep:first-child {
			display: none;
		}
		.step {
			flex: 0 1 auto;
			min-width: 3rem;
		}
		.theme {
			order: 2;
		}
		main {
			padding: 1.25rem;
		}
	}
</style>
