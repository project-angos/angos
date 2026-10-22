<script lang="ts">
	import { onMount } from 'svelte';
	import { initTheme, setTheme, getThemePreference, type ThemePreference } from '#lib/theme.svelte.js';

	let current: ThemePreference = $state('system');

	onMount(() => {
		initTheme();
		current = getThemePreference();
	});

	function handleChange(newTheme: ThemePreference) {
		current = newTheme;
		setTheme(newTheme);
	}
</script>

<div class="theme-switcher" role="group" aria-label="Theme">
	<button
		class:active={current === 'light'}
		onclick={() => handleChange('light')}
		title="Light theme"
		aria-label="Light theme"
		aria-pressed={current === 'light'}
	>
		<svg viewBox="0 0 24 24" aria-hidden="true">
			<circle cx="12" cy="12" r="4" />
			<path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" />
		</svg>
	</button>
	<button
		class:active={current === 'dark'}
		onclick={() => handleChange('dark')}
		title="Dark theme"
		aria-label="Dark theme"
		aria-pressed={current === 'dark'}
	>
		<svg viewBox="0 0 24 24" aria-hidden="true">
			<path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z" />
		</svg>
	</button>
	<button
		class:active={current === 'system'}
		onclick={() => handleChange('system')}
		title="System theme"
		aria-label="System theme"
		aria-pressed={current === 'system'}
	>
		<svg viewBox="0 0 24 24" aria-hidden="true">
			<rect x="2" y="3" width="20" height="14" rx="2" />
			<path d="M8 21h8M12 17v4" />
		</svg>
	</button>
</div>

<style>
	/* Three quiet icon buttons in one ring; the chosen one is tinted. */
	.theme-switcher {
		display: inline-flex;
		flex: none;
		border-radius: var(--radius);
		box-shadow: var(--shadow-ring);
		overflow: hidden;
	}
	button {
		padding: 0.3rem 0.45rem;
		border-radius: 0;
		background: var(--surface);
		color: var(--muted);
		box-shadow: none;
	}
	button + button {
		border-left: 1px solid var(--border);
	}
	button:hover {
		background: var(--hover);
		color: var(--text);
	}
	button.active {
		background: var(--hover-strong);
		color: var(--text);
	}
	svg {
		width: 0.875rem;
		height: 0.875rem;
		fill: none;
		stroke: currentColor;
		stroke-width: 2;
		stroke-linecap: round;
		stroke-linejoin: round;
	}
</style>
