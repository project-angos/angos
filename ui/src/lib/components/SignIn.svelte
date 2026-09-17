<script lang="ts">
	import { signIn, signOut, signedInAs, signInAvailable } from '$lib/auth.svelte';

	// Both track module state, so the control follows a sign-in that finishes
	// after it mounted and a configuration that loads after it.
	const available = $derived(signInAvailable());
	const subject = $derived(signedInAs());
</script>

{#if available}
	<div class="sign-in">
		{#if subject}
			<span class="who" title={subject}>{subject}</span>
			<button onclick={signOut}>Sign out</button>
		{:else}
			<button onclick={signIn}>Sign in</button>
		{/if}
	</div>
{/if}

<style>
	.sign-in {
		display: inline-flex;
		align-items: center;
		gap: 0.5rem;
		flex: none;
	}
	.who {
		max-width: 10rem;
		color: var(--muted);
		font-size: 0.8125rem;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}
	button {
		padding: 0.3rem 0.5rem;
		border-radius: var(--radius);
		background: var(--surface);
		box-shadow: var(--shadow-ring);
		color: var(--muted);
		font-size: 0.8125rem;
		font-weight: 500;
		white-space: nowrap;
	}
	button:hover {
		background: var(--hover);
		color: var(--text);
	}
</style>
