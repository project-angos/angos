<script lang="ts">
	interface Props {
		tags: string[];
		/** The page's confirm key; a tag's is `tag:<name>`. */
		deleteConfirm?: string | null;
		disabled?: boolean;
		ondelete?: (tag: string) => void;
		onconfirmchange?: (value: string | null) => void;
		getHref?: (tag: string) => string;
	}

	let {
		tags,
		deleteConfirm = null,
		disabled = false,
		ondelete,
		onconfirmchange,
		getHref
	}: Props = $props();
</script>

{#each tags as tag}
	{#if ondelete}
		<span class="tag-with-delete">
			<a class="tag" href={getHref?.(tag)}>{tag}</a>
			{#if deleteConfirm === `tag:${tag}`}
				<button class="tag-delete danger" onclick={() => ondelete?.(tag)} {disabled} title="Confirm delete tag">✓</button>
				<button class="tag-delete" onclick={() => onconfirmchange?.(null)} title="Cancel">✗</button>
			{:else}
				<button class="tag-delete" onclick={() => onconfirmchange?.(`tag:${tag}`)} title="Delete tag {tag}">×</button>
			{/if}
		</span>
	{:else}
		<span class="tag-standalone">{tag}</span>
	{/if}
{:else}
	<span class="untagged">untagged</span>
{/each}
