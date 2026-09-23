<script lang="ts">
	import type { LayerEntry, LayerFileDetails } from '$lib/api';
	import { authedFetch, fetchLayerFileDetails, layerFileUrl } from '$lib/api';
	import { highlight } from '$lib/highlight';
	import { SECRET_LABELS, formatMode, formatSize, secretKinds, type FsNode } from '$lib/utils';
	import CopyButton from './CopyButton.svelte';
	import ErrorState from './ErrorState.svelte';
	import LoadingState from './LoadingState.svelte';

	interface Props {
		namespace: string;
		/** Every layer's digest; the bytes are read from the node's. */
		digests: string[];
		/** A file or hard link, or a symlink that leads out of the image. */
		node: FsNode;
		/** The entry holding the bytes: a hard link's target. */
		bytes: LayerEntry | null;
		/** The layers holding a version of the path, and the one the image shows, if any. */
		versions: number[];
		live: number | null;
		layerlabel: (layer: number) => string;
		onversion: (layer: number) => void;
		onclose: () => void;
	}

	let { namespace, digests, node, bytes, versions, live, layerlabel, onversion, onclose }: Props = $props();

	/** What the pane reads of a file; a larger one shows truncated. */
	const PREVIEW_LIMIT = 512 * 1024;
	/** An image past this shows as a binary file, to download. */
	const IMAGE_LIMIT = 16 * 1024 * 1024;
	const ELF_TYPE = 'application/x-executable';
	/** What the registry decodes: an ELF binary, or a PEM file's certificates. */
	const DETAILED_TYPES = [ELF_TYPE, 'application/x-pem-file', 'application/x-x509-ca-cert'];
	/** What an `<img>` shows; an SVG's scripts do not run there. */
	const IMAGE_TYPES = [
		'image/avif',
		'image/bmp',
		'image/gif',
		'image/jpeg',
		'image/png',
		'image/svg+xml',
		'image/webp',
		'image/x-icon'
	];
	const VIEWS = { preview: 'Preview', source: 'Source', diff: 'Diff' };
	type View = keyof typeof VIEWS;

	// Raw HTML stays text and script links are refused by default; images are
	// off so a README cannot fetch from anywhere it likes.
	async function markdown(text: string): Promise<string> {
		const { default: MarkdownIt } = await import('markdown-it');
		const md = new MarkdownIt({ highlight: (code, language) => highlight(code, language) ?? '' });
		return md.disable('image').render(text);
	}

	// Streams a file and stops past `limit`, so a large one costs no more than
	// the part used.
	async function head(url: string, limit: number, signal?: AbortSignal) {
		const response = await authedFetch(url, { signal });
		if (!response.ok || !response.body) throw new Error(`HTTP ${response.status}`);
		const reader = response.body.getReader();
		const chunks: Uint8Array[] = [];
		let length = 0;
		while (length <= limit) {
			const { done, value } = await reader.read();
			if (done) break;
			chunks.push(value);
			length += value.length;
		}
		reader.cancel();
		const bytes = new Uint8Array(Math.min(length, limit));
		let at = 0;
		for (const chunk of chunks) {
			if (at >= bytes.length) break;
			bytes.set(chunk.subarray(0, bytes.length - at), at);
			at += chunk.length;
		}
		return { bytes, length };
	}

	const isText = (bytes: Uint8Array) => !bytes.subarray(0, 8192).includes(0);

	let text = $state<string | null>(null);
	/** An image's bytes as an object URL, for the `<img>` to show. */
	let picture = $state<string | null>(null);
	let details = $state<LayerFileDetails | null>(null);
	let binary = $state(false);
	let truncated = $state(false);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let view = $state<View>('preview');
	let code = $state<HTMLElement>();

	const digest = $derived(digests[node.layer]);
	const content = $derived(bytes?.content);
	const mimeType = $derived(content?.mime_type ?? '');
	const size = $derived(bytes?.size ?? node.entry?.size ?? 0);
	const highlighted = $derived(text !== null && content ? highlight(text, mimeType) : null);
	const gutter = $derived(text?.replace(/\n$/, '').split('\n').map((_, i) => i + 1).join('\n') ?? '');
	const elf = $derived(details?.elf);
	const certificates = $derived(details?.certificates ?? []);
	const secretLines = $derived(content?.secrets?.map((secret) => secret.line) ?? []);
	/** How the file reads best when not as its text. */
	const rich = $derived(
		picture
			? 'image'
			: elf
				? 'elf'
				: mimeType === 'text/markdown' && text !== null
				? 'markdown'
				: certificates.some((block) => block.certificate)
					? 'certificates'
					: null
	);
	/** The version a diff compares with: the next one down, or up from the lowest. */
	const other = $derived(
		versions.filter((layer) => layer < node.layer).at(-1) ?? versions.find((layer) => layer > node.layer) ?? null
	);
	const views = $derived(
		(Object.keys(VIEWS) as View[]).filter((option) =>
			option === 'preview' ? rich !== null : text !== null && (option === 'source' || other !== null)
		)
	);
	const shown = $derived(views.includes(view) ? view : views[0]);
	const modified = $derived(node.entry ? new Date(node.entry.mtime * 1000).toISOString().slice(0, 10) : '');

	$effect(() => {
		if (node.kind === 'symlink') return;
		const controller = new AbortController();
		read(layerFileUrl(namespace, digest, node.path), controller.signal);
		return () => {
			controller.abort();
			if (picture) URL.revokeObjectURL(picture);
		};
	});

	// The first secret's line scrolls to the middle of the view.
	$effect(() => {
		const mark = secretLines.length > 0 && text !== null ? code?.querySelector<HTMLElement>('.fs-text .fs-mark') : null;
		if (mark && code) code.scrollTop = mark.offsetTop - code.clientHeight / 2;
	});

	async function read(url: string, signal: AbortSignal) {
		// A secret shows in the text, where its line is marked.
		if (content?.secrets?.length) view = 'source';
		text = null;
		picture = null;
		details = null;
		binary = false;
		truncated = false;
		error = null;
		loading = true;
		const image = IMAGE_TYPES.includes(mimeType);
		try {
			// A file listed before its type was known may be one the registry decodes.
			// An ELF binary shows what it decodes, never its bytes.
			const [found, file] = await Promise.all([
				!content || DETAILED_TYPES.includes(mimeType) ? fetchLayerFileDetails(namespace, digest, node.path) : null,
				mimeType === ELF_TYPE ? null : head(url, image ? IMAGE_LIMIT : PREVIEW_LIMIT, signal)
			]);
			// Another file opened while this one finished: its state is not ours to set.
			if (signal.aborted) return;
			details = found?.data ?? null;
			if (!file) {
				binary = true;
				return;
			}
			const { bytes, length } = file;
			if (image && length <= IMAGE_LIMIT) picture = URL.createObjectURL(new Blob([bytes], { type: mimeType }));
			truncated = length > PREVIEW_LIMIT;
			binary = !isText(bytes);
			text = binary ? null : new TextDecoder().decode(bytes.subarray(0, PREVIEW_LIMIT));
		} catch (e) {
			if (!signal.aborted) error = e instanceof Error ? e.message : 'Request failed';
		} finally {
			if (!signal.aborted) loading = false;
		}
	}

	/** The line changes from the older of the two versions to the newer; null when the other is binary. */
	async function changes(text: string, other: number) {
		const { bytes, length } = await head(layerFileUrl(namespace, digests[other], node.path), PREVIEW_LIMIT);
		if (!isText(bytes)) return null;
		const { structuredPatch } = await import('diff');
		const theirs = new TextDecoder().decode(bytes);
		const [before, after] = other < node.layer ? [theirs, text] : [text, theirs];
		const patch = structuredPatch('', '', before, after, '', '', { context: 3 });
		return { hunks: patch.hunks, cut: truncated || length > PREVIEW_LIMIT };
	}
</script>

<aside class="fs-preview" aria-label="File">
	<div class="fs-preview-head">
		<div class="fs-preview-title">
			<span class="fs-preview-path" title={node.path}>
				<span class="fs-preview-dir">{node.path.slice(0, node.path.length - node.name.length)}</span>
				<span class="fs-preview-name">{node.name}</span>
			</span>
			{#if views.length > 1}
				<div class="view-toggle" role="group" aria-label="Show">
					{#each views as option}
						<button type="button" class:active={shown === option} onclick={() => (view = option)}>
							{VIEWS[option]}
						</button>
					{/each}
				</div>
			{/if}
			{#if node.kind !== 'symlink'}
				<a class="btn secondary" href={layerFileUrl(namespace, digest, node.path, true)} download={node.name}>
					Download
				</a>
			{/if}
			<button type="button" class="fs-close" onclick={onclose} aria-label="Close" title="Close (Esc)">
				<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18" /></svg>
			</button>
		</div>
		<div class="fs-preview-meta">
			{#if versions.length > 1}
				<span class="fs-versions" role="group" aria-label="Versions">
					{#each versions as layer}
						<button
							type="button"
							class="badge fs-version"
							class:active={layer === node.layer}
							title={layer === live ? 'The version in the image' : 'Overwritten by a later layer'}
							onclick={() => onversion(layer)}>{layerlabel(layer)}</button
						>
					{/each}
				</span>
			{:else}
				<span class="badge">{layerlabel(node.layer)}</span>
			{/if}
			{#if node.kind !== 'symlink' && live !== node.layer}
				<span class="badge waste-unchanged">{live === null ? 'removed from the image' : 'overwritten'}</span>
			{/if}
			{#if content}<span>{content.mime_type}</span>{/if}
			{#if node.kind !== 'symlink'}<span>{formatSize(size)}</span>{/if}
			{#if node.entry}
				<span class="mono">{formatMode(node.entry.mode)}</span>
				{#if node.entry.capabilities?.length}
					<span class="mono" title="Linux capabilities">{node.entry.capabilities.join(',')}</span>
				{/if}
				<span>{node.entry.uid}:{node.entry.gid}</span>
				<span>{modified}</span>
			{/if}
			{#each secretKinds(content?.secrets ?? []) as [kind, lines]}
				<span class="severity severity-critical secret">
					{SECRET_LABELS[kind]}, line{lines.length > 1 ? 's' : ''} {lines.join(', ')}
				</span>
			{/each}
		</div>
		{#if content}
			<div class="fs-preview-digests">
				<span>
					SHA-256 <code title={content.sha256}>{content.sha256.slice(0, 12)}…</code>
					<CopyButton text={content.sha256} label="Copy SHA-256" />
				</span>
				<span>
					SHA-512 <code title={content.sha512}>{content.sha512.slice(0, 12)}…</code>
					<CopyButton text={content.sha512} label="Copy SHA-512" />
				</span>
			</div>
		{/if}
	</div>
	<div class="fs-preview-body">
		{#if node.kind === 'symlink'}
			<p class="muted">Points to {node.entry?.link}, which is not in the image.</p>
		{:else if error}
			<ErrorState message="Could not read the file ({error})." />
		{:else if loading}
			<LoadingState message="Reading the file" />
		{:else if shown === 'preview' && rich === 'image'}
			<div class="fs-rich fs-image"><img src={picture} alt={node.name} /></div>
		{:else if shown === 'preview' && rich === 'elf' && elf}
			<div class="fs-rich">
				<table>
					<tbody>
						<tr>
							<td class="label">Type</td>
							<td>{elf.type}</td>
						</tr>
						<tr>
							<td class="label">Architecture</td>
							<td>{elf.machine}, {elf.bits}-bit {elf.endian}-endian</td>
						</tr>
						<tr>
							<td class="label">Linking</td>
							<td>
								{#if elf.interpreter}
									dynamic, loaded by <code>{elf.interpreter}</code>
								{:else}
									{elf.dynamic ? 'dynamic' : 'static'}
								{/if}
							</td>
						</tr>
						{#if elf.soname}
							<tr>
								<td class="label">Soname</td>
								<td><code>{elf.soname}</code></td>
							</tr>
						{/if}
						<tr>
							<td class="label">Needs</td>
							{#if elf.dynamic}
								<td class="fs-libraries">
									{#each elf.needed as library, i}{i > 0 ? ', ' : ''}<code>{library}</code>{:else}
										<span class="muted">nothing</span>
									{/each}
								</td>
							{:else}
								<td class="muted">nothing: it is statically linked</td>
							{/if}
						</tr>
						<tr>
							<td class="label">Hardening</td>
							<td>
								{#if elf.relro === 'none'}
									<span class="severity severity-medium">no RELRO</span>
								{:else}
									{elf.relro} RELRO
								{/if}
								·
								{#if elf.executable_stack}
									<span class="severity severity-high">executable stack</span>
								{:else}
									non-executable stack
								{/if}
							</td>
						</tr>
						<tr>
							<td class="label">Entry point</td>
							<td><code>{elf.entry}</code></td>
						</tr>
						{#if elf.build_id}
							<tr>
								<td class="label">Build ID</td>
								<td><code>{elf.build_id}</code></td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{:else if shown === 'preview' && rich === 'markdown' && text !== null}
			<div class="fs-rich">
				{#await markdown(text) then html}
					<div class="fs-markdown">{@html html}</div>
				{/await}
			</div>
		{:else if shown === 'preview' && rich === 'certificates'}
			<div class="fs-rich">
				<table>
					<thead>
						<tr>
							<th>Subject</th>
							<th>Issuer</th>
							<th class="col-medium">Valid until</th>
						</tr>
					</thead>
					<tbody>
						{#each certificates as block}
							<tr>
								{#if block.certificate}
									<td>
										{block.certificate.subject}
										{#if block.certificate.names.length > 0}
											<div class="muted">{block.certificate.names.join(', ')}</div>
										{/if}
									</td>
									<td>{block.certificate.issuer}</td>
									<td class="nowrap">
										{block.certificate.not_after.slice(0, 10)}
										{#if new Date(block.certificate.not_after) < new Date()}
											<span class="severity severity-critical">expired</span>
										{/if}
									</td>
								{:else}
									<td class="muted" colspan="3">{block.label.toLowerCase()}</td>
								{/if}
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else if shown === 'diff' && text !== null && other !== null}
			<div class="fs-rich">
				{#await changes(text, other)}
					<LoadingState message="Comparing the versions" />
				{:then diff}
					{@const [older, newer] = other < node.layer ? [other, node.layer] : [node.layer, other]}
					{#if !diff}
						<p class="muted">The version in {layerlabel(other)} is binary and does not compare.</p>
					{:else if diff.hunks.length === 0}
						<p class="muted">The same text in {layerlabel(older)} and {layerlabel(newer)}.</p>
					{:else}
						<div class="fs-diff">
							<div class="fs-hunk">
								{layerlabel(older)} → {layerlabel(newer)}{#if diff.cut}, their first {formatSize(PREVIEW_LIMIT)}{/if}
							</div>
							{#each diff.hunks as hunk}
								<div class="fs-hunk">@@ -{hunk.oldStart},{hunk.oldLines} +{hunk.newStart},{hunk.newLines} @@</div>
								{#each hunk.lines as line}
									<div class:fs-add={line.startsWith('+')} class:fs-del={line.startsWith('-')}>{line}</div>
								{/each}
							{/each}
						</div>
					{/if}
				{:catch}
					<ErrorState message="Could not read the other version." />
				{/await}
			</div>
		{:else if binary}
			<p class="muted">Binary file, {formatSize(size)}.</p>
		{:else if text !== null}
			{#snippet marks()}{#each secretLines as line}<span class="fs-mark" style:top="calc(0.875rem + {line - 1}lh)"
					></span>{/each}{/snippet}
			<div class="fs-code" bind:this={code}>
				<pre class="fs-gutter" aria-hidden="true">{@render marks()}{gutter}</pre>
				{#if highlighted}
					<pre class="fs-text">{@render marks()}{@html highlighted}</pre>
				{:else}
					<pre class="fs-text">{@render marks()}{text}</pre>
				{/if}
			</div>
			{#if truncated}
				<p class="fs-truncated muted">
					The first {formatSize(PREVIEW_LIMIT)} of {formatSize(size)}; download the file for the rest.
				</p>
			{/if}
		{/if}
	</div>
</aside>
