import { base } from '$app/paths';
import type { ManifestEntry, Platform, Manifest, Descriptor, ReferrerInfo, LayerEntry, LayerEntryKind, LayerListing } from './api';

export type AttestationType = 'slsa' | 'sbom' | 'signature' | 'vuln' | 'artifact';

export interface TreeNode {
	manifest: ManifestEntry;
	children: { manifest: ManifestEntry; platform?: Platform }[];
	attestations: {
		digest: string;
		type: AttestationType;
		artifactType?: string;
		annotations?: Record<string, string>;
	}[];
}

export type TreeRowKind = 'root' | 'child' | 'attestation' | 'referrer';

export interface TreeRowNode {
	digest: string;
	kind: TreeRowKind;
	/** Whether a following sibling exists (drives the `has-next` connector). */
	hasNext: boolean;
	/** Tags for the second column (root rows only). */
	tags: string[];
	/** Platform badge for the second column (child rows only). */
	platform?: Platform;
	/** Attestation badge for the second column (attestation/referrer rows). */
	attestationType?: AttestationType;
	/** The referrer's annotations, carrying a scan summary when it is a report. */
	annotations?: Record<string, string>;
	pushed_at?: string;
	last_pulled_at?: string;
	/** Whether pushed/pulled columns show timestamps (true for root/child rows). */
	showDates: boolean;
	/** Whether this node can be expanded to reveal its children. */
	canExpand: boolean;
	children: TreeRowNode[];
}

/**
 * Flattens the {@link TreeNode} structure into a uniform recursive shape that a
 * single recursive `TreeRow` component can render at arbitrary depth.
 */
export function buildTreeRows(tree: TreeNode[]): TreeRowNode[] {
	return tree.map((node) => {
		const children: TreeRowNode[] = [];
		node.children.forEach((child) => {
			const referrers = child.manifest.referrers ?? [];
			children.push({
				digest: child.manifest.digest,
				kind: 'child',
				hasNext: false,
				tags: [],
				platform: child.platform,
				pushed_at: child.manifest.pushed_at,
				last_pulled_at: child.manifest.last_pulled_at,
				showDates: true,
				canExpand: referrers.length > 0,
				children: referrers.map((referrer) => ({
					digest: referrer.digest,
					kind: 'referrer' as const,
					hasNext: false,
					tags: [],
					attestationType: getAttestationType(referrer),
					annotations: referrer.annotations,
					showDates: false,
					canExpand: false,
					children: []
				}))
			});
		});
		node.attestations.forEach((att) => {
			children.push({
				digest: att.digest,
				kind: 'attestation',
				hasNext: false,
				tags: [],
				attestationType: att.type,
				annotations: att.annotations,
				showDates: false,
				canExpand: false,
				children: []
			});
		});
		// Mark connector lines for every sibling except the last.
		children.forEach((child, idx) => {
			child.hasNext = idx !== children.length - 1;
			child.children.forEach((grandchild, gidx) => {
				grandchild.hasNext = gidx !== child.children.length - 1;
			});
		});
		return {
			digest: node.manifest.digest,
			kind: 'root' as const,
			hasNext: false,
			tags: node.manifest.tags,
			pushed_at: node.manifest.pushed_at,
			last_pulled_at: node.manifest.last_pulled_at,
			showDates: true,
			canExpand: children.length > 0,
			children
		};
	});
}

const SLSA_ARTIFACT_TYPES = new Set([
	'application/vnd.in-toto+json',
]);

const SBOM_ARTIFACT_TYPES = new Set([
	'text/spdx',
	'text/spdx+xml',
	'text/spdx+json',
	'application/spdx+json',
	'application/vnd.cyclonedx',
	'application/vnd.cyclonedx+xml',
	'application/vnd.cyclonedx+json',
	'application/vnd.syft+json',
	'application/vnd.goharbor.harbor.sbom.v1',
]);

const SIGNATURE_ARTIFACT_TYPES = new Set([
	'application/vnd.cncf.notary.signature',
	'application/vnd.dev.cosign.artifact.sig.v1+json',
	'application/vnd.dev.cosign.simplesigning.v1+json',
	'application/vnd.dsse.envelope.v1+json',
	'application/vnd.dev.sigstore.bundle.v0.3+json',
]);

const SLSA_PREDICATE_TYPES = new Set([
	'https://slsa.dev/provenance/v0.2',
	'https://slsa.dev/provenance/v1',
]);

const SBOM_PREDICATE_TYPES = new Set([
	'https://spdx.dev/Document',
	'https://cyclonedx.org/bom',
]);

const VULN_ARTIFACT_TYPES = new Set([
	'application/sarif+json',
]);

const VULN_PREDICATE_TYPES = new Set([
	'https://cosign.sigstore.dev/attestation/vuln/v1',
	'https://in-toto.io/attestation/vulns/v0.1',
	'https://in-toto.io/attestation/vulns/v0.2',
]);

export function getAttestationType(referrer: ReferrerInfo): AttestationType {
	const artifactType = referrer.artifactType ?? '';
	const predicateType = referrer.annotations?.['in-toto.io/predicate-type'] ?? '';

	// The predicate names the content; the artifact type may only name the envelope.
	if (SLSA_PREDICATE_TYPES.has(predicateType)) return 'slsa';
	if (SBOM_PREDICATE_TYPES.has(predicateType)) return 'sbom';
	if (VULN_PREDICATE_TYPES.has(predicateType)) return 'vuln';

	if (SLSA_ARTIFACT_TYPES.has(artifactType)) return 'slsa';
	if (SBOM_ARTIFACT_TYPES.has(artifactType)) return 'sbom';
	if (SIGNATURE_ARTIFACT_TYPES.has(artifactType)) return 'signature';
	if (VULN_ARTIFACT_TYPES.has(artifactType)) return 'vuln';

	return 'artifact';
}

/**
 * A browse URL is the full registry path, with no marker separating the
 * repository from the namespace under it: a repository name may contain
 * slashes, so only the configured names can tell them apart.
 */
export function pathUrl(path: string): string {
	return `${base}/${path}`;
}

export function manifestUrl(path: string, reference: string): string {
	const separator = reference.startsWith('sha256:') || reference.startsWith('sha512:') ? '@' : ':';
	return `${base}/${path}${separator}${reference}`;
}

/** The page rendering the vulnerability report stored at `digest`. */
export function digestConfirmKey(digest: string): string {
	return `digest:${digest}`;
}

export function tagConfirmKey(tag: string): string {
	return `tag:${tag}`;
}

export function uploadConfirmKey(uuid: string): string {
	return `upload:${uuid}`;
}

export const selectedUploadsConfirmKey = 'uploads:selected';

const WELL_KNOWN_ANNOTATIONS: Record<string, string> = {
	'org.opencontainers.image.created': 'created',
	'org.opencontainers.image.authors': 'authors',
	'org.opencontainers.image.url': 'url',
	'org.opencontainers.image.documentation': 'documentation',
	'org.opencontainers.image.source': 'source',
	'org.opencontainers.image.version': 'version',
	'org.opencontainers.image.revision': 'revision',
	'org.opencontainers.image.vendor': 'vendor',
	'org.opencontainers.image.licenses': 'licenses',
	'org.opencontainers.image.title': 'title',
	'org.opencontainers.image.description': 'description',
	'org.opencontainers.image.base.digest': 'base_digest',
	'org.opencontainers.image.base.name': 'base_name',
};

export function formatSize(bytes: number): string {
	const units = ['B', 'KB', 'MB', 'GB'];
	let i = 0;
	let size = bytes;
	while (size >= 1024 && i < units.length - 1) {
		size /= 1024;
		i++;
	}
	return `${size.toFixed(1)} ${units[i]}`;
}

export function formatPlatform(platform?: Platform): string {
	if (!platform) return '';
	let result = `${platform.os}/${platform.architecture}`;
	if (platform.variant) {
		result += `/${platform.variant}`;
	}
	return result;
}

export function formatTimeAgo(dateString: string): string {
	const date = new Date(dateString);
	const now = new Date();
	const seconds = Math.floor((now.getTime() - date.getTime()) / 1000);

	if (seconds < 60) return `${seconds}s ago`;
	const minutes = Math.floor(seconds / 60);
	if (minutes < 60) return `${minutes}m ago`;
	const hours = Math.floor(minutes / 60);
	if (hours < 24) return `${hours}h ago`;
	const days = Math.floor(hours / 24);
	return `${days}d ago`;
}

/**
 * A retention window in words, for stating how far back a listing reaches.
 * Truncates to the largest whole unit, which is what a configured window is
 * set in.
 */
export function formatRetention(seconds: number): string {
	const units: [number, string][] = [[86400, 'day'], [3600, 'hour'], [60, 'minute'], [1, 'second']];
	for (const [size, name] of units) {
		if (seconds >= size) {
			const value = Math.floor(seconds / size);
			return `${value} ${name}${value === 1 ? '' : 's'}`;
		}
	}
	return `${seconds} seconds`;
}

export function displayNamespace(namespace: string, repository: string): string {
	const prefix = repository + '/';
	if (namespace.startsWith(prefix)) {
		return namespace.slice(prefix.length);
	}
	return namespace;
}

export function buildTree(manifests: ManifestEntry[]): TreeNode[] {
	const childDigests = new Set<string>();
	const referrerDigests = new Set<string>();
	const parentToChildren = new Map<string, { manifest: ManifestEntry; platform?: Platform }[]>();
	const manifestToAttestations = new Map<string, TreeNode['attestations']>();

	for (const m of manifests) {
		// Skip a self-reference: a manifest that lists itself as a parent or
		// referrer must not exclude itself from the roots, or it renders nowhere.
		const parents = (m.parents ?? []).filter((parent) => parent.digest !== m.digest);
		if (parents.length > 0) {
			childDigests.add(m.digest);
			for (const parent of parents) {
				const children = parentToChildren.get(parent.digest) ?? [];
				children.push({ manifest: m, platform: parent.platform });
				parentToChildren.set(parent.digest, children);
			}
		}

		const referrers = (m.referrers ?? []).filter((referrer) => referrer.digest !== m.digest);
		if (referrers.length > 0) {
			const attestations: TreeNode['attestations'] = [];
			for (const referrer of referrers) {
				referrerDigests.add(referrer.digest);
				attestations.push({
					digest: referrer.digest,
					type: getAttestationType(referrer),
					artifactType: referrer.artifactType,
					annotations: referrer.annotations,
				});
			}
			manifestToAttestations.set(m.digest, attestations);
		}
	}

	const roots: TreeNode[] = [];
	for (const m of manifests) {
		if (!childDigests.has(m.digest) && !referrerDigests.has(m.digest)) {
			const children = parentToChildren.get(m.digest) ?? [];
			children.sort((a, b) => {
				const pa = formatPlatform(a.platform);
				const pb = formatPlatform(b.platform);
				return pa.localeCompare(pb);
			});
			const attestations = manifestToAttestations.get(m.digest) ?? [];
			roots.push({ manifest: m, children, attestations });
		}
	}

	roots.sort((a, b) => {
		if (a.manifest.tags.length > 0 && b.manifest.tags.length === 0) return -1;
		if (a.manifest.tags.length === 0 && b.manifest.tags.length > 0) return 1;
		return 0;
	});

	return roots;
}

export function isInteractiveTarget(event: MouseEvent): boolean {
	const target = event.target as HTMLElement;
	return (
		target.tagName === 'BUTTON' ||
		!!target.closest('button') ||
		target.tagName === 'A' ||
		!!target.closest('a')
	);
}

export function getTagConfirm(deleteConfirm: string | null): string | null {
	if (deleteConfirm?.startsWith('tag:')) {
		return deleteConfirm.slice(4);
	}
	return null;
}

export function getAnnotationLabel(key: string): string {
	return WELL_KNOWN_ANNOTATIONS[key] ?? key;
}

export function isUrl(value: string): boolean {
	return value.startsWith('http://') || value.startsWith('https://');
}

export function isOrasArtifact(m: Manifest): boolean {
	if (m.artifactType) return true;
	if (m.config?.mediaType === 'application/vnd.oci.empty.v1+json') return true;
	return m.layers?.some(l => l.annotations?.['org.opencontainers.image.title']) ?? false;
}

export function getFileName(layer: Descriptor): string | null {
	return layer.annotations?.['org.opencontainers.image.title'] ?? null;
}

/**
 * The namespaces sitting under `namespace`, named relative to it.
 *
 * A registry name such as `repo/team/app` creates no object at `repo/team`, so
 * an intermediate level holds no manifests of its own and would otherwise be a
 * dead end with nothing to click through to.
 */
export interface NamespaceDescendant {
	path: string;
	label: string;
	tag_count?: number;
	manifest_count?: number;
	upload_count?: number;
}

export function descendantNamespaces(
	entries: { name: string; tag_count?: number; manifest_count?: number; upload_count?: number }[],
	namespace: string
): NamespaceDescendant[] {
	const prefix = `${namespace}/`;
	return entries
		.filter((entry) => entry.name.startsWith(prefix))
		.map((entry) => ({
			path: entry.name,
			label: entry.name.slice(prefix.length),
			tag_count: entry.tag_count,
			manifest_count: entry.manifest_count,
			upload_count: entry.upload_count
		}))
		.sort((a, b) => a.label.localeCompare(b.label));
}

/**
 * The repository owning `path`, which is the longest configured name that is
 * `path` itself or a prefix of it. A repository name may contain slashes, so a
 * browse path cannot be split into repository and namespace without this.
 * `null` when the path lies outside every repository, as an intermediate
 * segment does.
 */
export function resolveRepository(names: string[], path: string): string | null {
	let owner: string | null = null;
	for (const name of names) {
		if (path !== name && !path.startsWith(`${name}/`)) {
			continue;
		}
		if (owner === null || name.length > owner.length) {
			owner = name;
		}
	}
	return owner;
}

// ---- Layer filesystems ----

/** A tar stream the indexer can walk: plain or gzipped, not zstd. */
export function isFilesystemLayer(mediaType: string): boolean {
	return mediaType.includes('.tar') && !mediaType.endsWith('+zstd') && !mediaType.endsWith('.zst');
}

export interface FsNode {
	name: string;
	path: string;
	kind: LayerEntryKind;
	/** Absent for a directory no layer listed but a path implies. */
	entry: LayerEntry | null;
	/** Index of the layer that last set this node. */
	layer: number;
	children: Map<string, FsNode>;
}

export interface FsDeletion {
	path: string;
	/** Index of the layer whose whiteout removed it. */
	layer: number;
}

export interface FsTree {
	root: FsNode;
	deletions: FsDeletion[];
}

function fsDir(name: string, path: string, layer: number): FsNode {
	return { name, path, kind: 'dir', entry: null, layer, children: new Map() };
}

/**
 * Applies the layers in order the way a runtime does: an entry replaces the
 * lower layers' one, a whiteout removes a path and everything under it, an
 * opaque marker empties a directory of what the lower layers put there.
 */
export function mergeLayers(listings: LayerListing[]): FsTree {
	const root = fsDir('', '', -1);
	const deletions: FsDeletion[] = [];
	const parentOf = (path: string, layer: number): FsNode => {
		let node = root;
		const parts = path.split('/');
		for (const part of parts.slice(0, -1)) {
			let child = node.children.get(part);
			if (!child) {
				child = fsDir(part, node.path ? `${node.path}/${part}` : part, layer);
				node.children.set(part, child);
			}
			node = child;
		}
		return node;
	};
	listings.forEach((listing, layer) => {
		for (const entry of listing.entries) {
			const name = entry.path.split('/').pop() ?? entry.path;
			if (entry.kind === 'opaque') {
				const dir = parentOf(`${entry.path}/x`, layer);
				for (const child of dir.children.values()) {
					deletions.push({ path: child.path, layer });
				}
				dir.children.clear();
				continue;
			}
			const parent = parentOf(entry.path, layer);
			if (entry.kind === 'whiteout') {
				if (parent.children.delete(name)) deletions.push({ path: entry.path, layer });
				continue;
			}
			const existing = parent.children.get(name);
			parent.children.set(name, {
				name,
				path: entry.path,
				kind: entry.kind,
				entry,
				layer,
				// A directory listed again keeps what lower layers put in it.
				children: entry.kind === 'dir' && existing?.kind === 'dir' ? existing.children : new Map()
			});
		}
	});
	return { root, deletions };
}

/** `0o755` as `rwxr-xr-x`. */
export function formatMode(mode: number): string {
	const bits = 'rwxrwxrwx';
	return bits
		.split('')
		.map((bit, i) => ((mode >> (8 - i)) & 1 ? bit : '-'))
		.join('');
}

/** The children of a node, directories first, each group by name. */
export function sortedChildren(node: FsNode): FsNode[] {
	return [...node.children.values()].sort((a, b) => {
		const dirs = Number(b.kind === 'dir') - Number(a.kind === 'dir');
		return dirs || a.name.localeCompare(b.name);
	});
}

/** What a tree is narrowed to: a set of layers to focus, and a path fragment. */
export interface FsMatcher {
	layers: Set<number>;
	text: string;
}

export function fsMatches(matcher: FsMatcher, node: FsNode): boolean {
	return (
		(matcher.layers.size === 0 || matcher.layers.has(node.layer)) &&
		(matcher.text === '' || node.path.toLowerCase().includes(matcher.text))
	);
}

/** A node stays in a narrowed tree when it or anything under it matches. */
export function fsVisible(matcher: FsMatcher, node: FsNode): boolean {
	return fsMatches(matcher, node) || [...node.children.values()].some((c) => fsVisible(matcher, c));
}

/** The parent folder's path, `''` at the top. */
export function fsParent(path: string): string {
	return path.slice(0, Math.max(path.lastIndexOf('/'), 0));
}

/**
 * Where a symlink leads, through further links on the way, or null when it
 * leaves the image or loops. Anything but a symlink is its own target.
 */
export function fsResolve(root: FsNode, node: FsNode, hops = 16): FsNode | null {
	if (node.kind !== 'symlink' || !node.entry?.link) return node;
	if (hops === 0) return null;
	const link = node.entry.link;
	const parts = [...(link.startsWith('/') ? [] : fsParent(node.path).split('/')), ...link.split('/')];
	let current: FsNode | null = root;
	for (const part of parts) {
		if (!current || part === '' || part === '.') continue;
		if (part === '..') {
			current = fsNodeAt(root, fsParent(current.path));
			continue;
		}
		const child: FsNode | undefined = current.children.get(part);
		current = child ? fsResolve(root, child, hops - 1) : null;
	}
	return current;
}

/** The node at `path` under `root`, or the root when nothing is there. */
export function fsNodeAt(root: FsNode, path: string): FsNode {
	let node = root;
	for (const name of path.split('/').filter(Boolean)) {
		const child = node.children.get(name);
		if (!child) return root;
		node = child;
	}
	return node;
}

// ---- Vulnerability reports ----

export type Severity = 'critical' | 'high' | 'medium' | 'low' | 'unknown';
export const SEVERITIES: Severity[] = ['critical', 'high', 'medium', 'low', 'unknown'];

export interface ScanSummary {
	scanner?: string;
	counts: Record<Severity, number>;
	total: number;
}

/** The `io.angos.scan.*` annotations the scan handler writes on a report. */
export function parseScanSummary(annotations?: Record<string, string>): ScanSummary | null {
	if (!annotations) return null;
	const counts = {} as Record<Severity, number>;
	let present = false;
	for (const severity of SEVERITIES) {
		const value = annotations[`io.angos.scan.${severity}`];
		if (value !== undefined) present = true;
		counts[severity] = Number(value ?? 0) || 0;
	}
	if (!present) return null;
	const total = SEVERITIES.reduce((sum, severity) => sum + counts[severity], 0);
	return { scanner: annotations['io.angos.scan.scanner'], counts, total };
}

export interface Finding {
	id: string;
	severity: Severity;
	pkg?: string;
	installed?: string;
	fixed?: string;
	description: string;
	url?: string;
}

export interface ParsedReport {
	findings: Finding[];
	summary: ScanSummary;
}

type SarifRule = {
	id?: string;
	shortDescription?: { text?: string };
	fullDescription?: { text?: string };
	helpUri?: string;
	properties?: { 'security-severity'?: string; tags?: string[] };
};
type SarifResult = {
	ruleId?: string;
	ruleIndex?: number;
	message?: { text?: string };
};

const SEVERITY_WORDS: Record<string, Severity> = {
	critical: 'critical',
	high: 'high',
	medium: 'medium',
	moderate: 'medium',
	low: 'low',
	negligible: 'low'
};

function severityWord(word: string): Severity | null {
	return SEVERITY_WORDS[word.toLowerCase()] ?? null;
}

// The scanner's own severity word when it states one (Trivy tags its rule,
// Grype writes it into the message), else the CVSS score bucketed the way
// GitHub code scanning does. Mirrors the registry's summary.
function severityOf(rule: SarifRule | undefined, message: string): Severity {
	for (const tag of rule?.properties?.tags ?? []) {
		const word = severityWord(tag);
		if (word) return word;
	}
	const stated = message.match(/(?:Severity:|\bAn?)\s+(\w+)/);
	if (stated) {
		const word = severityWord(stated[1]);
		if (word) return word;
	}
	const score = Number(rule?.properties?.['security-severity']);
	if (score >= 9) return 'critical';
	if (score >= 7) return 'high';
	if (score >= 4) return 'medium';
	if (score > 0) return 'low';
	return 'unknown';
}

function field(message: string, label: string): string | undefined {
	return message.match(new RegExp(`${label}:\\s*([^\\n]*)`))?.[1]?.trim() || undefined;
}

/** A SARIF document from Trivy or Grype as a flat, sorted list of findings. */
export function parseSarif(document: unknown): ParsedReport {
	const run = (document as { runs?: unknown[] })?.runs?.[0] as
		| { tool?: { driver?: { name?: string; version?: string; rules?: SarifRule[] } }; results?: SarifResult[] }
		| undefined;
	const driver = run?.tool?.driver;
	const scanner = driver?.name ? `${driver.name}${driver.version ? ' ' + driver.version : ''}` : undefined;
	const rules = driver?.rules ?? [];
	const counts: Record<Severity, number> = { critical: 0, high: 0, medium: 0, low: 0, unknown: 0 };
	const findings: Finding[] = [];
	for (const result of run?.results ?? []) {
		const rule =
			(result.ruleIndex !== undefined ? rules[result.ruleIndex] : undefined) ??
			rules.find((candidate) => candidate.id === result.ruleId);
		const message = result.message?.text ?? '';
		const severity = severityOf(rule, message);
		counts[severity] += 1;
		// Trivy lists "Package:", "Installed Version:" and "Fixed Version:";
		// Grype says "in <kind> package: <name>, version <v>".
		const grype = message.match(/package:\s*([^,\s]+),\s*version\s+(\S+)/i);
		findings.push({
			id: result.ruleId ?? rule?.id ?? '?',
			severity,
			pkg: field(message, 'Package') ?? grype?.[1],
			installed: field(message, 'Installed Version') ?? grype?.[2],
			fixed: field(message, 'Fixed Version'),
			description:
				rule?.shortDescription?.text ?? rule?.fullDescription?.text ?? message.split('\n')[0],
			url: rule?.helpUri
		});
	}
	const rank = (severity: Severity) => SEVERITIES.indexOf(severity);
	findings.sort((a, b) => rank(a.severity) - rank(b.severity) || a.id.localeCompare(b.id));
	const total = findings.length;
	return { findings, summary: { scanner, counts, total } };
}

/** The newest vulnerability report among `referrers` that carries a summary. */
export function latestScanReport(referrers: ReferrerInfo[]): ReferrerInfo | null {
	const reports = referrers.filter(
		(referrer) => getAttestationType(referrer) === 'vuln' && parseScanSummary(referrer.annotations)
	);
	reports.sort((a, b) =>
		(b.annotations?.['org.opencontainers.image.created'] ?? '').localeCompare(
			a.annotations?.['org.opencontainers.image.created'] ?? ''
		)
	);
	return reports[0] ?? null;
}
