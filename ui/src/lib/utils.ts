import { base } from '$app/paths';
import type { ManifestEntry, Platform, Manifest, Descriptor, ReferrerInfo } from './api';

export type AttestationType = 'slsa' | 'sbom' | 'signature' | 'artifact';

export interface TreeNode {
	manifest: ManifestEntry;
	children: { manifest: ManifestEntry; platform?: Platform }[];
	attestations: { digest: string; type: AttestationType; artifactType?: string }[];
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

export function getAttestationType(referrer: ReferrerInfo): AttestationType {
	const artifactType = referrer.artifactType ?? '';
	const predicateType = referrer.annotations?.['in-toto.io/predicate-type'] ?? '';

	if (SLSA_ARTIFACT_TYPES.has(artifactType)) return 'slsa';
	if (SBOM_ARTIFACT_TYPES.has(artifactType)) return 'sbom';
	if (SIGNATURE_ARTIFACT_TYPES.has(artifactType)) return 'signature';

	if (SLSA_PREDICATE_TYPES.has(predicateType)) return 'slsa';
	if (SBOM_PREDICATE_TYPES.has(predicateType)) return 'sbom';

	return 'artifact';
}

export function repoUrl(name: string): string {
	return `${base}/${name}`;
}

export function namespaceUrl(repo: string, namespace: string): string {
	return `${base}/${repo}/${namespace}`;
}

export function manifestUrl(repo: string, namespace: string, reference: string): string {
	const separator = reference.startsWith('sha256:') || reference.startsWith('sha512:') ? '@' : ':';
	return `${base}/${repo}/${namespace}${separator}${reference}`;
}

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
	const manifestToAttestations = new Map<string, { digest: string; type: AttestationType; artifactType?: string }[]>();

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
			const attestations: { digest: string; type: AttestationType; artifactType?: string }[] = [];
			for (const referrer of referrers) {
				referrerDigests.add(referrer.digest);
				attestations.push({
					digest: referrer.digest,
					type: getAttestationType(referrer),
					artifactType: referrer.artifactType,
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
