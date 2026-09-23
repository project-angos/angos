// The layer merge's accounting of wasted bytes and identical files, run by
// `npm test` on Node's own runner. `utils.ts` imports `$app/paths` for its
// links, which only SvelteKit provides, so it is stubbed.
import { register } from 'node:module';
import { test } from 'node:test';
import assert from 'node:assert/strict';

register(
	'data:text/javascript,' +
		encodeURIComponent(
			`export const resolve = (specifier, context, next) => specifier === '$app/paths'
				? { url: 'data:text/javascript,export const base = ""', shortCircuit: true }
				: next(specifier, context);`
		)
);
const { mergeLayers, fsDuplicates, fsRisks, formatMode } = await import('../src/lib/utils.ts');

const entry = (path, kind, size = 0, sha256) => ({
	path,
	kind,
	size,
	mode: 0o644,
	uid: 0,
	gid: 0,
	mtime: 0,
	offset: 0,
	...(sha256 && { content: { sha256, sha512: '', mime_type: 'text/plain' } })
});
const layer = (...entries) => ({ compressed: false, uncompressed_size: 0, entries });

const tree = mergeLayers([
	layer(
		entry('app', 'dir'),
		entry('app/a', 'file', 100, 'A'),
		entry('app/b', 'file', 200, 'B'),
		entry('etc/key', 'file', 50, 'K'),
		entry('cache', 'dir'),
		entry('cache/x', 'file', 30, 'X'),
		entry('lib', 'dir'),
		entry('lib/y', 'file', 40, 'Y'),
		entry('empty', 'file', 0, 'E')
	),
	layer(
		entry('app/a', 'file', 100, 'A'),
		entry('app/b', 'file', 210, 'B2'),
		entry('etc/key', 'whiteout'),
		entry('cache', 'opaque'),
		entry('lib', 'file', 5, 'L'),
		entry('copy1', 'file', 70, 'C')
	),
	layer(entry('app', 'dir'), entry('copy2', 'file', 70, 'C'), entry('copy3', 'file', 70, 'C'))
]);

test('a later layer wastes the bytes it overwrites or removes, largest first', () => {
	assert.deepEqual(
		tree.wasted.map((waste) => [waste.node.path, waste.size, waste.node.layer, waste.by, waste.change]),
		[
			['app/b', 200, 0, 1, 'replaced'],
			['app/a', 100, 0, 1, 'unchanged'],
			['etc/key', 50, 0, 1, 'removed'],
			['lib/y', 40, 0, 1, 'removed'],
			['cache/x', 30, 0, 1, 'removed']
		]
	);
});

test('a folder listed again keeps what lower layers put in it', () => {
	assert.ok(tree.root.children.get('app')?.children.has('a'));
});

test('identical files group by digest, the most bytes spent on copies first', () => {
	assert.deepEqual(
		fsDuplicates(tree.root).map((set) => [set.size, set.nodes.map((node) => node.path)]),
		[[70, ['copy1', 'copy2', 'copy3']]]
	);
});

test('setuid, setgid, capable and world-writable entries are risky, a sticky folder is not', () => {
	const at = (path, kind, mode) => ({ ...entry(path, kind), mode });
	const risky = fsRisks(
		mergeLayers([
			layer(
				at('usr/bin/su', 'file', 0o4755),
				at('usr/bin/wall', 'file', 0o2755),
				{ ...at('usr/bin/ping', 'file', 0o755), capabilities: ['cap_net_raw'] },
				at('srv/drop', 'dir', 0o777),
				at('srv/drop/log', 'file', 0o666),
				at('tmp', 'dir', 0o1777),
				at('var/mail', 'dir', 0o2775),
				at('etc/passwd', 'file', 0o644)
			)
		]).root
	);
	assert.deepEqual(
		risky.map(({ node, risks }) => [node.path, risks]),
		[
			['srv/drop', ['world-writable']],
			['srv/drop/log', ['world-writable']],
			['usr/bin/ping', ['capabilities']],
			['usr/bin/su', ['setuid']],
			['usr/bin/wall', ['setgid']]
		]
	);
	assert.equal(formatMode(0o4755), 'rwsr-xr-x');
	assert.equal(formatMode(0o2644), 'rw-r-Sr--');
	assert.equal(formatMode(0o1777), 'rwxrwxrwt');
});
