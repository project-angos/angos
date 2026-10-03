// The repository names every browse path resolves against: read once per page
// load, and read again after a failure or on request. `api.ts` reaches the
// registry through `auth.svelte` and `utils.ts` links through `$app/paths`,
// which only SvelteKit can load, so both are stubbed, with no session.
import { register } from 'node:module';
import { test } from 'node:test';
import assert from 'node:assert/strict';

register(
	'data:text/javascript,' +
		encodeURIComponent(
			`const stubs = {
				'./auth.svelte': 'export const authHeaders = () => ({}); export const signInOnUnauthorized = async () => {};',
				'$app/paths': 'export const base = ""'
			};
			export const resolve = (specifier, context, next) => {
				if (specifier in stubs) {
					return { url: 'data:text/javascript,' + encodeURIComponent(stubs[specifier]), shortCircuit: true };
				}
				return next(specifier === './utils' ? './utils.ts' : specifier, context);
			};`
		)
);
const { fetchRepositoryNames } = await import('../src/lib/api.ts');

test('the repository names are read once, again after a failure or when fresh', async () => {
	let reads = 0;
	let up = false;
	globalThis.fetch = async () => {
		reads++;
		return up
			? Response.json({ repositories: [{ name: 'library' }], total: 1 })
			: new Response(null, { status: 503 });
	};

	assert.equal((await fetchRepositoryNames()).error, 'HTTP 503');
	up = true;
	assert.deepEqual((await fetchRepositoryNames()).data, ['library']);
	assert.deepEqual((await fetchRepositoryNames()).data, ['library']);
	assert.equal(reads, 2, 'a failed read is retried, a good one kept');

	await fetchRepositoryNames(true);
	assert.equal(reads, 3, 'fresh rereads past the kept names');
});
