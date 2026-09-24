// Screenshots every web UI view of the registry generate.sh seeded, light and
// dark, into the directory given as the argument, as 1316px-wide pages at 2x
// cropped to their content, the way doc/reference/ui.md embeds them.
import { chromium } from 'playwright';
import { mkdir, readFile } from 'node:fs/promises';
import path from 'node:path';

const registry = 'http://127.0.0.1:8900';
const out = process.argv[2];

const exact = (page, selector, text) => page.locator(selector, { hasText: new RegExp(`^${text}$`) });
const expandAnnotations = (page) => page.locator('.toggle-annotations').first().click();
// An open file's pane is at most as tall as the window: one as tall as the page
// shows it the way a tall screen does.
const fitWindow = async (page) =>
	page.setViewportSize({ width: 1316, height: await page.evaluate(() => document.body.scrollHeight) });

const VIEWS = [
	{ name: 'repositories', url: '/' },
	{ name: 'namespaces', url: '/library' },
	{ name: 'manifests', url: '/library/alpine', prepare: (page) => page.locator('button.tree-toggle').first().click() },
	{
		name: 'uploads',
		url: '/library/nginx',
		prepare: async (page) => {
			for (const toggle of await page.locator('button.tree-toggle').all()) await toggle.click();
		}
	},
	{ name: 'manifest-details', url: '/library/nginx:1.25-alpine', prepare: expandAnnotations },
	{ name: 'manifest-index', url: '/library/alpine:3.19' },
	{ name: 'pull-history', url: '/library/nginx:1.25-alpine#history' },
	{ name: 'vulnerabilities', url: '/library/alpine:3.19#vulnerabilities/linux/amd64' },
	{
		name: 'filesystem',
		url: '/library/nginx:1.25-alpine#filesystem/docker-entrypoint.d/15-local-resolvers.envsh',
		prepare: fitWindow
	},
	{
		// The packages the image's last layer added over the nginx install.
		name: 'filesystem-diff',
		url: '/library/nginx:1.25-alpine#filesystem/etc/apk/world',
		prepare: async (page) => {
			await page.getByRole('button', { name: 'Diff' }).click();
			await page.locator('.fs-diff').waitFor();
			// The link opened /etc, too long a folder to keep the shot short; the
			// click that folds it leaves the keyboard's shade on its row.
			await exact(page, '.fs-label', 'etc').click();
			await page.evaluate(() => document.activeElement.blur());
			await fitWindow(page);
		}
	},
	{
		// A dynamic binary: what it needs and how it is hardened.
		name: 'filesystem-elf',
		url: '/library/nginx:1.25-alpine#filesystem/usr/sbin/nginx',
		prepare: async (page) => {
			// The link opened /usr/sbin, too long a folder to keep the shot short.
			await exact(page, '.fs-label', 'usr').click();
			await page.evaluate(() => document.activeElement.blur());
			await fitWindow(page);
		}
	},
	{
		name: 'filesystem-icons',
		url: '/library/nginx:1.25-alpine#filesystem',
		prepare: async (page) => {
			await page.getByRole('button', { name: 'Icons' }).click();
			for (const name of ['etc', 'nginx']) await exact(page, '.fs-tile-name', name).click();
		}
	},
	{
		name: 'filesystem-layers',
		url: '/library/nginx:1.25-alpine#filesystem',
		prepare: async (page) => {
			// A folder opened first keeps the table taller than the menu over it.
			await exact(page, '.fs-label', 'etc').click();
			await page.locator('details.menu summary').click();
			await page.locator('.menu-panel input[type=checkbox]').last().check();
		}
	},
	{
		// The SSH key the last layer deleted, open beside every leak it found.
		name: 'filesystem-secrets',
		url: '/apps/webapp:1.0#filesystem/root/.ssh/id_ed25519@L2',
		prepare: async (page) => {
			await page.getByRole('button', { name: 'Secrets' }).click();
			await fitWindow(page);
		}
	},
	{
		// The launcher granted a capability, beside the setuid script and the folder anyone can write.
		name: 'filesystem-permissions',
		url: '/apps/webapp:1.0#filesystem/usr/local/bin/serve',
		prepare: async (page) => {
			await page.getByRole('button', { name: 'Permissions' }).click();
			await fitWindow(page);
		}
	},
	{
		name: 'filesystem-waste',
		url: '/library/nginx:1.25-alpine#filesystem',
		prepare: (page) => page.getByRole('button', { name: 'Waste' }).click()
	},
	{ name: 'oras-files', url: '/artifacts/charts/demo:1.0', prepare: expandAnnotations },
	{ name: 'jobs', url: '/jobs/replication' }
];

const json = async (url) => (await fetch(registry + url)).json();

// Waits until nothing is pending. A dead-lettered job is reported, not fatal:
// the buildx attestation manifests of an index are enqueued too, and their
// in-toto layers are not something a scanner reads.
async function drained(queue) {
	while ((await json(`/v2/_angos/jobs/list?queue=${queue}`)).jobs.length > 0) {
		await new Promise((resolve) => setTimeout(resolve, 2000));
	}
	for (const job of (await json(`/v2/_angos/jobs/failed?queue=${queue}`)).failed) {
		console.warn(`${queue} job failed: ${job.lock_key}: ${job.last_error}`);
	}
}

async function settle(page) {
	await page.waitForLoadState('networkidle');
	await page.locator('.loading').first().waitFor({ state: 'detached' });
}

async function shoot(contexts, view) {
	for (const [theme, context] of Object.entries(contexts)) {
		const page = await context.newPage();
		await page.goto(registry + view.url);
		await settle(page);
		if (view.prepare) {
			await view.prepare(page);
			await settle(page);
		}
		// A click scrolled the page and left the pointer over something; the
		// sticky bar belongs at the top of the shot and nothing hovered.
		await page.evaluate(() => window.scrollTo(0, 0));
		await page.mouse.move(0, 0);
		await page.screenshot({ path: path.join(out, `ui-${view.name}-${theme}.png`), fullPage: true });
		await page.close();
		console.log(`ui-${view.name}-${theme}.png`);
	}
}

// The two themes side by side, split down the middle of one view.
async function composite(browser, view) {
	const image = async (theme) =>
		`data:image/png;base64,${(await readFile(path.join(out, `ui-${view}-${theme}.png`))).toString('base64')}`;
	const page = await browser.newPage({ viewport: { width: 1316, height: 320 }, deviceScaleFactor: 2 });
	await page.setContent(`
		<div id="both" style="position: relative; width: 1316px; line-height: 0">
			<img src="${await image('light')}" style="width: 100%; clip-path: inset(0 50% 0 0)">
			<img src="${await image('dark')}" style="position: absolute; inset: 0; width: 100%; clip-path: inset(0 0 0 50%)">
		</div>`);
	await page.locator('#both').screenshot({ path: path.join(out, 'ui-dark-light.png') });
	await page.close();
	console.log('ui-dark-light.png');
}

await mkdir(out, { recursive: true });
const browser = await chromium.launch();
const contexts = {};
for (const theme of ['light', 'dark']) {
	contexts[theme] = await browser.newContext({
		viewport: { width: 1316, height: 320 },
		deviceScaleFactor: 2,
		// The registry never challenges, so the identity is sent unasked.
		extraHTTPHeaders: { Authorization: `Basic ${Buffer.from('admin:test').toString('base64')}` }
	});
	await contexts[theme].addInitScript((theme) => localStorage.setItem('theme-preference', theme), theme);
}
await drained('scan');
await drained('index');
for (const view of VIEWS) await shoot(contexts, view);
await composite(browser, 'manifests');
await browser.close();
