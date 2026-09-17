import { goto } from '$app/navigation';
import { loadConfig, getOidcConfig } from './config.svelte';

interface Session {
	/** The ID token, sent to the registry as a bearer. */
	token: string;
	/** The token's `exp`, in milliseconds. */
	expiresAt: number;
	/** What the top bar shows for the signed-in user. */
	subject: string;
}

interface Pending {
	verifier: string;
	state: string;
	nonce: string;
	/** Where the user was when they signed in, returned to afterwards. */
	origin: string;
}

const SESSION_KEY = 'angos-oidc-session';
const PENDING_KEY = 'angos-oidc-pending';

/** One automatic sign-in per page load, so a token the registry keeps
 * refusing cannot bounce the browser between here and the provider. */
let attempted = false;

function base64url(bytes: Uint8Array): string {
	return btoa(String.fromCharCode(...bytes))
		.replace(/\+/g, '-')
		.replace(/\//g, '_')
		.replace(/=+$/, '');
}

function randomString(): string {
	return base64url(crypto.getRandomValues(new Uint8Array(32)));
}

async function codeChallenge(verifier: string): Promise<string> {
	const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
	return base64url(new Uint8Array(digest));
}

/** A JWT's payload, unverified: the registry is what validates the token. */
function decodeClaims(token: string): Record<string, unknown> {
	try {
		const payload = token.split('.')[1] ?? '';
		const base64 = payload
			.replace(/-/g, '+')
			.replace(/_/g, '/')
			.padEnd(Math.ceil(payload.length / 4) * 4, '=');
		const bytes = Uint8Array.from(atob(base64), (character) => character.charCodeAt(0));
		return JSON.parse(new TextDecoder().decode(bytes));
	} catch {
		return {};
	}
}

function claimString(claims: Record<string, unknown>, name: string): string | null {
	const value = claims[name];
	return typeof value === 'string' ? value : null;
}

function sessionFromToken(token: string): Session | null {
	const claims = decodeClaims(token);
	if (typeof claims.exp !== 'number') return null;
	return {
		token,
		expiresAt: claims.exp * 1000,
		subject:
			claimString(claims, 'preferred_username') ??
			claimString(claims, 'email') ??
			claimString(claims, 'name') ??
			claimString(claims, 'sub') ??
			'signed in'
	};
}

function storedSession(): Session | null {
	if (typeof sessionStorage === 'undefined') return null;
	const stored = sessionStorage.getItem(SESSION_KEY);
	if (!stored) return null;
	try {
		return JSON.parse(stored);
	} catch {
		return null;
	}
}

let session: Session | null = $state(storedSession());

/** The session while it is still valid. There is no refresh token, so an
 * expired one is simply not sent and means signing in again. */
function currentSession(): Session | null {
	return session && session.expiresAt > Date.now() ? session : null;
}

export function authHeaders(): Record<string, string> {
	const active = currentSession();
	return active ? { Authorization: `Bearer ${active.token}` } : {};
}

export function signedInAs(): string | null {
	return currentSession()?.subject ?? null;
}

/** Whether sign-in is offered at all, which takes a configured `[ui.oidc]`. */
export function signInAvailable(): boolean {
	return getOidcConfig() !== null;
}

export function signOut() {
	session = null;
	sessionStorage.removeItem(SESSION_KEY);
	// Reloaded for the same reason signing in is: the views hold what the
	// session could see, and nothing refetches them where they are.
	location.reload();
}

async function endpoints(issuer: string): Promise<{ authorization: string; token: string }> {
	const url = `${issuer.replace(/\/$/, '')}/.well-known/openid-configuration`;
	const response = await fetch(url);
	if (!response.ok) {
		throw new Error(`OIDC discovery failed (HTTP ${response.status})`);
	}
	const document = await response.json();
	return { authorization: document.authorization_endpoint, token: document.token_endpoint };
}

/** The one URI the provider has to allow, the same for every page the user
 * signs in from: where they were is restored from storage afterwards. */
function redirectUri(): string {
	return `${location.origin}/`;
}

export async function signIn(): Promise<void> {
	await loadConfig();
	const config = getOidcConfig();
	if (!config) return;

	const pending: Pending = {
		verifier: randomString(),
		state: randomString(),
		nonce: randomString(),
		origin: location.href
	};
	sessionStorage.setItem(PENDING_KEY, JSON.stringify(pending));

	const { authorization } = await endpoints(config.issuer);
	const params = new URLSearchParams({
		response_type: 'code',
		client_id: config.client_id,
		redirect_uri: redirectUri(),
		scope: config.scopes,
		state: pending.state,
		nonce: pending.nonce,
		code_challenge: await codeChallenge(pending.verifier),
		code_challenge_method: 'S256'
	});
	location.assign(`${authorization}?${params}`);
}

/** Starts sign-in on the first request the registry refuses, so a private
 * registry needs no click while a public one is still browsed anonymously. */
export async function signInOnUnauthorized(): Promise<void> {
	if (attempted || currentSession() || !signInAvailable()) return;
	attempted = true;
	await signIn();
}

async function exchangeCode(code: string, pending: Pending): Promise<string> {
	await loadConfig();
	const config = getOidcConfig();
	if (!config) {
		throw new Error('Sign-in is not configured');
	}

	const { token } = await endpoints(config.issuer);
	const response = await fetch(token, {
		method: 'POST',
		headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
		body: new URLSearchParams({
			grant_type: 'authorization_code',
			code,
			redirect_uri: redirectUri(),
			client_id: config.client_id,
			code_verifier: pending.verifier
		})
	});
	if (!response.ok) {
		throw new Error(`Token exchange failed (HTTP ${response.status})`);
	}

	const body = await response.json();
	if (typeof body.id_token !== 'string') {
		throw new Error('The provider returned no ID token');
	}
	return body.id_token;
}

/**
 * Finishes a sign-in the provider redirected back from, then returns the user
 * to the page they left. Does nothing on a page load that is not such a
 * redirect. Returns what went wrong, or `null` when nothing did.
 */
export async function completeSignIn(): Promise<string | null> {
	const params = new URLSearchParams(location.search);
	const code = params.get('code');
	const failure = params.get('error');
	if (!code && !failure) return null;

	const stored = sessionStorage.getItem(PENDING_KEY);
	sessionStorage.removeItem(PENDING_KEY);
	if (!stored) return null;
	const pending: Pending = JSON.parse(stored);

	const restore = () => goto(pending.origin, { replaceState: true });

	if (failure) {
		await restore();
		return `Sign-in was refused: ${failure}`;
	}
	// An unsolicited or replayed redirect: the code was not requested here.
	if (params.get('state') !== pending.state) {
		await restore();
		return 'Sign-in failed: the provider returned an unexpected state';
	}

	try {
		const token = await exchangeCode(code ?? '', pending);
		if (decodeClaims(token).nonce !== pending.nonce) {
			await restore();
			return 'Sign-in failed: the ID token was issued for another request';
		}
		const established = sessionFromToken(token);
		if (!established) {
			await restore();
			return 'Sign-in failed: the ID token carries no expiry';
		}
		session = established;
		sessionStorage.setItem(SESSION_KEY, JSON.stringify(established));
		// A document load, not a client-side one: the views fetch as they mount,
		// which here already happened without the session, and returning to the
		// route they are on would not mount them again.
		location.replace(pending.origin);
		return null;
	} catch (e) {
		await restore();
		return e instanceof Error ? e.message : 'Sign-in failed';
	}
}
