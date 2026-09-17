export interface OidcConfig {
	issuer: string;
	client_id: string;
	/** The OAuth `scope` parameter, verbatim. */
	scopes: string;
}

interface UiConfig {
	name: string;
	oidc?: OidcConfig;
}

let config: UiConfig = $state({ name: 'angos' });
let loaded = $state(false);

export async function loadConfig(): Promise<void> {
	if (loaded) return;

	try {
		const response = await fetch('/v2/_angos/ui/config');
		if (response.ok) {
			const data: UiConfig = await response.json();
			config = data;
		}
	} catch {
		// Use defaults on error
	}
	loaded = true;
}

export function getRegistryName(): string {
	return config.name;
}

export function getOidcConfig(): OidcConfig | null {
	return config.oidc ?? null;
}
