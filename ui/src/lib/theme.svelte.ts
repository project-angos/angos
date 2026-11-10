export type ThemePreference = 'light' | 'dark' | 'system';

const STORAGE_KEY = 'theme-preference';

let preference: ThemePreference = $state('system');

function updateTheme() {
	const system = window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
	document.documentElement.setAttribute('data-theme', preference === 'system' ? system : preference);
}

export function initTheme() {
	const stored = localStorage.getItem(STORAGE_KEY) as ThemePreference | null;
	if (stored && ['light', 'dark', 'system'].includes(stored)) {
		preference = stored;
	}

	updateTheme();

	window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', () => {
		if (preference === 'system') {
			updateTheme();
		}
	});
}

export function setTheme(newPreference: ThemePreference) {
	preference = newPreference;
	localStorage.setItem(STORAGE_KEY, newPreference);
	updateTheme();
}

export function getThemePreference(): ThemePreference {
	return preference;
}
