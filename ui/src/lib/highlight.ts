import hljs from 'highlight.js/lib/core';
import type { LanguageFn } from 'highlight.js';
import bash from 'highlight.js/lib/languages/bash';
import c from 'highlight.js/lib/languages/c';
import cpp from 'highlight.js/lib/languages/cpp';
import css from 'highlight.js/lib/languages/css';
import go from 'highlight.js/lib/languages/go';
import ini from 'highlight.js/lib/languages/ini';
import java from 'highlight.js/lib/languages/java';
import javascript from 'highlight.js/lib/languages/javascript';
import json from 'highlight.js/lib/languages/json';
import lua from 'highlight.js/lib/languages/lua';
import markdown from 'highlight.js/lib/languages/markdown';
import perl from 'highlight.js/lib/languages/perl';
import php from 'highlight.js/lib/languages/php';
import python from 'highlight.js/lib/languages/python';
import ruby from 'highlight.js/lib/languages/ruby';
import rust from 'highlight.js/lib/languages/rust';
import sql from 'highlight.js/lib/languages/sql';
import typescript from 'highlight.js/lib/languages/typescript';
import xml from 'highlight.js/lib/languages/xml';
import yaml from 'highlight.js/lib/languages/yaml';

// None ships for PEM: its armor lines, headers and a bundle's comments stand
// out from the base64 between them.
const pem: LanguageFn = () => ({
	name: 'PEM',
	contains: [
		{ scope: 'section', begin: /^-----(BEGIN|END) [A-Z0-9 ]+-----$/ },
		{ scope: 'attr', begin: /^[A-Za-z-]+:/ },
		{ scope: 'comment', begin: /^#/, end: /$/ }
	]
});

// Each grammar, with the media types the layer index gives the files it colors.
const grammars: [string, LanguageFn, string[]][] = [
	['bash', bash, ['application/x-sh']],
	['c', c, ['text/x-c']],
	['cpp', cpp, ['text/x-c++']],
	['css', css, ['text/css']],
	['go', go, ['text/x-go']],
	['ini', ini, ['text/x-toml']],
	['java', java, ['text/x-java']],
	['javascript', javascript, ['text/javascript', 'application/javascript']],
	['json', json, ['application/json']],
	['lua', lua, ['text/x-lua']],
	['markdown', markdown, ['text/markdown']],
	['pem', pem, ['application/x-pem-file', 'application/x-x509-ca-cert']],
	['perl', perl, ['application/x-perl']],
	['php', php, ['application/x-httpd-php']],
	['python', python, ['text/x-python']],
	['ruby', ruby, ['text/x-ruby']],
	['rust', rust, ['text/x-rust']],
	['sql', sql, ['application/x-sql']],
	['typescript', typescript, ['application/typescript']],
	['xml', xml, ['text/xml', 'text/html', 'application/xhtml+xml', 'image/svg+xml']],
	['yaml', yaml, ['text/x-yaml']]
];

for (const [name, grammar, types] of grammars) {
	hljs.registerLanguage(name, grammar);
	hljs.registerAliases(types, { languageName: name });
}

/** The text as escaped, colored HTML, or null when no grammar knows the media type or language name. */
export function highlight(text: string, language: string): string | null {
	if (!hljs.getLanguage(language)) return null;
	return hljs.highlight(text, { language, ignoreIllegals: true }).value;
}
