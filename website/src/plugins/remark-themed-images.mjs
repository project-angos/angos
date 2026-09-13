import {visit} from 'unist-util-visit';

// Turns a `<picture>` with light and dark sources, which GitHub renders as
// is, into two markdown images the theme shows one of. Emitting markdown
// image nodes rather than `<img>` tags matters: Docusaurus bundles the
// former as assets and leaves the latter's relative paths untouched, so
// this must run before its default plugins.
export default function remarkThemedImages() {
  return (tree) => {
    visit(tree, (node) => {
      if (node.type === 'mdxJsxFlowElement' && node.name === 'picture') {
        transformPicture(node);
      }
    });
  };
}

function transformPicture(node) {
  let darkSrc = '';
  let lightSrc = '';
  let alt = '';

  for (const child of node.children || []) {
    if (child.name === 'source') {
      const media = child.attributes?.find(a => a.name === 'media')?.value || '';
      const srcset = child.attributes?.find(a => a.name === 'srcset' || a.name === 'srcSet')?.value || '';
      if (media.includes('dark')) {
        darkSrc = srcset;
      } else if (media.includes('light')) {
        lightSrc = srcset;
      }
    } else if (child.name === 'img') {
      alt = child.attributes?.find(a => a.name === 'alt')?.value || '';
      if (!lightSrc) {
        lightSrc = child.attributes?.find(a => a.name === 'src')?.value || '';
      }
    }
  }

  if (!darkSrc) darkSrc = lightSrc;

  node.name = 'span';
  node.attributes = [
    {type: 'mdxJsxAttribute', name: 'className', value: 'themed-image'}
  ];
  node.children = [themed('light-only', lightSrc, alt), themed('dark-only', darkSrc, alt)];
}

function themed(className, url, alt) {
  return {
    type: 'mdxJsxFlowElement',
    name: 'span',
    attributes: [{type: 'mdxJsxAttribute', name: 'className', value: className}],
    children: [{type: 'image', url, alt}],
  };
}
