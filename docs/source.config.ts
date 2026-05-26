import {
  defineDocs,
  defineConfig,
} from 'fumadocs-mdx/config';

export const docs = defineDocs({
  dir: 'content/docs',
});

// Shiki themes tuned to match Sepp tokens.
// Plex Mono with mild italics on comments only.
// Mirrors §7.2 of design_handoff_docs/README.md.
const seppLight = {
  name: 'sepp-light',
  type: 'light',
  colors: {
    'editor.background': '#f1ede5',
    'editor.foreground': '#2a241e',
  },
  tokenColors: [
    { scope: ['comment', 'punctuation.definition.comment'], settings: { foreground: '#8c8576', fontStyle: 'italic' } },
    { scope: ['keyword', 'storage', 'storage.type', 'keyword.control', 'keyword.operator.new', 'keyword.other'], settings: { foreground: '#8a3a82' } },
    { scope: ['string', 'string.quoted', 'punctuation.definition.string'], settings: { foreground: '#6d7e3a' } },
    { scope: ['constant.numeric', 'constant.language', 'constant.language.boolean'], settings: { foreground: '#b5651d' } },
    { scope: ['entity.name.function', 'support.function', 'meta.function-call', 'variable.function'], settings: { foreground: '#2a5fa8' } },
    { scope: ['entity.name.type', 'entity.name.class', 'support.type', 'support.class'], settings: { foreground: '#b5651d' } },
    { scope: ['variable', 'variable.other', 'variable.parameter'], settings: { foreground: '#2a241e' } },
    { scope: ['punctuation', 'meta.brace', 'punctuation.separator'], settings: { foreground: '#2a241e' } },
  ],
} as const;

const seppDark = {
  name: 'sepp-dark',
  type: 'dark',
  colors: {
    'editor.background': '#1c1916',
    'editor.foreground': '#d4cdbd',
  },
  tokenColors: [
    { scope: ['comment', 'punctuation.definition.comment'], settings: { foreground: '#6b6557', fontStyle: 'italic' } },
    { scope: ['keyword', 'storage', 'storage.type', 'keyword.control', 'keyword.operator.new', 'keyword.other'], settings: { foreground: '#c98ec0' } },
    { scope: ['string', 'string.quoted', 'punctuation.definition.string'], settings: { foreground: '#a8bf6a' } },
    { scope: ['constant.numeric', 'constant.language', 'constant.language.boolean'], settings: { foreground: '#d99560' } },
    { scope: ['entity.name.function', 'support.function', 'meta.function-call', 'variable.function'], settings: { foreground: '#7eaad4' } },
    { scope: ['entity.name.type', 'entity.name.class', 'support.type', 'support.class'], settings: { foreground: '#d99560' } },
    { scope: ['variable', 'variable.other', 'variable.parameter'], settings: { foreground: '#d4cdbd' } },
    { scope: ['punctuation', 'meta.brace', 'punctuation.separator'], settings: { foreground: '#d4cdbd' } },
  ],
} as const;

export default defineConfig({
  mdxOptions: {
    rehypeCodeOptions: {
      themes: {
        light: seppLight as any,
        dark: seppDark as any,
      },
    },
  },
});
