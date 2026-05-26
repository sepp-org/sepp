import { loader } from 'fumadocs-core/source';
import { docs } from '@/.source';

// fumadocs-mdx returns `files: () => [...]` (a lazy thunk); fumadocs-core's
// loader destructures `files` and expects it to be array-like. Unwrap here.
const mdx = docs.toFumadocsSource() as { files: (() => unknown[]) | unknown[] };
const files = typeof mdx.files === 'function' ? mdx.files() : mdx.files;

export const source = loader({
  baseUrl: '/docs',
  source: { files: files as never },
});
