import { source } from '@/lib/source';
import { createFromSource } from 'fumadocs-core/search/server';

// `createFromSource` builds an advanced (structured) index from the MDX
// frontmatter + content. We use it instead of `createSearchAPI('simple', ...)`
// because the static-search client has a bug where `searchSimple` is called
// with the wrapper object instead of the actual Orama db, which silently
// errors and leaves the dialog empty. The `searchAdvanced` path correctly
// unwraps and works.
const api = createFromSource(source);

export const revalidate = false;
export const GET = api.staticGET;
