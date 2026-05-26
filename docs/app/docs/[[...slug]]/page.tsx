import * as React from 'react';
import { source } from '@/lib/source';
import {
  DocsPage,
  DocsBody,
  DocsTitle,
  DocsDescription,
} from 'fumadocs-ui/page';
import { notFound } from 'next/navigation';
import { getMDXComponents } from '@/mdx-components';

interface MDXPageData {
  title?: string;
  description?: string;
  body: React.ComponentType<{ components?: unknown }>;
  toc?: unknown;
  full?: boolean;
}

export default async function Page(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const data = page.data as unknown as MDXPageData;
  const MDXContent = data.body;

  return (
    <DocsPage toc={data.toc as never} full={data.full}>
      <DocsTitle>{data.title}</DocsTitle>
      {data.description ? <DocsDescription>{data.description}</DocsDescription> : null}
      <DocsBody>
        <MDXContent components={getMDXComponents()} />
      </DocsBody>
    </DocsPage>
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();
  const data = page.data as unknown as MDXPageData;
  return {
    title: data.title,
    description: data.description,
  };
}
