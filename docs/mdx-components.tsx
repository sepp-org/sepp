import defaultMdxComponents from 'fumadocs-ui/mdx';
import { ImageZoom, type ImageZoomProps } from 'fumadocs-ui/components/image-zoom';
import type { MDXComponents } from 'mdx/types';
import { Tab, Tabs } from 'fumadocs-ui/components/tabs';
import { Step, Steps } from 'fumadocs-ui/components/steps';
import { File, Files, Folder } from 'fumadocs-ui/components/files';
import { Accordion, Accordions } from 'fumadocs-ui/components/accordion';
import { InlineTOC } from 'fumadocs-ui/components/inline-toc';
import { TypeTable } from 'fumadocs-ui/components/type-table';
import { Callout } from '@/components/callout';
import { Card, Cards } from '@/components/card';
import { LangTabs } from '@/components/lang-tabs';
import { Mermaid } from '@/components/mermaid';
import { PageFoot } from '@/components/page-foot';
import {
  IconLayers,
  IconBolt,
  IconTerminal,
  IconBook,
} from '@/components/icons';

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    img: (props) => <ImageZoom {...(props as ImageZoomProps)} />,
    // Sepp-themed
    Callout,
    Card,
    Cards,
    LangTabs,
    Mermaid,
    PageFoot,
    IconLayers,
    IconBolt,
    IconTerminal,
    IconBook,
    // Fumadocs primitives
    Tab,
    Tabs,
    Step,
    Steps,
    File,
    Files,
    Folder,
    Accordion,
    Accordions,
    InlineTOC,
    TypeTable,
    ...components,
  };
}
