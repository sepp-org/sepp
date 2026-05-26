import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { SeppMark } from '@/components/sepp-mark';
import { SEPP_VERSION } from '@/lib/sepp-version';

export const baseOptions: BaseLayoutProps = {
  nav: {
    title: (
      <span className="sepp-brand">
        <SeppMark size={22} />
        <span className="sepp-wordmark">sepp</span>
        <span className="sepp-version">v{SEPP_VERSION}</span>
      </span>
    ),
    url: '/',
    transparentMode: 'none',
  },
  githubUrl: 'https://github.com/sepp-org/sepp',
};
