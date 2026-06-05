'use client';

import {
  Children,
  isValidElement,
  useEffect,
  useState,
  type ReactNode,
} from 'react';
import {
  IconRust,
  IconPython,
  IconGo,
  IconJavaScript,
  IconTypeScript,
  IconShell,
} from './lang-icons';

const LANG_ICON: Record<string, (p: { size?: number }) => React.ReactElement> = {
  Rust: IconRust,
  Python: IconPython,
  Go: IconGo,
  JavaScript: IconJavaScript,
  TypeScript: IconTypeScript,
  Shell: IconShell,
};

// GitHub linguist brand colors — same hues we used for the legacy dots.
const LANG_COLOR: Record<string, string> = {
  Rust: '#dea584',
  Python: '#3572A5',
  Go: '#00ADD8',
  JavaScript: '#f1e05a',
  TypeScript: '#3178c6',
};

interface LangTabsProps {
  items: string[];
  children: ReactNode;
  /**
   * If set, the active tab is remembered in localStorage so all code blocks
   * on the page can swap languages together.
   */
  groupId?: string;
}

export function LangTabs({ items, children, groupId = 'lang' }: LangTabsProps) {
  const [active, setActive] = useState(0);

  // Restore + persist active tab across blocks
  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(`sepp-langtabs:${groupId}`);
      if (stored !== null) {
        const idx = items.indexOf(stored);
        if (idx >= 0) setActive(idx);
      }
    } catch {
      // ignore
    }
  }, [groupId, items]);

  const setActiveByIdx = (idx: number) => {
    setActive(idx);
    try {
      window.localStorage.setItem(`sepp-langtabs:${groupId}`, items[idx]);
    } catch {
      // ignore
    }
  };

  const panels = Children.toArray(children).filter(isValidElement);

  return (
    <div className="sepp-tabs">
      <div role="tablist" aria-label="Language">
        {items.map((label, i) => {
          const Icon = LANG_ICON[label];
          const color = LANG_COLOR[label];
          return (
            <button
              key={label}
              role="tab"
              type="button"
              aria-selected={i === active}
              data-state={i === active ? 'active' : 'inactive'}
              onClick={() => setActiveByIdx(i)}
            >
              {Icon ? (
                <span
                  className="sepp-tab-icon"
                  style={color ? { color } : undefined}
                >
                  <Icon size={13} />
                </span>
              ) : null}
              {label}
            </button>
          );
        })}
      </div>
      {panels.map((panel, i) => (
        <div
          key={i}
          role="tabpanel"
          hidden={i !== active}
          aria-labelledby={`tab-${items[i]}`}
        >
          {panel}
        </div>
      ))}
    </div>
  );
}
