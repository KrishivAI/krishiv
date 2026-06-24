'use client';

import { useState } from 'react';

export type CodeTab = {
  id: string;
  label: string;
  language: string;
  code: string;
};

export function CodeTabs({ tabs }: { tabs: CodeTab[] }) {
  const [active, setActive] = useState(tabs[0]?.id ?? '');
  const [copied, setCopied] = useState(false);
  const current = tabs.find((t) => t.id === active) ?? tabs[0];

  const onCopy = async () => {
    if (!current) return;
    try {
      if (typeof navigator !== 'undefined' && navigator.clipboard) {
        await navigator.clipboard.writeText(current.code);
      }
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
    } catch {
      setCopied(false);
    }
  };

  if (!current) return null;

  const lines = current.code.split('\n');

  return (
    <>
      <div className="code-tabs" role="tablist" aria-label="Code example">
        {tabs.map((t) => (
          <button
            key={t.id}
            role="tab"
            aria-selected={t.id === active}
            className={t.id === active ? 'active' : ''}
            onClick={() => setActive(t.id)}
            type="button"
          >
            {t.label}
          </button>
        ))}
      </div>
      <button className="copy-button" aria-label={`Copy ${current.label} example`} onClick={onCopy} type="button">
        {copied ? (
          <svg viewBox="0 0 20 20" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true"><path d="m4 10 4 4 8-9"/></svg>
        ) : (
          <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden="true"><rect x="7" y="5" width="9" height="12" rx="1.5"/><path d="M4 13V4.5A1.5 1.5 0 0 1 5.5 3H13"/></svg>
        )}
      </button>
      <pre aria-label={`${current.label} example`}><code>
        {lines.map((line, i) => (
          <span className="line" key={i}><span className="num">{i + 1}</span><span>{highlight(line, current.language)}</span></span>
        ))}
      </code></pre>
    </>
  );
}

function highlight(line: string, language: string) {
  if (language === 'sql') {
    return line
      .replace(/\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|LIMIT|AS|AND|OR|NOT|IN|IS|NULL|TRUE|FALSE|INTERVAL|INSERT|UPDATE|DELETE|CREATE|DROP|JOIN|LEFT|RIGHT|INNER|OUTER|ON|INTO|VALUES|SET)\b/g, '<b>$1</b>')
      .replace(/\b(SUM|COUNT|AVG|MIN|MAX|COALESCE|CASE|WHEN|THEN|ELSE|END|NOW|CURRENT_DATE|CURRENT_TIMESTAMP|CAST|TRY_CAST|DATE_TRUNC)\b/g, '<i>$1</i>')
      .replace(/\b(INTERVAL)\b\s*('[^']*')/g, '<u>INTERVAL</u> <mark>$2</mark>')
      .replace(/(--.*$)/g, '<em>$1</em>');
  }
  if (language === 'rust') {
    return line
      .replace(/\b(use|fn|async|await|let|mut|pub|return|if|else|match|for|while|in|as|impl|trait|struct|enum|self|Self|where|true|false)\b/g, '<b>$1</b>')
      .replace(/\b(Ok|Err|None|Some|Vec|String|Result|Option|Session|Arc|std|tokio|krishiv_api|krishiv)\b/g, '<i>$1</i>')
      .replace(/(\/\/.*$)/g, '<em>$1</em>')
      .replace(/(&quot;(?:[^&]|&(?!quot;))*&quot;|"[^"]*")/g, '<mark>$1</mark>');
  }
  if (language === 'python') {
    return line
      .replace(/\b(import|from|as|def|return|if|else|elif|for|while|in|is|not|and|or|True|False|None|class|with|lambda|yield|raise|try|except|finally)\b/g, '<b>$1</b>')
      .replace(/\b(ks|krishiv|pyarrow|pa|pa\.|pd|pandas|numpy|np)\b/g, '<i>$1</i>')
      .replace(/("""[\s\S]*?""")/g, '<em>$1</em>')
      .replace(/(#[^\n]*)/g, '<em>$1</em>')
      .replace(/('[^']*'|"[^"]*")/g, '<mark>$1</mark>');
  }
  return line;
}
