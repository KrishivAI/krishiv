'use client';

import { useEffect, useRef } from 'react';

const COPY_ICON = '<svg viewBox="0 0 20 20" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="7" y="5" width="9" height="12" rx="1.5"/><path d="M4 13V4.5A1.5 1.5 0 0 1 5.5 3H13"/></svg>';

function enhance(pre: HTMLPreElement) {
  if (pre.parentElement?.classList.contains('codeblock-wrap')) return;

  const code = pre.querySelector('code');
  const cls = (code?.className || pre.className || '').toString();
  const langMatch = cls.match(/language-([\w+-]+)/);
  const lang = langMatch ? langMatch[1] : null;

  const wrap = document.createElement('div');
  wrap.className = 'codeblock-wrap';
  pre.parentNode?.insertBefore(wrap, pre);
  wrap.appendChild(pre);

  const header = document.createElement('div');
  header.className = 'codeblock-header';

  if (lang) {
    const langSpan = document.createElement('span');
    langSpan.className = 'codeblock-lang';
    langSpan.textContent = lang;
    header.appendChild(langSpan);
  } else {
    const spacer = document.createElement('span');
    header.appendChild(spacer);
  }

  const copyBtn = document.createElement('button');
  copyBtn.type = 'button';
  copyBtn.className = 'codeblock-copy';
  copyBtn.setAttribute('aria-label', 'Copy code to clipboard');
  copyBtn.innerHTML = `${COPY_ICON}<span>Copy</span>`;
  copyBtn.addEventListener('click', async () => {
    const text = (code?.textContent ?? pre.textContent ?? '').replace(/\n$/, '');
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      copyBtn.classList.add('copied');
      copyBtn.innerHTML = '<span>Copied</span>';
      setTimeout(() => {
        copyBtn.classList.remove('copied');
        copyBtn.innerHTML = `${COPY_ICON}<span>Copy</span>`;
      }, 1400);
    } catch {
      copyBtn.innerHTML = '<span>Press ⌘C</span>';
    }
  });
  header.appendChild(copyBtn);
  wrap.insertBefore(header, pre);
  pre.classList.add('codeblock-pre');
}

export function CodeBlockBody({ html }: { html: string }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    const pres = ref.current.querySelectorAll<HTMLPreElement>('pre');
    pres.forEach(enhance);
  }, [html]);
  return <div ref={ref} className="prose" dangerouslySetInnerHTML={{ __html: html }} />;
}
