'use client';

import { type KeyboardEvent, useRef, useState } from 'react';

const examples = {
  cli: {
    label: 'CLI',
    file: 'terminal',
    language: 'bash',
    code: (
      <>
        <span className="lp-code-dim">$</span> cargo run -p krishiv -- sql {'\\'}
        {'\n'}  --query <span className="lp-code-string">&quot;SELECT 42 AS answer&quot;</span>
      </>
    ),
  },
  python: {
    label: 'Python',
    file: 'query.py',
    language: 'python',
    code: (
      <>
        <span className="lp-code-keyword">import</span> krishiv <span className="lp-code-keyword">as</span> ks
        {'\n\n'}session = ks.Session.embedded()
        {'\n'}session.sql(
        {'\n'}  <span className="lp-code-string">&quot;SELECT 42 AS answer&quot;</span>
        {'\n'}).show()
      </>
    ),
  },
  rust: {
    label: 'Rust',
    file: 'main.rs',
    language: 'rust',
    code: (
      <>
        <span className="lp-code-keyword">use</span> krishiv_api::{'{'}Result, Session{'}'};
        {'\n\n'}<span className="lp-code-keyword">fn</span> <span className="lp-code-function">main</span>() -&gt; Result&lt;()&gt; {'{'}
        {'\n'}  <span className="lp-code-keyword">let</span> session = Session::builder().build()?;
        {'\n'}  <span className="lp-code-keyword">let</span> rows = session
        {'\n'}    .sql(<span className="lp-code-string">&quot;SELECT 42 AS answer&quot;</span>)?
        {'\n'}    .collect()?;
        {'\n'}  println!(<span className="lp-code-string">&quot;{'{}'}&quot;</span>, rows.pretty()?);
        {'\n'}  Ok(())
        {'\n'}{'}'}
      </>
    ),
  },
} as const;

type ExampleName = keyof typeof examples;
const exampleNames = Object.keys(examples) as ExampleName[];

export function LandingCodeDemo() {
  const [active, setActive] = useState<ExampleName>('cli');
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const example = examples[active];

  function selectFromKeyboard(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    let next = index;

    if (event.key === 'ArrowRight') next = (index + 1) % exampleNames.length;
    else if (event.key === 'ArrowLeft') next = (index - 1 + exampleNames.length) % exampleNames.length;
    else if (event.key === 'Home') next = 0;
    else if (event.key === 'End') next = exampleNames.length - 1;
    else return;

    event.preventDefault();
    setActive(exampleNames[next]);
    tabs.current[next]?.focus();
  }

  return (
    <div className="lp-code-demo">
      <div className="lp-code-editor">
        <div className="lp-code-tabs" role="tablist" aria-label="Engine API examples">
          {exampleNames.map((name, index) => (
            <button
              aria-controls="landing-code-panel"
              aria-selected={active === name}
              className={active === name ? 'is-active' : undefined}
              id={`landing-tab-${name}`}
              key={name}
              onClick={() => setActive(name)}
              onKeyDown={(event) => selectFromKeyboard(event, index)}
              ref={(element) => { tabs.current[index] = element; }}
              role="tab"
              tabIndex={active === name ? 0 : -1}
              type="button"
            >
              {examples[name].label}
            </button>
          ))}
        </div>
        <div className="lp-code-filebar">
          <span>{example.file}</span>
          <span>{example.language}</span>
        </div>
        <pre
          aria-labelledby={`landing-tab-${active}`}
          id="landing-code-panel"
          role="tabpanel"
          tabIndex={0}
        >
          <code>{example.code}</code>
        </pre>
      </div>

      <div className="lp-code-result" aria-label="Query execution result">
        <div className="lp-code-result-head">
          <span>Execution trace</span>
          <b><i /> Complete</b>
        </div>
        <ol className="lp-trace">
          <li><span>01</span><div><strong>Session</strong><small>Embedded placement</small></div><b>ready</b></li>
          <li><span>02</span><div><strong>DataFusion plan</strong><small>Logical → physical</small></div><b>planned</b></li>
          <li><span>03</span><div><strong>Arrow execution</strong><small>In-process operators</small></div><b>1 batch</b></li>
        </ol>
        <div className="lp-result-table">
          <div><span>answer</span><span>Int64</span></div>
          <strong>42</strong>
        </div>
        <p>One public entry point. Placement remains explicit.</p>
      </div>
    </div>
  );
}
