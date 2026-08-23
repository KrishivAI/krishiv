// Thin React wrapper around a CodeMirror 6 EditorView with SQL mode and a
// run/format keymap — the platform console's editor pattern, minus its
// schema-aware autocomplete (the engine daemon exposes no catalog endpoint
// to feed it, and fabricated completions are worse than none).

import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { PostgreSQL, sql } from "@codemirror/lang-sql";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, placeholder } from "@codemirror/view";
import { useEffect, useRef } from "react";

export function CodeMirrorSql({
  value,
  onChange,
  onRun,
  onFormat,
}: {
  value: string;
  onChange: (sql: string) => void;
  onRun: () => boolean;
  onFormat: () => boolean;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);
  const cbRef = useRef({ onChange, onRun, onFormat });
  cbRef.current = { onChange, onRun, onFormat };

  useEffect(() => {
    if (!hostRef.current) return;
    const state = EditorState.create({
      doc: value,
      extensions: [
        history(),
        keymap.of([
          { key: "Mod-Enter", run: () => cbRef.current.onRun() },
          { key: "Mod-Shift-f", run: () => cbRef.current.onFormat() },
          indentWithTab,
          ...defaultKeymap,
          ...historyKeymap,
        ]),
        sql({ dialect: PostgreSQL }),
        placeholder("SELECT …  (⌘⏎ to run, ⌘⇧F to format)"),
        EditorView.updateListener.of((u) => {
          if (u.docChanged) cbRef.current.onChange(u.state.doc.toString());
        }),
        EditorView.theme({
          "&": { fontSize: "13px" },
          ".cm-content": { fontFamily: "ui-monospace, monospace", minHeight: "9rem" },
          "&.cm-focused": { outline: "none" },
        }),
      ],
    });
    const view = new EditorView({ state, parent: hostRef.current });
    viewRef.current = view;
    return () => view.destroy();
    // The view owns the document after mount; `value` is only the seed.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // External value changes (format button) replace the doc.
  useEffect(() => {
    const view = viewRef.current;
    if (view && view.state.doc.toString() !== value) {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } });
    }
  }, [value]);

  return (
    <div
      ref={hostRef}
      className="rounded border border-border bg-surface [&_.cm-editor]:bg-transparent [&_.cm-gutters]:hidden"
    />
  );
}
