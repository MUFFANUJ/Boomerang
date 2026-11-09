// Pretty-printed JSON with lightweight syntax coloring. Values are rendered as text nodes.
import { Fragment, useMemo, type ReactNode } from "react";

const TOKEN = /("(?:\\.|[^"\\])*")(\s*:)?|\b(true|false)\b|\b(null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;

function highlight(src: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  let i = 0;
  for (const m of src.matchAll(TOKEN)) {
    const idx = m.index ?? 0;
    if (idx > last) out.push(<Fragment key={i++}>{src.slice(last, idx)}</Fragment>);
    if (m[1]) {
      out.push(
        <span key={i++} className={m[2] ? "json-key" : "json-str"}>
          {m[1]}
        </span>,
      );
      if (m[2]) out.push(<Fragment key={i++}>{m[2]}</Fragment>);
    } else if (m[3]) out.push(<span key={i++} className="json-bool">{m[3]}</span>);
    else if (m[4]) out.push(<span key={i++} className="json-null">{m[4]}</span>);
    else if (m[5]) out.push(<span key={i++} className="json-num">{m[5]}</span>);
    last = idx + m[0].length;
  }
  if (last < src.length) out.push(<Fragment key={i++}>{src.slice(last)}</Fragment>);
  return out;
}

export function JsonView({ value, maxHeight }: { value: unknown; maxHeight?: number }) {
  const text = useMemo(() => {
    if (typeof value === "string") {
      try {
        return JSON.stringify(JSON.parse(value), null, 2);
      } catch {
        return value;
      }
    }
    try {
      return JSON.stringify(value, null, 2) ?? "null";
    } catch {
      return String(value);
    }
  }, [value]);
  const nodes = useMemo(() => (text.length > 200_000 ? [text] : highlight(text)), [text]);
  return (
    <pre className="code" style={maxHeight ? { maxHeight } : undefined}>
      {nodes}
    </pre>
  );
}
