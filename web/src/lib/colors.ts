// Chart color roles. Values live in tokens.css; charts reference the CSS variables so light/dark
// swap in one place. Canvas code reads resolved values via readCssVar().
import { useEffect, useRef, useState } from "react";

export const SERIES = [
  "var(--series-1)",
  "var(--series-2)",
  "var(--series-3)",
  "var(--series-4)",
  "var(--series-5)",
  "var(--series-6)",
  "var(--series-7)",
  "var(--series-8)",
] as const;
export const SERIES_OTHER = "var(--series-other)";
export const SERIES_CONTEXT = "var(--series-context)";

export const C = {
  accent: "var(--series-1)",
  context: SERIES_CONTEXT,
  good: "var(--good)",
  warning: "var(--warning)",
  serious: "var(--serious)",
  critical: "var(--critical)",
  grid: "var(--grid)",
  axis: "var(--axis)",
  muted: "var(--text-muted)",
};

export function readCssVar(name: string, el: Element = document.documentElement): string {
  return getComputedStyle(el).getPropertyValue(name).trim();
}

/** Increments when the OS color scheme flips, so canvas code can re-read tokens. */
export function useColorSchemeVersion(): number {
  const [v, setV] = useState(0);
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: light)");
    const h = () => setV((x) => x + 1);
    mq.addEventListener("change", h);
    return () => mq.removeEventListener("change", h);
  }, []);
  return v;
}

/**
 * Stable categorical slots per entity id (color follows the entity, never its rank).
 * A slot is released only when its entity is gone from `active`, and is then reused.
 * Past 8 concurrent entities, extras fold into the "other" gray.
 */
export function useStableSlots(ids: string[], active: string[]): Map<string, string> {
  const ref = useRef(new Map<string, number>());
  const map = ref.current;
  const activeSet = new Set(active);
  for (const [id] of map) if (!activeSet.has(id) && !ids.includes(id)) map.delete(id);
  const used = new Set(map.values());
  for (const id of ids) {
    if (map.has(id)) continue;
    let slot = -1;
    for (let i = 0; i < SERIES.length; i++)
      if (!used.has(i)) {
        slot = i;
        break;
      }
    if (slot >= 0) {
      map.set(id, slot);
      used.add(slot);
    }
  }
  const out = new Map<string, string>();
  for (const id of ids) {
    const s = map.get(id);
    out.set(id, s === undefined ? SERIES_OTHER : SERIES[s]);
  }
  return out;
}
