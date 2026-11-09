// Number, time and duration formatting helpers.

const compactFmt = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });
const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/** 1,284 / 12.9K / 4.2M — full under 10,000, compact above. */
export function fmtNum(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  if (Math.abs(n) < 10_000) return intFmt.format(n);
  return compactFmt.format(n);
}

/** Always full digits with separators. */
export function fmtInt(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  return intFmt.format(n);
}

/** Rates: one decimal under 10, integer above, compact above 10k. */
export function fmtRate(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  if (Math.abs(n) < 10) return n.toFixed(n === 0 ? 0 : 1);
  return fmtNum(Math.round(n));
}

/** 0..1 ratio to percent. */
export function fmtPct(r: number | null | undefined, digits = 1): string {
  if (r === null || r === undefined || !Number.isFinite(r)) return "—";
  const p = r * 100;
  if (p === 100 || p === 0) return `${p.toFixed(0)}%`;
  return `${p.toFixed(digits)}%`;
}

/** Latency: "84 ms", "1.24 s". */
export function fmtMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "—";
  if (ms === 0) return "0 ms";
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(2)} s`;
  return fmtDuration(ms);
}

/** Human duration for delays: "500 ms", "30 s", "5 min", "1 h 30 min", "2 d". */
export function fmtDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${Number.isInteger(s) ? s : s.toFixed(1)} s`;
  const m = s / 60;
  if (m < 60) {
    const whole = Math.floor(m);
    const rem = Math.round(s - whole * 60);
    return rem ? `${whole} min ${rem} s` : `${whole} min`;
  }
  const h = m / 60;
  if (h < 24) {
    const whole = Math.floor(h);
    const rem = Math.round(m - whole * 60);
    return rem ? `${whole} h ${rem} min` : `${whole} h`;
  }
  const d = h / 24;
  const whole = Math.floor(d);
  const rem = Math.round(h - whole * 24);
  return rem ? `${whole} d ${rem} h` : `${whole} d`;
}

export function fmtBytes(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}

export function parseTs(ts: string | null | undefined): number | null {
  if (!ts) return null;
  const t = Date.parse(ts);
  return Number.isNaN(t) ? null : t;
}

/** "3s ago", "5m ago", "2h ago", "in 12s". */
export function fmtRelative(ts: string | number | null | undefined, now = Date.now()): string {
  const t = typeof ts === "number" ? ts : parseTs(ts ?? null);
  if (t === null) return "—";
  const diff = now - t;
  const abs = Math.abs(diff);
  let out: string;
  if (abs < 1500) return diff >= 0 ? "just now" : "now";
  if (abs < 60_000) out = `${Math.round(abs / 1000)}s`;
  else if (abs < 3_600_000) out = `${Math.round(abs / 60_000)}m`;
  else if (abs < 86_400_000) out = `${Math.round(abs / 3_600_000)}h`;
  else out = `${Math.round(abs / 86_400_000)}d`;
  return diff >= 0 ? `${out} ago` : `in ${out}`;
}

const dtFmt = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});
const timeFmt = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });

export function fmtDateTime(ts: string | number | null | undefined): string {
  const t = typeof ts === "number" ? ts : parseTs(ts ?? null);
  if (t === null) return "—";
  return dtFmt.format(t);
}

export function fmtTime(ts: string | number | null | undefined): string {
  const t = typeof ts === "number" ? ts : parseTs(ts ?? null);
  if (t === null) return "—";
  return timeFmt.format(t);
}

/** Precise timestamp with milliseconds, for attempt timelines. */
export function fmtTimePrecise(ts: string | null | undefined): string {
  const t = parseTs(ts);
  if (t === null) return "—";
  const d = new Date(t);
  return `${timeFmt.format(t)}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}

export function shortId(id: string, keep = 10): string {
  if (id.length <= keep + 4) return id;
  return `${id.slice(0, keep)}…`;
}

/** Axis tick formatter: full under 10k, compact above. */
export function fmtAxis(n: number): string {
  if (Math.abs(n) < 10_000) return intFmt.format(n);
  return compactFmt.format(n);
}
