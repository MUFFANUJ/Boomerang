import { useState, type ReactNode } from "react";
import type {
  AttemptOutcome,
  CircuitState,
  ComponentHealth,
  DeliveryState,
  EndpointStatus,
  WorkerStatus,
} from "../api/types";
import { ApiError, errorMessage } from "../api/client";
import { IconAlert, IconCheck, IconCopy, IconInfo, IconPause, IconSkull, IconX } from "./Icons";

/* ------------------------------------------------------------------ */
/* Layout primitives                                                   */
/* ------------------------------------------------------------------ */

export function Card({
  title,
  sub,
  actions,
  children,
  className = "",
  bodyClassName = "card-body",
  footer,
}: {
  title?: ReactNode;
  sub?: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
  className?: string;
  bodyClassName?: string;
  footer?: ReactNode;
}) {
  return (
    <section className={`card ${className}`}>
      {(title || actions) && (
        <div className="card-head">
          <div>
            {title && <h2>{title}</h2>}
            {sub && <div className="card-sub">{sub}</div>}
          </div>
          {actions && <div className="row row-wrap">{actions}</div>}
        </div>
      )}
      <div className={bodyClassName}>{children}</div>
      {footer && <div className="card-foot">{footer}</div>}
    </section>
  );
}

export function PageHead({ title, sub, actions }: { title: ReactNode; sub?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="page-head">
      <div>
        <h1>{title}</h1>
        {sub && <p>{sub}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </div>
  );
}

export function EmptyState({
  icon,
  title,
  children,
  compact,
}: {
  icon?: ReactNode;
  title: ReactNode;
  children?: ReactNode;
  compact?: boolean;
}) {
  return (
    <div className={`empty${compact ? " compact" : ""}`}>
      <div className="empty-icon">{icon ?? <IconInfo size={18} />}</div>
      <div className="empty-title">{title}</div>
      {children && <div>{children}</div>}
    </div>
  );
}

export function Loading({ label = "Loading…", compact }: { label?: string; compact?: boolean }) {
  return (
    <div className={`empty${compact ? " compact" : ""}`}>
      <div className="spinner spinner-lg" />
      <div>{label}</div>
    </div>
  );
}

export function Spinner() {
  return <span className="spinner" aria-label="Loading" />;
}

export function ErrorNote({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  if (!error) return null;
  const unreachable = error instanceof ApiError && (error.status === 0 || error.status >= 502);
  return (
    <div className="notice notice-critical" role="alert">
      <IconAlert />
      <div className="grow">
        <div>{errorMessage(error)}</div>
        {unreachable && (
          <div style={{ opacity: 0.8, marginTop: 2 }}>
            The gateway at localhost:8080 is not answering. The view will fill in once the backend is up.
          </div>
        )}
      </div>
      {onRetry && (
        <button className="btn btn-sm" onClick={onRetry}>
          Retry
        </button>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Status badges: always icon + label, never color alone               */
/* ------------------------------------------------------------------ */

export type Tone = "good" | "warning" | "serious" | "critical" | "info" | "neutral";

function toneIcon(tone: Tone): ReactNode {
  switch (tone) {
    case "good":
      return <IconCheck size={12} />;
    case "warning":
    case "serious":
      return <IconAlert size={12} />;
    case "critical":
      return <IconX size={12} />;
    case "info":
      return <span className="dot" style={{ background: "currentColor", width: 6, height: 6 }} />;
    default:
      return <span className="dot" style={{ background: "currentColor", width: 6, height: 6, opacity: 0.6 }} />;
  }
}

export function Badge({
  tone = "neutral",
  children,
  icon,
  small,
  title,
}: {
  tone?: Tone;
  children: ReactNode;
  icon?: ReactNode | false;
  small?: boolean;
  title?: string;
}) {
  return (
    <span className={`badge badge-${tone}${small ? " badge-sm" : ""}`} title={title}>
      {icon === false ? null : (icon ?? toneIcon(tone))}
      {children}
    </span>
  );
}

export function CircuitBadge({ state, small }: { state: CircuitState; small?: boolean }) {
  const map: Record<CircuitState, [Tone, string]> = {
    closed: ["good", "Circuit closed"],
    half_open: ["warning", "Half-open"],
    open: ["critical", "Circuit open"],
  };
  const [tone, label] = map[state] ?? ["neutral", state];
  return (
    <Badge tone={tone} small={small}>
      {label}
    </Badge>
  );
}

export function EndpointStatusBadge({ status, small }: { status: EndpointStatus; small?: boolean }) {
  const map: Record<EndpointStatus, [Tone, string, ReactNode?]> = {
    healthy: ["good", "Healthy"],
    degraded: ["warning", "Degraded"],
    failing: ["serious", "Failing"],
    circuit_open: ["critical", "Circuit open"],
    paused: ["neutral", "Paused", <IconPause size={12} key="p" />],
  };
  const [tone, label, icon] = map[status] ?? ["neutral", status];
  return (
    <Badge tone={tone} small={small} icon={icon}>
      {label}
    </Badge>
  );
}

export function DeliveryStateBadge({ state, small }: { state: DeliveryState; small?: boolean }) {
  const map: Record<DeliveryState, [Tone, string, ReactNode?]> = {
    delivered: ["good", "Delivered"],
    pending: ["info", "Pending"],
    retrying: ["warning", "Retrying"],
    dead: ["critical", "Dead", <IconSkull size={12} key="s" />],
    paused: ["neutral", "Paused", <IconPause size={12} key="p" />],
  };
  const [tone, label, icon] = map[state] ?? ["neutral", state];
  return (
    <Badge tone={tone} small={small} icon={icon}>
      {label}
    </Badge>
  );
}

export function OutcomeBadge({ outcome, small }: { outcome: AttemptOutcome; small?: boolean }) {
  const map: Record<AttemptOutcome, [Tone, string, ReactNode?]> = {
    success: ["good", "Success"],
    failure: ["critical", "Failure"],
    deferred: ["warning", "Deferred"],
    dead: ["critical", "Dead-lettered", <IconSkull size={12} key="s" />],
  };
  const [tone, label, icon] = map[outcome] ?? ["neutral", outcome];
  return (
    <Badge tone={tone} small={small} icon={icon}>
      {label}
    </Badge>
  );
}

export function ComponentStatusBadge({ status }: { status: ComponentHealth["status"] }) {
  const map: Record<ComponentHealth["status"], [Tone, string]> = {
    up: ["good", "Up"],
    degraded: ["warning", "Degraded"],
    down: ["critical", "Down"],
  };
  const [tone, label] = map[status] ?? ["neutral", status];
  return <Badge tone={tone}>{label}</Badge>;
}

export function WorkerStatusBadge({ status }: { status: WorkerStatus }) {
  const map: Record<WorkerStatus, [Tone, string]> = {
    running: ["good", "Running"],
    stopping: ["warning", "Stopping"],
    stale: ["critical", "Stale"],
  };
  const [tone, label] = map[status] ?? ["neutral", status];
  return <Badge tone={tone}>{label}</Badge>;
}

/** Tone for a 0..1 success rate. */
export function successTone(r: number | null | undefined): Tone {
  if (r === null || r === undefined) return "neutral";
  if (r >= 0.99) return "good";
  if (r >= 0.9) return "warning";
  return "critical";
}

/* ------------------------------------------------------------------ */
/* Controls                                                            */
/* ------------------------------------------------------------------ */

export function Toggle({
  checked,
  onChange,
  label,
  disabled,
  accent,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  label?: ReactNode;
  disabled?: boolean;
  accent?: boolean;
}) {
  return (
    <label className={`switch${accent ? " switch-accent" : ""}`}>
      <input type="checkbox" checked={checked} disabled={disabled} onChange={(e) => onChange(e.target.checked)} />
      <span className="switch-track" />
      {label && <span>{label}</span>}
    </label>
  );
}

export function Range({
  value,
  min,
  max,
  step = 1,
  onChange,
  onCommit,
  disabled,
  ariaLabel,
}: {
  value: number;
  min: number;
  max: number;
  step?: number;
  onChange: (v: number) => void;
  onCommit?: (v: number) => void;
  disabled?: boolean;
  ariaLabel?: string;
}) {
  const pct = max > min ? ((value - min) / (max - min)) * 100 : 0;
  return (
    <input
      type="range"
      className="range"
      min={min}
      max={max}
      step={step}
      value={value}
      disabled={disabled}
      aria-label={ariaLabel}
      style={{ ["--pct" as string]: `${pct}%` }}
      onChange={(e) => onChange(Number(e.target.value))}
      onPointerUp={(e) => onCommit?.(Number((e.target as HTMLInputElement).value))}
      onKeyUp={(e) => onCommit?.(Number((e.target as HTMLInputElement).value))}
    />
  );
}

export function Segmented<T extends string>({
  value,
  options,
  onChange,
}: {
  value: T;
  options: { value: T; label: ReactNode }[];
  onChange: (v: T) => void;
}) {
  return (
    <div className="segmented" role="tablist">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          role="tab"
          aria-selected={o.value === value}
          className={o.value === value ? "active" : ""}
          onClick={() => onChange(o.value)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    // Fallback for non-secure contexts.
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  }
}

export function CopyButton({ text, label, small = true }: { text: string; label?: string; small?: boolean }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      className={`btn ${small ? "btn-sm" : ""} ${label ? "" : "btn-icon"}`}
      title="Copy to clipboard"
      onClick={async () => {
        if (await copyText(text)) {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1400);
        }
      }}
    >
      {copied ? <IconCheck size={14} /> : <IconCopy size={14} />}
      {label && <span>{copied ? "Copied" : label}</span>}
    </button>
  );
}

/* ------------------------------------------------------------------ */
/* Figures                                                             */
/* ------------------------------------------------------------------ */

export interface LegendEntry {
  label: ReactNode;
  color: string;
  kind?: "line" | "rect" | "dot";
}

export function Legend({ items }: { items: LegendEntry[] }) {
  return (
    <div className="legend">
      {items.map((it, i) => (
        <span className="legend-item" key={i}>
          {it.kind === "rect" ? (
            <span className="legend-rect" style={{ background: it.color }} />
          ) : it.kind === "dot" ? (
            <span className="dot" style={{ background: it.color }} />
          ) : (
            <span className="legend-line" style={{ background: it.color }} />
          )}
          {it.label}
        </span>
      ))}
    </div>
  );
}

export function Meter({
  value,
  tone,
  label,
}: {
  value: number; // 0..1
  tone?: "good" | "warning" | "critical";
  label?: string;
}) {
  const v = Math.max(0, Math.min(1, value));
  return (
    <div
      className={`meter${tone ? ` tone-${tone}` : ""}`}
      role="meter"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(v * 100)}
      aria-label={label}
    >
      <span style={{ width: `${v * 100}%` }} />
    </div>
  );
}

export function SegBar({ parts, label }: { parts: { value: number; color: string; name: string }[]; label: string }) {
  const total = parts.reduce((a, p) => a + Math.max(0, p.value), 0);
  if (total <= 0) return <div className="segbar" aria-label={`${label}: no data`} style={{ background: "var(--surface-3)" }} />;
  return (
    <div className="segbar" role="img" aria-label={`${label}: ${parts.map((p) => `${p.name} ${p.value}`).join(", ")}`}>
      {parts
        .filter((p) => p.value > 0)
        .map((p) => (
          <span
            key={p.name}
            title={`${p.name}: ${p.value.toLocaleString()} (${((p.value / total) * 100).toFixed(1)}%)`}
            style={{ width: `${(p.value / total) * 100}%`, background: p.color }}
          />
        ))}
    </div>
  );
}
