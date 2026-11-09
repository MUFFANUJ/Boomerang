// Event detail: payload, keys, per-delivery status, and a vertical attempt timeline per delivery.
// Used by the Event Explorer and by the Live Flow drawer (compact).
import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { Attempt, DeliverySummary } from "../api/types";
import { useAsync, useNow } from "../lib/hooks";
import { useToast } from "../lib/toast";
import { fmtBytes, fmtDateTime, fmtDuration, fmtMs, fmtRelative, fmtTimePrecise, parseTs } from "../lib/format";
import { IconExternal, IconReplay, IconSkull } from "./Icons";
import { JsonView } from "./JsonView";
import { Badge, CopyButton, DeliveryStateBadge, EmptyState, ErrorNote, Loading, OutcomeBadge } from "./ui";
import "./event-detail.css";

export function EventDetail({
  tenantId,
  eventId,
  compact = false,
}: {
  tenantId: string;
  eventId: string;
  compact?: boolean;
}) {
  const toast = useToast();
  const { data, error, loading, reload } = useAsync(
    (signal) => api.getEvent(tenantId, eventId, signal),
    [tenantId, eventId],
  );
  const [busy, setBusy] = useState<string | null>(null);

  const attemptsByDelivery = useMemo(() => {
    const m = new Map<string, Attempt[]>();
    for (const a of data?.attempts ?? []) {
      const arr = m.get(a.delivery_id);
      if (arr) arr.push(a);
      else m.set(a.delivery_id, [a]);
    }
    return m;
  }, [data]);

  const failedCount = (data?.deliveries ?? []).filter((d) => d.state !== "delivered").length;

  const replayAll = async () => {
    setBusy("all");
    try {
      const r = await api.replayEvent(tenantId, eventId);
      toast.success(`Replayed ${r.replayed} ${r.replayed === 1 ? "delivery" : "deliveries"}`);
      reload();
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(null);
    }
  };

  const replayOne = async (d: DeliverySummary) => {
    setBusy(d.delivery_id);
    try {
      await api.replayDelivery(tenantId, d.delivery_id);
      toast.success(`Replay queued for ${d.endpoint_name}`);
      reload();
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(null);
    }
  };

  if (error && !data) return <ErrorNote error={error} onRetry={reload} />;
  if (!data) return <Loading label="Loading event…" />;

  const ev = data.event;
  return (
    <div className={`evd${compact ? " evd-compact" : ""}${loading ? " is-stale" : ""}`}>
      <div className="evd-head">
        <div className="grow">
          <div className="row row-wrap" style={{ gap: 10 }}>
            <span className="chip evd-type">{ev.event_type}</span>
            <code className="evd-id">{ev.event_id}</code>
            <CopyButton text={ev.event_id} />
          </div>
          <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>
            Created {fmtDateTime(ev.created_at)} ({fmtRelative(ev.created_at)}) · tenant <code>{ev.tenant_id}</code>
          </div>
        </div>
        <div className="row row-wrap">
          {compact && (
            <Link className="btn btn-sm" to={`/events/${encodeURIComponent(ev.event_id)}`}>
              <IconExternal size={14} /> Open in Explorer
            </Link>
          )}
          <button className="btn btn-sm" onClick={reload} disabled={loading}>
            Refresh
          </button>
          <button
            className="btn btn-sm btn-primary"
            onClick={replayAll}
            disabled={failedCount === 0 || busy !== null}
            title={failedCount === 0 ? "Every delivery succeeded" : "Replay every delivery that is not delivered"}
          >
            <IconReplay size={14} /> Replay all failed{failedCount ? ` (${failedCount})` : ""}
          </button>
        </div>
      </div>

      <div className={compact ? "stack" : "evd-grid"}>
        <section className="card card-pad evd-meta">
          <div className="section-title" style={{ marginBottom: 10 }}>
            Event
          </div>
          <dl className="kv">
            <dt>Idempotency key</dt>
            <dd>{ev.idempotency_key ? <code>{ev.idempotency_key}</code> : <span className="muted">none</span>}</dd>
            <dt>Ordering key</dt>
            <dd>{ev.ordering_key ? <code>{ev.ordering_key}</code> : <span className="muted">none</span>}</dd>
            <dt>Payload size</dt>
            <dd>{fmtBytes(ev.payload_bytes)}</dd>
            <dt>Payload ref</dt>
            <dd>
              {ev.payload_ref ? (
                <span className="row">
                  <code style={{ overflowWrap: "anywhere" }}>{ev.payload_ref}</code>
                  <span className="badge badge-sm badge-info">offloaded</span>
                </span>
              ) : (
                <span className="muted">inline (not offloaded)</span>
              )}
            </dd>
          </dl>
          <div className="section-title" style={{ margin: "16px 0 8px" }}>
            Payload
          </div>
          <JsonView value={ev.payload} maxHeight={compact ? 260 : 460} />
        </section>

        <section className="stack">
          <div className="section-title">
            Deliveries <span className="muted">({data.deliveries.length})</span>
          </div>
          {data.deliveries.length === 0 && (
            <div className="card">
              <EmptyState compact title="No deliveries">
                No endpoint of this tenant was subscribed to <code>{ev.event_type}</code> when it arrived.
              </EmptyState>
            </div>
          )}
          {data.deliveries.map((d) => (
            <DeliveryBlock
              key={d.delivery_id}
              delivery={d}
              attempts={attemptsByDelivery.get(d.delivery_id) ?? []}
              busy={busy === d.delivery_id}
              disabled={busy !== null}
              onReplay={() => replayOne(d)}
            />
          ))}
        </section>
      </div>
    </div>
  );
}

function DeliveryBlock({
  delivery,
  attempts,
  busy,
  disabled,
  onReplay,
}: {
  delivery: DeliverySummary;
  attempts: Attempt[];
  busy: boolean;
  disabled: boolean;
  onReplay: () => void;
}) {
  return (
    <div className="card evd-delivery">
      <div className="evd-delivery-head">
        <div className="grow">
          <div className="row row-wrap" style={{ gap: 8 }}>
            <strong>{delivery.endpoint_name}</strong>
            <DeliveryStateBadge state={delivery.state} small />
          </div>
          <div className="muted" style={{ fontSize: 12, marginTop: 3 }}>
            <code>{delivery.delivery_id}</code> · {delivery.attempts} attempt{delivery.attempts === 1 ? "" : "s"} ·
            updated {fmtRelative(delivery.updated_at)}
          </div>
        </div>
        <button
          className="btn btn-sm"
          onClick={onReplay}
          disabled={disabled}
          title="Re-enqueue this delivery (receivers dedupe by Boomerang-Id)"
        >
          {busy ? <span className="spinner" /> : <IconReplay size={14} />} Replay
        </button>
      </div>
      {attempts.length === 0 ? (
        <div className="muted evd-noattempts">No attempts recorded yet.</div>
      ) : (
        <ol className="timeline">
          {groupAttempts(attempts).map((g) =>
            g.items.length === 1 ? (
              <AttemptItem key={g.key} a={g.items[0]} />
            ) : (
              <DeferredGroup key={g.key} items={g.items} />
            ),
          )}
        </ol>
      )}
    </div>
  );
}

interface AttemptGroup {
  key: string;
  items: Attempt[];
}

/** Collapse runs of consecutive deferred attempts that share a reason (e.g. a circuit-open hold). */
function groupAttempts(attempts: Attempt[]): AttemptGroup[] {
  const out: AttemptGroup[] = [];
  attempts.forEach((a, i) => {
    const prev = out[out.length - 1];
    const last = prev?.items[prev.items.length - 1];
    if (prev && last && a.outcome === "deferred" && last.outcome === "deferred" && a.reason === last.reason) {
      prev.items.push(a);
    } else {
      out.push({ key: `${a.attempt}-${a.ts}-${i}`, items: [a] });
    }
  });
  return out;
}

function DeferredGroup({ items }: { items: Attempt[] }) {
  const [open, setOpen] = useState(false);
  const first = items[0];
  const last = items[items.length - 1];
  return (
    <li className="tl-item tl-warning">
      <span className="tl-marker tl-marker-group" aria-hidden="true">
        {items.length > 99 ? "99+" : items.length}
      </span>
      <div className="tl-body">
        <div className="row" style={{ gap: 8 }}>
          <Badge tone="warning" small>
            Deferred ×{items.length}
          </Badge>
          <span className="tl-group-reason" title={first.reason}>
            {first.reason || "No reason given"}
          </span>
          <span className="muted tnum" style={{ fontSize: 12, whiteSpace: "nowrap" }} title={`${first.ts} → ${last.ts}`}>
            {fmtTimePrecise(first.ts)} → {fmtTimePrecise(last.ts)}
          </span>
        </div>
        <div className="tl-lines muted" style={{ fontSize: 12 }}>
          {first.attempt === last.attempt
            ? `Attempt ${first.attempt} held, never sent`
            : `Attempts ${first.attempt}–${last.attempt} held, never sent`}
          {" · "}
          {fmtDuration((parseTs(last.ts) ?? 0) - (parseTs(first.ts) ?? 0))} on hold
        </div>
        {last.next_retry_at && (
          <div className="tl-lines">
            <span className="muted">Last next retry</span> {fmtTimePrecise(last.next_retry_at)}
          </div>
        )}
        <div>
          <button type="button" className="btn btn-sm btn-ghost tl-expand" onClick={() => setOpen((o) => !o)} aria-expanded={open}>
            {open ? "Hide" : "Show"} {items.length} individual attempts
          </button>
        </div>
        {open && (
          <ol className="timeline tl-nested">
            {items.map((a, i) => (
              <AttemptItem key={`${a.attempt}-${a.ts}-${i}`} a={a} />
            ))}
          </ol>
        )}
      </div>
    </li>
  );
}

function AttemptItem({ a }: { a: Attempt }) {
  const now = useNow(a.next_retry_at ? 1000 : 60_000);
  const retryAt = parseTs(a.next_retry_at);
  const tone =
    a.outcome === "success" ? "good" : a.outcome === "deferred" ? "warning" : "critical";
  return (
    <li className={`tl-item tl-${tone}`}>
      <span className="tl-marker" aria-hidden="true">
        {a.outcome === "dead" ? <IconSkull size={10} /> : null}
      </span>
      <div className="tl-body">
        <div className="row row-wrap" style={{ gap: 8 }}>
          <strong>Attempt {a.attempt}</strong>
          <OutcomeBadge outcome={a.outcome} small />
          <span className={`tl-status mono ${a.status_code >= 200 && a.status_code < 300 ? "ok" : ""}`}>
            {a.status_code === 0 ? "no response" : `HTTP ${a.status_code}`}
          </span>
          <span className="muted tnum">{fmtMs(a.latency_ms)}</span>
          <span className="grow" />
          <span className="muted tnum" title={a.ts} style={{ fontSize: 12 }}>
            {fmtTimePrecise(a.ts)}
          </span>
        </div>
        {(a.error || a.reason) && (
          <div className="tl-lines">
            {a.error && (
              <div>
                <span className="muted">Error</span> <span className="tl-error">{a.error}</span>
              </div>
            )}
            {a.reason && (
              <div>
                <span className="muted">Reason</span> {a.reason}
              </div>
            )}
          </div>
        )}
        {a.next_retry_at && (
          <div className="tl-lines">
            <span className="muted">Next retry</span> {fmtTimePrecise(a.next_retry_at)}
            {retryAt !== null && (
              <span className="muted"> ({retryAt > now ? fmtRelative(retryAt, now) : "due"})</span>
            )}
          </div>
        )}
        <div className="tl-lines muted" style={{ fontSize: 12 }}>
          Worker <code>{a.worker_id || "—"}</code>
        </div>
        <div className="row row-wrap" style={{ gap: 12, marginTop: 4 }}>
          <details className="collapsible">
            <summary>Response body{a.response_body ? ` (${a.response_body.length} B)` : " (empty)"}</summary>
            {a.response_body ? (
              <pre className="code wrap" style={{ maxHeight: 220 }}>
                {a.response_body}
              </pre>
            ) : (
              <div className="muted" style={{ fontSize: 12 }}>
                No body was returned.
              </div>
            )}
          </details>
          <details className="collapsible">
            <summary>Signature header</summary>
            {a.signature ? (
              <div className="secret-box">
                <code title={a.signature}>Boomerang-Signature: {a.signature}</code>
                <CopyButton text={a.signature} />
              </div>
            ) : (
              <div className="muted" style={{ fontSize: 12 }}>
                No request was sent on this attempt.
              </div>
            )}
          </details>
        </div>
      </div>
    </li>
  );
}

