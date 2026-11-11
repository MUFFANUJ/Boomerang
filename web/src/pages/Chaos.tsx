import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../api/client";
import type { Receiver, ReceiverChaos, StatsEndpoint } from "../api/types";
import { IconAlert, IconCheck, IconChaos, IconRefresh } from "../components/Icons";
import { Sparkline } from "../components/Sparkline";
import {
  Badge,
  Card,
  CircuitBadge,
  EmptyState,
  ErrorNote,
  Legend,
  Loading,
  PageHead,
  Range,
  Toggle,
  type Tone,
} from "../components/ui";
import { C } from "../lib/colors";
import { fmtMs, fmtNum, fmtPct, fmtRate } from "../lib/format";
import { useAction, useAsync } from "../lib/hooks";
import { useLiveSelector, type EndpointRatePoint } from "../lib/live";
import { useTenant } from "../lib/tenant";
import { useToast } from "../lib/toast";
import "./chaos.css";

const MODE_TONE: Record<Receiver["mode"], Tone> = {
  healthy: "good",
  slow: "warning",
  flaky: "serious",
  down: "critical",
};
const MODE_LABEL: Record<Receiver["mode"], string> = {
  healthy: "Healthy",
  slow: "Slow",
  flaky: "Flaky",
  down: "Down",
};

/** Sparkline window: last 2 minutes of the 5-minute rolling series. */
const SPARK_WINDOW_MS = 2 * 60 * 1000;
const EMPTY_SERIES: EndpointRatePoint[] = [];

/** Match a mock receiver to its endpoint in stats.endpoints (prefer the current tenant). */
function matchEndpoint(r: Receiver, endpoints: StatsEndpoint[], tenantId: string): StatsEndpoint | null {
  const name = r.name.toLowerCase();
  const display = r.display_name.toLowerCase();
  const candidates = endpoints.filter((e) => {
    const n = e.name.toLowerCase();
    return n === display || n.startsWith(name);
  });
  if (!candidates.length) return null;
  return candidates.find((e) => e.tenant_id === tenantId) ?? candidates[0];
}

export function ChaosPage() {
  const toast = useToast();
  const { tenantId } = useTenant();
  const chaos = useAsync((signal) => api.getChaos(signal), [], { pollMs: 2000 });
  const statsEndpoints = useLiveSelector((s) => s.stats?.endpoints);
  const endpointSeries = useLiveSelector((s) => s.endpointSeries);

  const [applyPreset, applying] = useAction(async (id: string, name: string) => {
    try {
      const r = await api.applyChaosPreset(id);
      chaos.setData((prev) => ({ presets: prev?.presets ?? [], receivers: r.receivers }));
      toast.success(`Preset applied: ${name}`);
    } catch (e) {
      toast.error(e);
    }
  });

  const [resetStats, resetting] = useAction(async () => {
    try {
      await api.resetChaosStats();
      chaos.reload();
      toast.success("Receiver stats reset");
    } catch (e) {
      toast.error(e);
    }
  });

  const onSaved = (rec: Receiver) =>
    chaos.setData((prev) =>
      prev
        ? { ...prev, receivers: prev.receivers.map((r) => (r.name === rec.name ? rec : r)) }
        : { presets: [], receivers: [rec] },
    );

  const data = chaos.data;

  return (
    <>
      <PageHead
        title="Chaos Playground"
        sub="Break the mock receivers on purpose and watch retries, circuit breakers and the DLQ respond."
        actions={
          <button className="btn" onClick={() => void resetStats()} disabled={resetting || !data}>
            <IconRefresh size={14} /> Reset stats
          </button>
        }
      />

      {chaos.error && !data ? <ErrorNote error={chaos.error} onRetry={chaos.reload} /> : null}

      {data && data.presets.length > 0 && (
        <Card title="Presets" sub="One click to set every receiver at once">
          <div className="preset-grid">
            {data.presets.map((p) => (
              <button
                key={p.id}
                className="preset"
                disabled={applying}
                onClick={() => void applyPreset(p.id, p.name)}
                title={p.description}
              >
                <span className="preset-name">
                  <IconChaos size={13} /> {p.name}
                </span>
                <span className="preset-desc">{p.description}</span>
              </button>
            ))}
          </div>
        </Card>
      )}

      {!data ? (
        chaos.loading ? (
          <Card>
            <Loading label="Loading receivers…" />
          </Card>
        ) : null
      ) : data.receivers.length === 0 ? (
        <Card>
          <EmptyState title="No mock receivers registered" icon={<IconChaos size={18} />}>
            /api/chaos returned an empty receiver list.
          </EmptyState>
        </Card>
      ) : (
        <div className="grid grid-3 chaos-grid">
          {data.receivers.map((r) => {
            const ep = statsEndpoints ? matchEndpoint(r, statsEndpoints, tenantId) : null;
            return (
              <ReceiverCard
                key={r.name}
                receiver={r}
                endpoint={ep}
                series={ep ? (endpointSeries[ep.id] ?? EMPTY_SERIES) : EMPTY_SERIES}
                onSaved={onSaved}
              />
            );
          })}
        </div>
      )}
      {chaos.error && data ? <ErrorNote error={chaos.error} /> : null}
    </>
  );
}

type SaveState = "idle" | "pending" | "saving" | "saved" | "error";

function ReceiverCard({
  receiver,
  endpoint,
  series,
  onSaved,
}: {
  receiver: Receiver;
  endpoint: StatsEndpoint | null;
  series: EndpointRatePoint[];
  onSaved: (r: Receiver) => void;
}) {
  const toast = useToast();
  const [draft, setDraft] = useState<ReceiverChaos>(receiver.chaos);
  const [saveState, setSaveState] = useState<SaveState>("idle");
  // While the user edits (and briefly after a save), polling must not clobber local values.
  const holdUntil = useRef(0);
  const timer = useRef<number | undefined>(undefined);
  const draftRef = useRef(draft);
  draftRef.current = draft;

  useEffect(() => {
    if (Date.now() < holdUntil.current) return;
    if (saveState === "pending" || saveState === "saving") return;
    const c = receiver.chaos;
    const d = draftRef.current;
    if (c.failure_rate !== d.failure_rate || c.delay_ms !== d.delay_ms || c.down !== d.down) setDraft(c);
    // Depend on the receiver object (new on every poll) so a value skipped during the hold
    // window is picked up by the next poll.
  }, [receiver]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => () => window.clearTimeout(timer.current), []);

  useEffect(() => {
    if (saveState !== "saved") return;
    const t = window.setTimeout(() => setSaveState("idle"), 2000);
    return () => window.clearTimeout(t);
  }, [saveState]);

  const save = async () => {
    setSaveState("saving");
    const body = draftRef.current;
    try {
      const rec = await api.putChaos(receiver.name, body);
      holdUntil.current = Date.now() + 2500;
      onSaved(rec);
      // Only mark saved if the user didn't edit again while the request was in flight.
      if (draftRef.current === body) setSaveState("saved");
    } catch (e) {
      setSaveState("error");
      toast.error(e);
    }
  };

  const edit = (patch: Partial<ReceiverChaos>, immediate = false) => {
    const next = { ...draftRef.current, ...patch };
    draftRef.current = next;
    setDraft(next);
    holdUntil.current = Date.now() + 5000;
    setSaveState("pending");
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => void save(), immediate ? 0 : 450);
  };

  const sparkData = useMemo(() => {
    if (!series.length) return series;
    const cutoff = series[series.length - 1].t - SPARK_WINDOW_MS;
    return series.filter((p) => p.t >= cutoff);
  }, [series]);

  const s = receiver.stats;

  return (
    <section className={`card receiver-card${draft.down ? " is-down" : ""}`}>
      <div className="card-head">
        <div>
          <h2>{receiver.display_name}</h2>
          <div className="card-sub mono">{receiver.url}</div>
        </div>
        <div className="row">
          <Badge tone={MODE_TONE[receiver.mode] ?? "neutral"}>{MODE_LABEL[receiver.mode] ?? receiver.mode}</Badge>
        </div>
      </div>

      <div className="card-body stack">
        <div className="receiver-endpoint">
          {endpoint ? (
            <>
              <span className="muted">{endpoint.name}</span>
              <CircuitBadge state={endpoint.circuit} small />
            </>
          ) : (
            <span className="muted">No matching endpoint in live stats</span>
          )}
        </div>

        <div className="chaos-control">
          <div className="row row-between">
            <label className="field-label" htmlFor={`fr-${receiver.name}`}>
              Failure rate
            </label>
            <span className="tnum chaos-value">{Math.round(draft.failure_rate * 100)}%</span>
          </div>
          <Range
            min={0}
            max={100}
            value={Math.round(draft.failure_rate * 100)}
            onChange={(v) => edit({ failure_rate: v / 100 })}
            ariaLabel={`${receiver.display_name} failure rate`}
          />
        </div>

        <div className="chaos-control">
          <div className="row row-between">
            <label className="field-label">Response delay</label>
            <span className="tnum chaos-value">{fmtMs(draft.delay_ms)}</span>
          </div>
          <Range
            min={0}
            max={10000}
            step={50}
            value={draft.delay_ms}
            onChange={(v) => edit({ delay_ms: v })}
            ariaLabel={`${receiver.display_name} response delay`}
          />
        </div>

        <div className="row row-between">
          <Toggle checked={draft.down} onChange={(v) => edit({ down: v }, true)} label="Receiver down" />
          <SaveIndicator state={saveState} onRetry={() => void save()} />
        </div>

        <hr className="divider" />

        <div className="receiver-live">
          <div className="row row-between">
            <span className="section-title" style={{ whiteSpace: "nowrap" }}>Rate · 2 min</span>
            <Legend
              items={[
                { label: "Delivered", color: C.accent },
                { label: "Failed", color: C.critical },
              ]}
            />
          </div>
          {endpoint ? (
            <>
              <Sparkline
                data={sparkData}
                series={[
                  { key: "delivered", label: "Delivered eps", color: C.accent },
                  { key: "failed", label: "Failed eps", color: C.critical },
                ]}
                height={48}
                format={(v) => `${fmtRate(v)} eps`}
              />
              <div className="receiver-rates">
                <span>
                  <strong>{fmtRate(endpoint.delivered_eps)}</strong> <span className="muted">delivered/s</span>
                </span>
                <span>
                  <strong>{fmtRate(endpoint.failed_eps)}</strong> <span className="muted">failed/s</span>
                </span>
                <span>
                  <strong>{fmtPct(endpoint.success_rate)}</strong> <span className="muted">success</span>
                </span>
              </div>
            </>
          ) : (
            <div className="muted receiver-nolive">Waiting for live endpoint stats…</div>
          )}
        </div>

        <div className="receiver-stats">
          <Stat label="Received" value={s.received} />
          <Stat label="Unique" value={s.unique} />
          <Stat label="Duplicates" value={s.duplicates} />
          <Stat label="Invalid signatures" value={s.invalid_signatures} alert={s.invalid_signatures > 0} />
          <Stat label="Injected failures" value={s.injected_failures} />
        </div>
      </div>
    </section>
  );
}

function Stat({ label, value, alert }: { label: string; value: number; alert?: boolean }) {
  return (
    <div className="rstat">
      <div className="rstat-label">
        {alert && <IconAlert size={11} style={{ color: "var(--critical-ink)" }} />}
        {label}
      </div>
      <div className="rstat-value">{fmtNum(value)}</div>
    </div>
  );
}

function SaveIndicator({ state, onRetry }: { state: SaveState; onRetry: () => void }) {
  if (state === "pending" || state === "saving")
    return (
      <span className="save-ind muted">
        <span className="spinner" style={{ width: 12, height: 12 }} /> Saving…
      </span>
    );
  if (state === "saved")
    return (
      <span className="save-ind" style={{ color: "var(--good-ink)" }}>
        <IconCheck size={13} /> Saved
      </span>
    );
  if (state === "error")
    return (
      <button className="btn btn-sm btn-danger" onClick={onRetry}>
        Save failed · retry
      </button>
    );
  return <span className="save-ind muted">Changes apply instantly</span>;
}
