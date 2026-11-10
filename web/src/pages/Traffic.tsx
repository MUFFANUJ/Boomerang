import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api, errorMessage } from "../api/client";
import type { SimSingleResponse, SimStatus } from "../api/types";
import { ChartTooltip } from "../components/ChartTooltip";
import { IconPlay, IconStop, IconBolt, IconTraffic } from "../components/Icons";
import {
  Badge,
  Card,
  EmptyState,
  ErrorNote,
  Legend,
  Meter,
  PageHead,
  Range,
  type Tone,
} from "../components/ui";
import { C } from "../lib/colors";
import { fmtAxis, fmtInt, fmtMs, fmtNum, fmtRate, fmtRelative } from "../lib/format";
import { useAction, useAsync, useNow } from "../lib/hooks";
import { useLiveSelector } from "../lib/live";
import { useToast } from "../lib/toast";
import "./traffic.css";

const MIN_EPS = 1;
const MAX_EPS = 2000;
const SLIDER_STEPS = 1000;
const BURST_PRESETS = [100, 1_000, 10_000, 50_000];

/** Log-scale mapping so 1–20 eps gets as much travel as 200–2000. */
function posToEps(pos: number): number {
  const lnMin = Math.log(MIN_EPS);
  const lnMax = Math.log(MAX_EPS);
  const v = Math.exp(lnMin + (pos / SLIDER_STEPS) * (lnMax - lnMin));
  if (v < 20) return Math.round(v);
  if (v < 200) return Math.round(v / 5) * 5;
  return Math.round(v / 10) * 10;
}
function epsToPos(eps: number): number {
  const lnMin = Math.log(MIN_EPS);
  const lnMax = Math.log(MAX_EPS);
  const clamped = Math.min(MAX_EPS, Math.max(MIN_EPS, eps));
  return Math.round(((Math.log(clamped) - lnMin) / (lnMax - lnMin)) * SLIDER_STEPS);
}
function clampEps(n: number): number {
  if (!Number.isFinite(n)) return MIN_EPS;
  return Math.min(MAX_EPS, Math.max(MIN_EPS, Math.round(n)));
}

const MODE_LABEL: Record<SimStatus["mode"], string> = {
  idle: "Idle",
  stream: "Steady stream",
  burst: "Burst",
  loadtest: "Load test",
};

export function TrafficPage() {
  const toast = useToast();
  const stats = useLiveSelector((s) => s.stats);
  const wsStatus = useLiveSelector((s) => s.status);

  const typesQ = useAsync((signal) => api.listEventTypes(signal), []);
  const eventTypes = typesQ.data ?? [];

  // Sim status: WS stats.sim is the live source; GET /api/sim/status and POST responses fill gaps.
  const initial = useAsync((signal) => api.simStatus(signal), []);
  const [sim, setSim] = useState<SimStatus | null>(null);
  useEffect(() => {
    if (initial.data && !sim) setSim(initial.data);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initial.data]);
  useEffect(() => {
    if (stats?.sim) setSim(stats.sim);
  }, [stats]);
  const simUnreachable = (stats !== null && stats.sim === null && !sim) || (!!initial.error && !sim);

  // Event-type selection, default: all known types.
  const [selected, setSelected] = useState<string[] | null>(null);
  useEffect(() => {
    if (selected === null && eventTypes.length) setSelected(eventTypes);
  }, [eventTypes, selected]);
  const chosen = selected ?? [];
  const toggleType = (t: string) =>
    setSelected((prev) => {
      const cur = prev ?? [];
      return cur.includes(t) ? cur.filter((x) => x !== t) : [...cur, t];
    });

  // Single event
  const [singleType, setSingleType] = useState<string>("");
  useEffect(() => {
    if (!singleType && eventTypes.length) setSingleType(eventTypes[0]);
  }, [eventTypes, singleType]);
  const [lastSingle, setLastSingle] = useState<(SimSingleResponse & { at: number }) | null>(null);
  const [sendSingle, sendingSingle] = useAction(async () => {
    try {
      const r = await api.simSingle(singleType);
      setLastSingle({ ...r, at: Date.now() });
    } catch (e) {
      toast.error(e);
    }
  });

  // Stream
  const [rate, setRate] = useState<number>(50);
  const [rateText, setRateText] = useState<string>("50");
  const setRateBoth = (n: number) => {
    const v = clampEps(n);
    setRate(v);
    setRateText(String(v));
  };
  const [startStream, startingStream] = useAction(async () => {
    try {
      const s = await api.simStream({ rate_eps: rate, event_types: chosen });
      setSim(s);
      toast.success(`Streaming ${fmtInt(rate)} eps`);
    } catch (e) {
      toast.error(e);
    }
  });

  // Burst
  const [fireBurst, firingBurst] = useAction(async (count: number) => {
    try {
      const s = await api.simBurst({ count, event_types: chosen });
      setSim(s);
      toast.success(`Burst of ${fmtInt(count)} events started`);
    } catch (e) {
      toast.error(e);
    }
  });

  // Load test
  const [lt, setLt] = useState({ start: "100", end: "2000", duration: "60" });
  const ltValid =
    Number(lt.start) >= 1 && Number(lt.end) >= 1 && Number(lt.duration) >= 5 && Number(lt.duration) <= 3600;
  const [startLoadTest, startingLt] = useAction(async () => {
    try {
      const s = await api.simLoadTest({
        start_eps: Math.round(Number(lt.start)),
        end_eps: Math.round(Number(lt.end)),
        duration_s: Math.round(Number(lt.duration)),
        event_types: chosen,
      });
      setSim(s);
      toast.success("Load test started");
    } catch (e) {
      toast.error(e);
    }
  });

  // Stop
  const [stop, stopping] = useAction(async () => {
    try {
      const s = await api.simStop();
      setSim(s);
      toast.info("ShopSim stopped");
    } catch (e) {
      toast.error(e);
    }
  });

  const running = !!sim?.running;
  const noTypes = chosen.length === 0;

  return (
    <>
      <PageHead
        title="Traffic Control"
        sub="Drive ShopSim, the demo storefront, to generate real webhook traffic into the ingestion API."
        actions={
          <button className="btn btn-danger" onClick={() => void stop()} disabled={stopping || !running}>
            <IconStop size={14} /> Stop ShopSim
          </button>
        }
      />

      {typesQ.error ? <ErrorNote error={typesQ.error} onRetry={typesQ.reload} /> : null}

      <div className="traffic-layout">
        <div className="stack">
          {/* Event types shared by stream / burst / load test */}
          <Card
            title="Event types"
            sub="Used by stream, burst and load test"
            actions={
              eventTypes.length > 0 && (
                <div className="row">
                  <button className="btn btn-ghost btn-sm" onClick={() => setSelected(eventTypes)}>
                    All
                  </button>
                  <button className="btn btn-ghost btn-sm" onClick={() => setSelected([])}>
                    None
                  </button>
                </div>
              )
            }
          >
            {typesQ.loading && !typesQ.data ? (
              <div className="muted">Loading event types…</div>
            ) : eventTypes.length === 0 ? (
              <div className="muted">No event types reported by /api/event-types.</div>
            ) : (
              <div className="type-checks">
                {eventTypes.map((t) => (
                  <label className="checkbox type-check" key={t}>
                    <input type="checkbox" checked={chosen.includes(t)} onChange={() => toggleType(t)} />
                    <span className="mono">{t}</span>
                  </label>
                ))}
              </div>
            )}
            {noTypes && eventTypes.length > 0 && (
              <div className="field-error" style={{ marginTop: 8 }}>
                Select at least one event type.
              </div>
            )}
          </Card>

          <div className="grid grid-2">
            <Card title="Single event" sub="Send one event and follow it">
              <div className="stack">
                <div className="row">
                  <select
                    className="select grow"
                    value={singleType}
                    onChange={(e) => setSingleType(e.target.value)}
                    aria-label="Event type"
                    disabled={!eventTypes.length}
                  >
                    {eventTypes.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                  <button
                    className="btn btn-primary"
                    onClick={() => void sendSingle()}
                    disabled={!singleType || sendingSingle}
                  >
                    Send
                  </button>
                </div>
                {lastSingle ? (
                  <div className="single-result">
                    <span className="muted">Accepted</span>
                    <Link to={`/events/${encodeURIComponent(lastSingle.id)}`} className="mono">
                      {lastSingle.id}
                    </Link>
                    <span className="muted">· {lastSingle.type}</span>
                  </div>
                ) : (
                  <div className="muted single-result">The returned event id will appear here.</div>
                )}
              </div>
            </Card>

            <Card title="Burst" sub="Fire N events as fast as possible">
              <div className="burst-grid">
                {BURST_PRESETS.map((n) => (
                  <button
                    key={n}
                    className="btn"
                    disabled={firingBurst || noTypes}
                    onClick={() => void fireBurst(n)}
                  >
                    <IconBolt size={13} /> {fmtInt(n)}
                  </button>
                ))}
              </div>
            </Card>
          </div>

          <Card title="Steady stream" sub="Constant rate, log-scale slider from 1 to 2,000 events/s">
            <div className="stack">
              <div className="stream-row">
                <Range
                  min={0}
                  max={SLIDER_STEPS}
                  value={epsToPos(rate)}
                  onChange={(p) => setRateBoth(posToEps(p))}
                  ariaLabel="Stream rate (events per second)"
                />
                <div className="input-group stream-input">
                  <input
                    className="input"
                    type="number"
                    min={MIN_EPS}
                    max={MAX_EPS}
                    value={rateText}
                    onChange={(e) => setRateText(e.target.value)}
                    onBlur={() => setRateBoth(Number(rateText))}
                    onKeyDown={(e) => e.key === "Enter" && setRateBoth(Number(rateText))}
                    aria-label="Rate in events per second"
                  />
                  <span className="input-suffix">eps</span>
                </div>
              </div>
              <div className="scale-ticks muted">
                {[1, 10, 100, 1000, 2000].map((t) => (
                  <span key={t} style={{ left: `${(epsToPos(t) / SLIDER_STEPS) * 100}%` }}>
                    {fmtInt(t)}
                  </span>
                ))}
              </div>
              <div className="row">
                <button
                  className="btn btn-primary"
                  onClick={() => void startStream()}
                  disabled={startingStream || noTypes}
                >
                  <IconPlay size={13} />
                  {sim?.mode === "stream" && running ? "Update rate" : "Start stream"}
                </button>
                {sim?.mode === "stream" && running && (
                  <span className="muted">Currently targeting {fmtInt(sim.rate_eps)} eps</span>
                )}
              </div>
            </div>
          </Card>

          <Card title="Load test" sub="Linear ramp from start to end rate; ShopSim records each step">
            <div className="stack">
              <div className="grid grid-3">
                <div className="field">
                  <label htmlFor="lt-start">Start rate</label>
                  <div className="input-group">
                    <input
                      id="lt-start"
                      className="input"
                      type="number"
                      min={1}
                      value={lt.start}
                      onChange={(e) => setLt({ ...lt, start: e.target.value })}
                    />
                    <span className="input-suffix">eps</span>
                  </div>
                </div>
                <div className="field">
                  <label htmlFor="lt-end">End rate</label>
                  <div className="input-group">
                    <input
                      id="lt-end"
                      className="input"
                      type="number"
                      min={1}
                      value={lt.end}
                      onChange={(e) => setLt({ ...lt, end: e.target.value })}
                    />
                    <span className="input-suffix">eps</span>
                  </div>
                </div>
                <div className="field">
                  <label htmlFor="lt-dur">Duration</label>
                  <div className="input-group">
                    <input
                      id="lt-dur"
                      className="input"
                      type="number"
                      min={5}
                      max={3600}
                      value={lt.duration}
                      onChange={(e) => setLt({ ...lt, duration: e.target.value })}
                    />
                    <span className="input-suffix">s</span>
                  </div>
                </div>
              </div>
              <div className="row">
                <button
                  className="btn btn-primary"
                  onClick={() => void startLoadTest()}
                  disabled={startingLt || !ltValid || noTypes}
                >
                  <IconTraffic size={14} /> Start load test
                </button>
                {!ltValid && <span className="field-error">Rates must be ≥ 1 and duration 5–3600 s.</span>}
              </div>
            </div>
          </Card>
        </div>

        <div className="stack">
          <SimFeedback
            sim={sim}
            unreachable={simUnreachable}
            loading={!sim && (initial.loading || (wsStatus !== "open" && !stats))}
            error={initial.error}
            onStop={() => void stop()}
            stopping={stopping}
          />
        </div>
      </div>

      {sim?.loadtest && <LoadTestCharts lt={sim.loadtest} running={running && sim.mode === "loadtest"} />}
    </>
  );
}

function modeTone(sim: SimStatus): Tone {
  if (!sim.running) return "neutral";
  return "info";
}

function SimFeedback({
  sim,
  unreachable,
  loading,
  error,
  onStop,
  stopping,
}: {
  sim: SimStatus | null;
  unreachable: boolean;
  loading: boolean;
  error: unknown;
  onStop: () => void;
  stopping: boolean;
}) {
  const now = useNow(1000);
  if (!sim) {
    return (
      <Card title="Live feedback">
        {loading ? (
          <EmptyState compact title="Waiting for ShopSim status…" icon={<span className="spinner" />} />
        ) : unreachable ? (
          <EmptyState compact title="ShopSim is not reporting" icon={<IconTraffic size={18} />}>
            The realtime stream has no <code>sim</code> block and <code>/api/sim/status</code>{" "}
            {error ? `failed (${errorMessage(error)})` : "returned nothing"}. Start the shopsim service to
            generate traffic.
          </EmptyState>
        ) : (
          <EmptyState compact title="No ShopSim status yet" />
        )}
      </Card>
    );
  }

  const hasTarget = (sim.mode === "stream" || sim.mode === "loadtest") && sim.rate_eps > 0;
  const ratio = hasTarget ? sim.current_eps / sim.rate_eps : 0;
  const meterTone = !hasTarget || !sim.running ? undefined : ratio >= 0.9 ? "good" : ratio >= 0.6 ? "warning" : "critical";
  const burstPct = sim.burst && sim.burst.total > 0 ? sim.burst.done / sim.burst.total : 0;

  return (
    <Card
      title="Live feedback"
      sub="From the realtime stats stream (stats.sim)"
      actions={
        <Badge tone={modeTone(sim)}>
          {MODE_LABEL[sim.mode] ?? sim.mode}
          {sim.running ? " · running" : ""}
        </Badge>
      }
    >
      <div className="stack">
        <div>
          <div className="feedback-eps">
            <span className="feedback-eps-value">{fmtRate(sim.current_eps)}</span>
            <span className="muted">
              eps{hasTarget ? ` of ${fmtInt(sim.rate_eps)} target` : " measured over the last second"}
            </span>
          </div>
          {hasTarget && (
            <Meter
              value={ratio}
              tone={meterTone}
              label={`Current ${fmtRate(sim.current_eps)} of target ${fmtInt(sim.rate_eps)} eps`}
            />
          )}
        </div>

        {sim.burst && (
          <div className="stack-sm">
            <div className="row row-between">
              <span className="secondary">Burst progress</span>
              <span className="tnum">
                {fmtInt(sim.burst.done)} / {fmtInt(sim.burst.total)} · {(burstPct * 100).toFixed(0)}%
              </span>
            </div>
            <Meter value={burstPct} label="Burst progress" tone={burstPct >= 1 ? "good" : undefined} />
          </div>
        )}

        <div className="counter-grid">
          <Counter label="Sent" value={sim.sent} />
          <Counter label="Accepted" value={sim.accepted} />
          <Counter label="Duplicates" value={sim.duplicates} />
          <Counter label="Errors" value={sim.errors} tone={sim.errors > 0 ? "critical" : undefined} />
          <Counter label="429 rate limited" value={sim.rate_limited} tone={sim.rate_limited > 0 ? "warning" : undefined} />
        </div>

        <div>
          <div className="section-title" style={{ marginBottom: 6 }}>
            Ingestion latency (seen by ShopSim)
          </div>
          <div className="lat-grid">
            <div>
              <span className="muted">p50</span>
              <strong>{fmtMs(sim.latency_ms.p50)}</strong>
            </div>
            <div>
              <span className="muted">p95</span>
              <strong>{fmtMs(sim.latency_ms.p95)}</strong>
            </div>
            <div>
              <span className="muted">p99</span>
              <strong>{fmtMs(sim.latency_ms.p99)}</strong>
            </div>
          </div>
        </div>

        {sim.event_types.length > 0 && (
          <div className="chips">
            {sim.event_types.map((t) => (
              <span className="chip" key={t}>
                {t}
              </span>
            ))}
          </div>
        )}

        <div className="last-event">
          <span className="muted">Last event</span>
          {sim.last_event ? (
            <>
              <Link className="mono" to={`/events/${encodeURIComponent(sim.last_event.id)}`}>
                {sim.last_event.id}
              </Link>
              <span className="muted">
                {sim.last_event.type} · {fmtRelative(sim.last_event.ts, now)}
              </span>
            </>
          ) : (
            <span className="muted">none yet</span>
          )}
        </div>

        {sim.running && (
          <button className="btn btn-danger" onClick={onStop} disabled={stopping}>
            <IconStop size={14} /> Stop
          </button>
        )}
      </div>
    </Card>
  );
}

function Counter({ label, value, tone }: { label: string; value: number; tone?: "critical" | "warning" }) {
  return (
    <div className="counter">
      <div className="counter-label">
        {tone && <span className="dot" style={{ background: tone === "critical" ? C.critical : C.warning }} />}
        {label}
      </div>
      <div className="counter-value">{fmtNum(value)}</div>
    </div>
  );
}

function LoadTestCharts({ lt, running }: { lt: NonNullable<SimStatus["loadtest"]>; running: boolean }) {
  const data = useMemo(() => [...lt.steps].sort((a, b) => a.t_s - b.t_s), [lt.steps]);
  const progress = lt.duration_s > 0 ? Math.min(1, lt.elapsed_s / lt.duration_s) : 0;
  const totalErrors = data.reduce((a, s) => a + s.errors, 0);
  const last = data[data.length - 1];
  const xDomain: [number, number] = [0, Math.max(lt.duration_s, last?.t_s ?? 0)];

  return (
    <Card
      title="Load test"
      sub={`${fmtInt(lt.start_eps)} → ${fmtInt(lt.end_eps)} eps over ${fmtInt(lt.duration_s)} s · ${
        running ? `${Math.round(lt.elapsed_s)} s elapsed` : "finished"
      }${totalErrors ? ` · ${fmtInt(totalErrors)} errors` : ""}`}
    >
      <div className="stack">
        <Meter value={progress} label="Load test progress" tone={progress >= 1 ? "good" : undefined} />
        {data.length === 0 ? (
          <EmptyState compact title="Waiting for the first step…">
            ShopSim reports one step per measurement interval.
          </EmptyState>
        ) : (
          <div className="grid grid-2">
            <div>
              <div className="row row-between" style={{ marginBottom: 8 }}>
                <div className="secondary">Target vs achieved rate (eps)</div>
                <Legend
                  items={[
                    { label: "Achieved", color: C.accent },
                    { label: "Target", color: C.context },
                  ]}
                />
              </div>
              <div className="chart-wrap" style={{ height: 240 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                    <CartesianGrid stroke={C.grid} vertical={false} />
                    <XAxis
                      dataKey="t_s"
                      type="number"
                      domain={xDomain}
                      tickFormatter={(v: number) => `${v}s`}
                      stroke={C.axis}
                      tickLine={false}
                    />
                    <YAxis tickFormatter={fmtAxis} stroke={C.axis} tickLine={false} axisLine={false} width={48} />
                    <Tooltip
                      cursor={{ stroke: C.axis, strokeWidth: 1 }}
                      content={
                        <ChartTooltip
                          labelFormatter={(l) => `t = ${String(l)} s`}
                          valueFormatter={(v) => `${fmtRate(v)} eps`}
                        />
                      }
                    />
                    <Line
                      name="Target"
                      dataKey="target_eps"
                      stroke={C.context}
                      strokeWidth={2}
                      dot={false}
                      isAnimationActive={false}
                    />
                    <Line
                      name="Achieved"
                      dataKey="achieved_eps"
                      stroke={C.accent}
                      strokeWidth={2}
                      dot={false}
                      activeDot={{ r: 4, stroke: "var(--surface-1)", strokeWidth: 2 }}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
            <div>
              <div className="row row-between" style={{ marginBottom: 8 }}>
                <div className="secondary">Ingestion p95 latency per step</div>
                <span className="muted tnum">{last ? `latest ${fmtMs(last.p95_ms)}` : ""}</span>
              </div>
              <div className="chart-wrap" style={{ height: 240 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={data} margin={{ top: 8, right: 12, bottom: 0, left: 0 }}>
                    <CartesianGrid stroke={C.grid} vertical={false} />
                    <XAxis
                      dataKey="t_s"
                      type="number"
                      domain={xDomain}
                      tickFormatter={(v: number) => `${v}s`}
                      stroke={C.axis}
                      tickLine={false}
                    />
                    <YAxis
                      tickFormatter={(v: number) => `${fmtAxis(v)} ms`}
                      stroke={C.axis}
                      tickLine={false}
                      axisLine={false}
                      width={60}
                    />
                    <Tooltip
                      cursor={{ stroke: C.axis, strokeWidth: 1 }}
                      content={
                        <ChartTooltip labelFormatter={(l) => `t = ${String(l)} s`} valueFormatter={(v) => fmtMs(v)} />
                      }
                    />
                    <Line
                      name="p95"
                      dataKey="p95_ms"
                      stroke={C.accent}
                      strokeWidth={2}
                      dot={false}
                      activeDot={{ r: 4, stroke: "var(--surface-1)", strokeWidth: 2 }}
                      isAnimationActive={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>
        )}
        {data.length > 0 && (
          <details className="collapsible">
            <summary>Step table</summary>
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th className="num">t</th>
                    <th className="num">Target eps</th>
                    <th className="num">Achieved eps</th>
                    <th className="num">p95</th>
                    <th className="num">Errors</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((s) => (
                    <tr key={s.t_s}>
                      <td className="num">{s.t_s}s</td>
                      <td className="num">{fmtRate(s.target_eps)}</td>
                      <td className="num">{fmtRate(s.achieved_eps)}</td>
                      <td className="num">{fmtMs(s.p95_ms)}</td>
                      <td className="num">{fmtInt(s.errors)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
        )}
      </div>
    </Card>
  );
}
