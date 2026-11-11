import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent as RMouseEvent,
  type PointerEvent as RPointerEvent,
  type ReactNode,
} from "react";
import { Link } from "react-router-dom";
import { api } from "../../api/client";
import type { Endpoint, FlowData, StatsEndpoint } from "../../api/types";
import { Drawer } from "../../components/Modal";
import { EventDetail } from "../../components/EventDetail";
import { IconInbox, IconPause, IconPlay, IconRefresh, IconSkull } from "../../components/Icons";
import { Badge, CircuitBadge, ErrorNote, Legend, PageHead, successTone } from "../../components/ui";
import { readCssVar, useColorSchemeVersion } from "../../lib/colors";
import { fmtInt, fmtNum, fmtPct, fmtRate, shortId } from "../../lib/format";
import { useAsync, useElementSize } from "../../lib/hooks";
import { DELIVERY_GROUP, useEndpointChange, useFlowSubscription, useLive } from "../../lib/live";
import { useTenant } from "../../lib/tenant";
import { FlowEngine, MAX_DOTS, type DotInfo, type FlowColors, type FlowCounters } from "./flowEngine";
import { computeLayout, type LaneDef } from "./layout";
import "./flow.css";

const EMPTY_COUNTERS: FlowCounters = {
  dots: 0,
  inFlight: 0,
  retrying: 0,
  deadSeen: 0,
  deliveredSeen: 0,
  failedSeen: 0,
  eventsSeen: 0,
  evicted: 0,
  lastItemAt: null,
};

function readColors(): FlowColors {
  return {
    surface: readCssVar("--surface-1"),
    surface2: readCssVar("--surface-2"),
    surface3: readCssVar("--surface-3"),
    border: readCssVar("--border-strong"),
    grid: readCssVar("--grid"),
    axis: readCssVar("--axis"),
    text: readCssVar("--text-primary"),
    textMuted: readCssVar("--text-muted"),
    inflight: readCssVar("--series-1"),
    good: readCssVar("--good"),
    critical: readCssVar("--critical"),
    warning: readCssVar("--warning"),
    font: readCssVar("--font-sans") || "system-ui, sans-serif",
  };
}

export function FlowPage() {
  const { tenantId } = useTenant();
  const live = useLive();
  const { stats, infra, status, sampleRate } = live;

  /* Endpoints → lanes ------------------------------------------------ */
  const endpoints = useAsync<Endpoint[]>((signal) => api.listEndpoints(tenantId, signal), [tenantId]);
  const [extraLanes, setExtraLanes] = useState<LaneDef[]>([]);
  const refetchTimer = useRef<number | undefined>(undefined);
  const scheduleRefetch = useCallback(() => {
    window.clearTimeout(refetchTimer.current);
    refetchTimer.current = window.setTimeout(() => endpoints.reload(), 800);
  }, [endpoints.reload]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => () => window.clearTimeout(refetchTimer.current), []);
  useEndpointChange(() => scheduleRefetch());

  const lanes = useMemo<LaneDef[]>(() => {
    const list: LaneDef[] = (endpoints.data ?? []).map((e) => ({ id: e.id, name: e.name }));
    for (const x of extraLanes) if (!list.some((l) => l.id === x.id)) list.push(x);
    return list;
  }, [endpoints.data, extraLanes]);

  /* Layout & engine -------------------------------------------------- */
  const [wrapRef, size] = useElementSize<HTMLDivElement>();
  const layout = useMemo(() => computeLayout(size.width || 960, lanes), [size.width, lanes]);
  const schemeVersion = useColorSchemeVersion();
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const engineRef = useRef<FlowEngine | null>(null);
  const [counters, setCounters] = useState<FlowCounters>(EMPTY_COUNTERS);
  const [paused, setPaused] = useState(false);
  const [hover, setHover] = useState<DotInfo | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const engine = new FlowEngine(canvas, setCounters);
    engineRef.current = engine;
    engine.start();
    return () => {
      engine.stop();
      engineRef.current = null;
    };
  }, []);

  useEffect(() => {
    engineRef.current?.setLayout(layout, window.devicePixelRatio || 1);
  }, [layout]);

  useEffect(() => {
    engineRef.current?.setColors(readColors());
  }, [schemeVersion]);

  useEffect(() => {
    engineRef.current?.setPaused(paused);
  }, [paused]);

  // New tenant: start from a clean canvas.
  useEffect(() => {
    engineRef.current?.clear();
    setExtraLanes([]);
    setHover(null);
  }, [tenantId]);

  const tenantRef = useRef(tenantId);
  tenantRef.current = tenantId;
  useFlowSubscription((data: FlowData) => {
    const engine = engineRef.current;
    if (!engine || document.hidden) return;
    const unknown = engine.ingest(data, tenantRef.current);
    if (unknown.length) {
      setExtraLanes((prev) => {
        let next = prev;
        for (const u of unknown) {
          if (next.some((l) => l.id === u.endpoint_id)) continue;
          if (next === prev) next = prev.slice();
          next.push({ id: u.endpoint_id, name: u.endpoint_name });
        }
        return next;
      });
      scheduleRefetch();
    }
  });

  /* Pointer --------------------------------------------------------- */
  const pointToDot = (e: RPointerEvent<HTMLCanvasElement> | RMouseEvent<HTMLCanvasElement>) => {
    const engine = engineRef.current;
    if (!engine) return null;
    const rect = e.currentTarget.getBoundingClientRect();
    return engine.hitTest(e.clientX - rect.left, e.clientY - rect.top, 12);
  };
  const onMove = (e: RPointerEvent<HTMLCanvasElement>) => {
    const info = pointToDot(e);
    engineRef.current?.setHover(info?.id ?? null);
    setHover((prev) => (prev?.id === info?.id && prev?.x === info?.x ? prev : info));
  };
  const onLeave = () => {
    engineRef.current?.setHover(null);
    setHover(null);
  };
  const onClick = (e: RMouseEvent<HTMLCanvasElement>) => {
    const info = pointToDot(e);
    if (info?.eventId) setSelected(info.eventId);
  };

  /* Live context for overlays --------------------------------------- */
  const statsById = useMemo(() => {
    const m = new Map<string, StatsEndpoint>();
    for (const ep of stats?.endpoints ?? []) m.set(ep.id, ep);
    return m;
  }, [stats]);
  const deliveryGroup = infra?.consumer_groups.find((g) => g.group === DELIVERY_GROUP) ?? null;
  const sim = stats?.sim ?? null;

  const stageRates: Record<string, string> = {
    shopsim: sim ? (sim.running ? `${fmtRate(sim.current_eps)} eps · ${sim.mode}` : "idle") : "—",
    ingestion: stats ? `${fmtRate(stats.rates.received_eps)} eps in` : "—",
    kafka: deliveryGroup ? `lag ${fmtNum(deliveryGroup.total_lag)}` : "—",
    workers: infra
      ? `${infra.workers.length} worker${infra.workers.length === 1 ? "" : "s"} · ${stats ? fmtRate(stats.rates.delivered_eps) : "—"}/s`
      : stats
        ? `${fmtRate(stats.rates.delivered_eps)}/s ok`
        : "—",
  };

  const noLanes = endpoints.data !== undefined && lanes.length === 0;
  let overlay: { title: string; body: ReactNode } | null = null;
  if (status !== "open" && counters.dots === 0) {
    overlay = {
      title: status === "connecting" ? "Connecting to the live stream…" : "Live stream offline",
      body: "Dots appear as soon as the WebSocket delivers flow updates. Reconnecting automatically.",
    };
  } else if (noLanes) {
    overlay = {
      title: "This tenant has no endpoints",
      body: (
        <>
          Receiver lanes come from the tenant's endpoints. <Link to="/endpoints">Create an endpoint</Link> to see
          deliveries.
        </>
      ),
    };
  } else if (counters.eventsSeen === 0 && counters.dots === 0) {
    overlay = {
      title: "Waiting for events",
      body: (
        <>
          Nothing is flowing for this tenant right now. <Link to="/traffic">Start ShopSim traffic</Link> to watch
          events travel through the pipeline.
        </>
      ),
    };
  }

  const L = layout;

  return (
    <>
      <PageHead
        title="Live Event Flow"
        sub="Every dot is a real sampled event from the WebSocket stream. Deliveries split per endpoint; failures boomerang through the retry lane; exhausted ones land in the dead-letter bin. Click a dot for its full history."
        actions={
          <>
            <button className="btn" onClick={() => setPaused((p) => !p)}>
              {paused ? <IconPlay size={14} /> : <IconPause size={14} />}
              {paused ? "Resume" : "Pause"}
            </button>
            <button className="btn" onClick={() => engineRef.current?.clear()}>
              <IconRefresh size={14} />
              Clear
            </button>
          </>
        }
      />

      {endpoints.error != null && <ErrorNote error={endpoints.error} onRetry={endpoints.reload} />}

      <section className="card flow-card">
        <div className="flow-toolbar">
          <Legend
            items={[
              { label: "In flight", color: "var(--series-1)", kind: "dot" },
              { label: "Delivered", color: "var(--good)", kind: "dot" },
              { label: "Failed → retry", color: "var(--critical)", kind: "dot" },
              { label: "Deferred (circuit open / throttled)", color: "var(--warning)", kind: "dot" },
              { label: "Dead-lettered", color: "var(--critical)", kind: "rect" },
            ]}
          />
          <div className="row row-wrap flow-meta">
            {paused && (
              <Badge tone="warning" icon={<IconPause size={12} />}>
                Paused
              </Badge>
            )}
            <span
              className="badge"
              title="The server samples flow by event id, so a sampled event's retries and DLQ moves are sampled too."
            >
              Sampling {sampleRate === null ? "—" : fmtPct(sampleRate, sampleRate < 0.1 ? 1 : 0)} of events
            </span>
            <span className="badge tnum" title={`At most ${MAX_DOTS} dots are kept on screen`}>
              {fmtInt(counters.dots)} / {MAX_DOTS} dots
            </span>
            {counters.evicted > 0 && (
              <span className="badge" title="Oldest dots removed to keep the animation smooth during bursts">
                {fmtNum(counters.evicted)} trimmed
              </span>
            )}
          </div>
        </div>

        <div className="flow-wrap" ref={wrapRef}>
          <div className="flow-stage" style={{ width: L.width, height: L.height }}>
            <canvas
              ref={canvasRef}
              className="flow-canvas"
              style={{ cursor: hover ? "pointer" : "default" }}
              onPointerMove={onMove}
              onPointerLeave={onLeave}
              onClick={onClick}
              aria-label="Animated view of events moving through ingestion, Kafka, workers and receivers"
              role="img"
            />

            {/* Live stage rates under each node */}
            {L.stages.map((s) => (
              <div
                key={s.key}
                className="flow-stage-rate tnum"
                style={{ left: s.x, top: L.pipeY + L.nodeH / 2 + 8, width: Math.max(L.nodeW + 30, 100) }}
              >
                <span className="flow-plate">{stageRates[s.key]}</span>
              </div>
            ))}

            {/* Per-receiver status strips, aligned to lanes */}
            {L.lanes.map((lane) => {
              const s = statsById.get(lane.id);
              const roomy = lane.h >= 62;
              return (
                <div
                  key={lane.id}
                  className="flow-strip"
                  style={{ left: L.stripX, top: lane.y - lane.h / 2, width: L.stripW, height: lane.h }}
                >
                  <div className="flow-strip-name" title={lane.name}>
                    {lane.name}
                  </div>
                  {s ? (
                    <>
                      <div className="row" style={{ gap: 6 }}>
                        <CircuitBadge state={s.circuit} small />
                        <span className={`flow-sr tone-${successTone(s.success_rate)}`}>
                          {fmtPct(s.success_rate)} ok
                        </span>
                      </div>
                      {roomy && (
                        <div className="flow-strip-rates tnum">
                          {fmtRate(s.delivered_eps)}/s delivered · {fmtRate(s.failed_eps)}/s failed
                        </div>
                      )}
                    </>
                  ) : (
                    <div className="flow-strip-rates">No live stats yet</div>
                  )}
                </div>
              );
            })}

            {/* Retry lane counter */}
            <div
              className="flow-retry-meta tnum"
              style={{ left: L.retry.left - 6, top: L.retry.y + L.retry.h / 2 + 6 }}
            >
              {fmtInt(counters.retrying)} sampled dots waiting to retry
              {stats && <> · {fmtNum(stats.totals.retrying)} deliveries retrying overall</>}
            </div>

            {/* DLQ bin label */}
            <div className="flow-dlq" style={{ left: L.bin.x, top: L.bin.y - 4, width: L.bin.w }}>
              <div className="flow-dlq-title">
                <IconSkull size={13} /> Dead-letter bin
              </div>
              <div className="flow-dlq-count tnum">
                <strong>{fmtInt(counters.deadSeen)}</strong> seen on this page
                {stats && <span className="muted"> · DLQ total {fmtNum(stats.totals.dlq)}</span>}
              </div>
            </div>

            {overlay && (
              <div className="flow-overlay" style={{ top: L.pipeTop, height: L.pipeBottom - L.pipeTop }}>
                <div className="flow-overlay-box">
                  <div className="empty-icon">
                    <IconInbox size={18} />
                  </div>
                  <div className="empty-title">{overlay.title}</div>
                  <div>{overlay.body}</div>
                </div>
              </div>
            )}

            {hover && (
              <div
                className="chart-tip flow-tip"
                style={{
                  left: Math.min(hover.x + 14, L.width - 250),
                  top: Math.max(4, hover.y - 12),
                }}
              >
                <div className="chart-tip-title mono">{shortId(hover.eventId, 18)}</div>
                {hover.eventType && (
                  <div className="chart-tip-row">
                    <strong>{hover.eventType}</strong>
                  </div>
                )}
                {hover.endpointName && (
                  <div className="chart-tip-row">
                    <span className="label">to</span> <strong>{hover.endpointName}</strong>
                  </div>
                )}
                <div className="chart-tip-row">
                  <span className="label">{hover.state}</span>
                </div>
                {hover.attempt > 0 && (
                  <div className="chart-tip-row">
                    <span className="label">attempt</span> <strong>{hover.attempt}</strong>
                    {hover.statusCode !== null && (
                      <>
                        <span className="label">status</span>
                        <strong>{hover.statusCode === 0 ? "no response" : hover.statusCode}</strong>
                      </>
                    )}
                  </div>
                )}
                {hover.reason && <div className="flow-tip-reason">{hover.reason}</div>}
                <div className="flow-tip-hint">Click for delivery history</div>
              </div>
            )}
          </div>
        </div>

        <div className="flow-foot">
          <span>
            Seen on this page: <strong className="tnum">{fmtInt(counters.eventsSeen)}</strong> events ·{" "}
            <strong className="tnum">{fmtInt(counters.deliveredSeen)}</strong> delivered ·{" "}
            <strong className="tnum">{fmtInt(counters.failedSeen)}</strong> failed attempts ·{" "}
            <strong className="tnum">{fmtInt(counters.deadSeen)}</strong> dead-lettered
          </span>
          <span className="muted">Counts cover sampled dots only; see Overview for exact totals.</span>
        </div>
      </section>

      {selected && (
        <Drawer
          title={
            <span>
              Event <span className="mono">{selected}</span>
            </span>
          }
          actions={
            <Link className="btn btn-sm" to={`/events/${encodeURIComponent(selected)}`}>
              Open in Explorer
            </Link>
          }
          onClose={() => setSelected(null)}
        >
          <EventDetail tenantId={tenantId} eventId={selected} compact />
        </Drawer>
      )}
    </>
  );
}
