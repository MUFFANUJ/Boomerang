// Live data: one WebSocket per app, reconnecting with exponential backoff.
// Exposes the latest stats/infra/integrity snapshots, rolling series built from
// ticks, and pub/sub for flow items and endpoint_change notifications.
import { createContext, useContext, useEffect, useRef, useSyncExternalStore, type ReactNode } from "react";
import { api } from "../api/client";
import type {
  FlowData,
  InfraSnapshot,
  IntegrityData,
  ServerMessage,
  StatsData,
  TimeseriesResponse,
} from "../api/types";

export type WsStatus = "connecting" | "open" | "closed";

/** One point per stats tick (1s). `received`/`deferred` are null for points seeded from history. */
export interface ThroughputPoint {
  t: number;
  received: number | null;
  delivered: number;
  failed: number;
  deferred: number | null;
  p95: number | null;
  successRate: number | null;
  seeded: boolean;
}

export interface EndpointRatePoint {
  t: number;
  delivered: number;
  failed: number;
}

export interface LagPoint {
  t: number;
  lag: number;
}

export interface LiveState {
  status: WsStatus;
  /** ms until the next reconnect attempt (when status is "closed"). */
  reconnectAt: number | null;
  attempts: number;
  lastMessageAt: number | null;
  stats: StatsData | null;
  infra: InfraSnapshot | null;
  integrity: IntegrityData | null;
  /** Rolling 5-minute throughput series (stats ticks, optionally seeded from /api/metrics/timeseries). */
  series: ThroughputPoint[];
  /** Rolling 5-minute per-endpoint delivered/failed eps, keyed by endpoint id. */
  endpointSeries: Record<string, EndpointRatePoint[]>;
  /** Rolling 5-minute total lag of the boomerang-delivery consumer group. */
  lagSeries: LagPoint[];
  sampleRate: number | null;
}

export const WINDOW_MS = 5 * 60 * 1000;
export const DELIVERY_GROUP = "boomerang-delivery";

type Listener = () => void;
type FlowListener = (data: FlowData) => void;
type EndpointChangeListener = (endpointId: string) => void;

function trimWindow<T extends { t: number }>(arr: T[], now: number): T[] {
  const cutoff = now - WINDOW_MS;
  let i = 0;
  while (i < arr.length && arr[i].t < cutoff) i++;
  return i === 0 ? arr : arr.slice(i);
}

export class LiveStore {
  state: LiveState = {
    status: "connecting",
    reconnectAt: null,
    attempts: 0,
    lastMessageAt: null,
    stats: null,
    infra: null,
    integrity: null,
    series: [],
    endpointSeries: {},
    lagSeries: [],
    sampleRate: null,
  };

  private listeners = new Set<Listener>();
  private flowListeners = new Set<FlowListener>();
  private endpointListeners = new Set<EndpointChangeListener>();
  private ws: WebSocket | null = null;
  private retryTimer: number | undefined;
  private pingTimer: number | undefined;
  private watchdog: number | undefined;
  private stopped = false;
  private seededKey: string | null = null;

  subscribe = (l: Listener) => {
    this.listeners.add(l);
    return () => {
      this.listeners.delete(l);
    };
  };

  getState = () => this.state;

  onFlow(l: FlowListener): () => void {
    this.flowListeners.add(l);
    return () => {
      this.flowListeners.delete(l);
    };
  }

  onEndpointChange(l: EndpointChangeListener): () => void {
    this.endpointListeners.add(l);
    return () => {
      this.endpointListeners.delete(l);
    };
  }

  private set(patch: Partial<LiveState>) {
    this.state = { ...this.state, ...patch };
    for (const l of this.listeners) l();
  }

  start() {
    this.stopped = false;
    this.connect();
    void this.bootstrap();
  }

  stop() {
    this.stopped = true;
    window.clearTimeout(this.retryTimer);
    window.clearInterval(this.pingTimer);
    window.clearInterval(this.watchdog);
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.close();
      this.ws = null;
    }
  }

  /** REST snapshots so pages render before the first WS tick. Only fills empty slots. */
  private async bootstrap() {
    const [overview, integrity, infra] = await Promise.allSettled([
      api.getOverview(),
      api.getIntegrity(),
      api.getInfra(),
    ]);
    if (overview.status === "fulfilled" && !this.state.stats) this.set({ stats: overview.value });
    if (integrity.status === "fulfilled" && !this.state.integrity) this.set({ integrity: integrity.value });
    if (infra.status === "fulfilled" && !this.state.infra) this.ingestInfra(infra.value);
  }

  private url(): string {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${window.location.host}/ws`;
  }

  private connect() {
    if (this.stopped) return;
    this.set({ status: "connecting", reconnectAt: null });
    let ws: WebSocket;
    try {
      ws = new WebSocket(this.url());
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.onopen = () => {
      this.set({ status: "open", attempts: 0, reconnectAt: null, lastMessageAt: Date.now() });
      window.clearInterval(this.pingTimer);
      this.pingTimer = window.setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: "ping" }));
      }, 20_000);
      window.clearInterval(this.watchdog);
      // stats arrive every second; if nothing for 12s, the socket is wedged.
      this.watchdog = window.setInterval(() => {
        const last = this.state.lastMessageAt ?? 0;
        if (Date.now() - last > 12_000 && ws.readyState === WebSocket.OPEN) ws.close();
      }, 4_000);
    };

    ws.onmessage = (ev) => {
      let msg: ServerMessage;
      try {
        msg = JSON.parse(typeof ev.data === "string" ? ev.data : "") as ServerMessage;
      } catch {
        return;
      }
      this.handle(msg);
    };

    ws.onerror = () => {
      /* onclose follows */
    };

    ws.onclose = () => {
      window.clearInterval(this.pingTimer);
      window.clearInterval(this.watchdog);
      if (this.ws === ws) this.ws = null;
      this.scheduleReconnect();
    };
  }

  private scheduleReconnect() {
    if (this.stopped) return;
    const attempts = this.state.attempts + 1;
    const base = Math.min(15_000, 500 * 2 ** Math.min(attempts - 1, 6));
    const delay = Math.round(base * (0.75 + Math.random() * 0.5));
    this.set({ status: "closed", attempts, reconnectAt: Date.now() + delay });
    window.clearTimeout(this.retryTimer);
    this.retryTimer = window.setTimeout(() => this.connect(), delay);
  }

  /** Skip the backoff wait and reconnect now. */
  reconnectNow() {
    if (this.state.status !== "closed") return;
    window.clearTimeout(this.retryTimer);
    this.connect();
  }

  private handle(msg: ServerMessage) {
    const now = Date.now();
    switch (msg.type) {
      case "stats":
        this.ingestStats(msg.data, now);
        break;
      case "infra":
        this.ingestInfra(msg.data, now);
        break;
      case "integrity":
        this.set({ integrity: msg.data, lastMessageAt: now });
        break;
      case "flow":
        this.state.lastMessageAt = now;
        if (this.state.sampleRate !== msg.data.sample_rate) this.set({ sampleRate: msg.data.sample_rate });
        for (const l of this.flowListeners) l(msg.data);
        break;
      case "endpoint_change":
        this.state.lastMessageAt = now;
        for (const l of this.endpointListeners) l(msg.data.endpoint_id);
        break;
      default:
        break;
    }
  }

  private ingestStats(s: StatsData, now: number) {
    const t = Date.parse(s.ts) || now;
    const point: ThroughputPoint = {
      t,
      received: s.rates.received_eps,
      delivered: s.rates.delivered_eps,
      failed: s.rates.failed_eps,
      deferred: s.rates.deferred_eps,
      p95: s.latency_ms?.p95 ?? null,
      successRate: s.success_rate,
      seeded: false,
    };
    let series = this.state.series;
    // Drop seeded points that overlap live ticks, and duplicate ticks.
    const last = series[series.length - 1];
    if (last && !last.seeded && last.t >= t) {
      series = series.slice(0, -1);
    }
    series = trimWindow(
      series.filter((p) => !p.seeded || p.t < t - 2500),
      t,
    );
    series = [...series, point];

    const endpointSeries: Record<string, EndpointRatePoint[]> = {};
    for (const ep of s.endpoints) {
      const prev = this.state.endpointSeries[ep.id] ?? [];
      endpointSeries[ep.id] = trimWindow([...prev, { t, delivered: ep.delivered_eps, failed: ep.failed_eps }], t);
    }

    this.set({ stats: s, series, endpointSeries, lastMessageAt: now });
  }

  private ingestInfra(d: InfraSnapshot, now = Date.now()) {
    const t = Date.parse(d.ts) || now;
    const group = d.consumer_groups.find((g) => g.group === DELIVERY_GROUP);
    let lagSeries = this.state.lagSeries;
    if (group) {
      const last = lagSeries[lagSeries.length - 1];
      if (!last || last.t < t) lagSeries = trimWindow([...lagSeries, { t, lag: group.total_lag }], t);
    }
    this.set({ infra: d, lagSeries, lastMessageAt: now });
  }

  /**
   * Seeds the throughput series from history. Bucket counts are converted to per-second
   * rates (count / bucket_s) so they share a scale with live ticks.
   */
  seedThroughput(resp: TimeseriesResponse, key: string) {
    if (this.seededKey === key) return;
    this.seededKey = key;
    const bucket = resp.bucket_s > 0 ? resp.bucket_s : 1;
    const live = this.state.series.filter((p) => !p.seeded);
    const firstLive = live[0]?.t ?? Infinity;
    const seeded: ThroughputPoint[] = resp.points
      .map((p) => ({
        t: Date.parse(p.ts),
        received: null,
        delivered: p.delivered / bucket,
        failed: p.failed / bucket,
        deferred: null,
        p95: p.p95_ms,
        successRate: p.delivered + p.failed > 0 ? p.delivered / (p.delivered + p.failed) : null,
        seeded: true,
      }))
      .filter((p) => Number.isFinite(p.t) && p.t < firstLive - 2500);
    const merged = trimWindow([...seeded, ...live].sort((a, b) => a.t - b.t), Date.now());
    this.set({ series: merged });
  }
}

const LiveContext = createContext<LiveStore | null>(null);

export function LiveProvider({ children }: { children: ReactNode }) {
  const storeRef = useRef<LiveStore | null>(null);
  if (!storeRef.current) storeRef.current = new LiveStore();
  useEffect(() => {
    const store = storeRef.current!;
    store.start();
    return () => store.stop();
  }, []);
  return <LiveContext.Provider value={storeRef.current}>{children}</LiveContext.Provider>;
}

export function useLiveStore(): LiveStore {
  const s = useContext(LiveContext);
  if (!s) throw new Error("useLiveStore must be used inside <LiveProvider>");
  return s;
}

/**
 * The WebSocket hook: latest snapshots + rolling series. Re-renders on every message that
 * changes state (stats 1s, infra 2s, integrity 3s).
 */
export function useLive(): LiveState {
  const store = useLiveStore();
  return useSyncExternalStore(store.subscribe, store.getState);
}

/** Select a slice of live state; re-renders only when the selected value changes identity. */
export function useLiveSelector<T>(selector: (s: LiveState) => T): T {
  const store = useLiveStore();
  return useSyncExternalStore(store.subscribe, () => selector(store.getState()));
}

/** Subscribe to `flow` batches. The handler is called outside React render. */
export function useFlowSubscription(handler: (data: FlowData) => void) {
  const store = useLiveStore();
  const ref = useRef(handler);
  ref.current = handler;
  useEffect(() => store.onFlow((d) => ref.current(d)), [store]);
}

/** Subscribe to `endpoint_change` notifications. */
export function useEndpointChange(handler: (endpointId: string) => void) {
  const store = useLiveStore();
  const ref = useRef(handler);
  ref.current = handler;
  useEffect(() => store.onEndpointChange((id) => ref.current(id)), [store]);
}
