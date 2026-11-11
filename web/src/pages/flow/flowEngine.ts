// Canvas engine for the Live Event Flow. Pure TypeScript, no React: React feeds it layout,
// colors and flow batches; it animates dots with requestAnimationFrame and reports coarse
// counters back a few times per second.
import type { AttemptOutcome, FlowAttemptItem, FlowData, FlowDeliveryRef } from "../../api/types";
import type { FlowLayout, LaneGeom } from "./layout";

export interface FlowColors {
  surface: string;
  surface2: string;
  surface3: string;
  border: string;
  grid: string;
  axis: string;
  text: string;
  textMuted: string;
  inflight: string;
  good: string;
  critical: string;
  warning: string;
  font: string;
}

export interface FlowCounters {
  dots: number;
  inFlight: number;
  retrying: number;
  deadSeen: number;
  deliveredSeen: number;
  failedSeen: number;
  eventsSeen: number;
  evicted: number;
  lastItemAt: number | null;
}

export interface DotInfo {
  id: number;
  eventId: string;
  eventType: string;
  endpointName: string;
  state: string;
  attempt: number;
  statusCode: number | null;
  reason: string;
  x: number;
  y: number;
}

type Phase =
  | "queued"
  | "pipeline"
  | "toLane"
  | "approach"
  | "waiting"
  | "finishing"
  | "absorbed"
  | "arcing"
  | "retrying"
  | "reentry"
  | "falling"
  | "expired";

const enum Col {
  Inflight = 0,
  Good = 1,
  Critical = 2,
  Warning = 3,
}

const enum Ease {
  Linear = 0,
  Out = 1,
  InOut = 2,
  In = 3,
}

interface Dot {
  id: number;
  kind: "event" | "delivery";
  eventId: string;
  eventType: string;
  deliveryId: string | null;
  endpointId: string | null;
  endpointName: string;
  lane: LaneGeom | null;
  deliveries: FlowDeliveryRef[] | null;
  /** attempts that arrived while this event dot was still in the pipeline */
  early: FlowAttemptItem[] | null;
  x: number;
  y: number;
  alpha: number;
  col: Col;
  dead: boolean;
  phase: Phase;
  // active motion segment
  sx: number;
  sy: number;
  cx: number;
  cy: number;
  ex: number;
  ey: number;
  curved: boolean;
  t0: number;
  dur: number;
  ease: Ease;
  /** -1..1, scaled by the band the dot is in */
  jitter: number;
  /** 0..1, horizontal spread so parked dots read as a stream, not a column */
  jx: number;
  /** x where the dot entered the retry lane */
  rx: number;
  /** last time an attempt for this dot arrived */
  touched: number;
  pending: FlowAttemptItem[];
  attempt: number;
  outcome: AttemptOutcome | null;
  statusCode: number | null;
  reason: string;
  phaseStart: number;
  retryAt: number | null;
  spawnAt: number;
  born: number;
}

export const MAX_DOTS = 600;
const R = 4;
const PIPE_MS = 1100;
const TO_LANE_MS = 380;
const APPROACH_MS = 420;
const FINISH_MS = 260;
const ABSORB_MS = 420;
const ARC_MS = 760;
const REENTRY_MS = 620;
const FALL_MS = 820;
const EXPIRE_MS = 600;
const WAIT_TIMEOUT_MS = 45_000;
const RETRY_NULL_TIMEOUT_MS = 180_000;
const MAX_RETRY_DELAY_MS = 10 * 60_000;
const MIN_RETRY_VISIBLE_MS = 700;
/** y offset of the dead-letter bin's open top (room above it for the HTML caption) */
const BIN_TOP = 40;
/** most dots parked in the retry lane before orphan deferrals stop spawning new dots */
const MAX_HELD = 240;
/** a held (deferred) dot with no news for this long fades out */
const HELD_TIMEOUT_MS = 30_000;

function ease(e: Ease, t: number): number {
  switch (e) {
    case Ease.Out:
      return 1 - (1 - t) * (1 - t) * (1 - t);
    case Ease.InOut:
      return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    case Ease.In:
      return t * t * t;
    default:
      return t;
  }
}

export class FlowEngine {
  private canvas: HTMLCanvasElement;
  private ctx: CanvasRenderingContext2D;
  private bg: HTMLCanvasElement;
  /** labels layer, drawn on top of the dots */
  private fg: HTMLCanvasElement;
  private layout: FlowLayout | null = null;
  private colors: FlowColors | null = null;
  private dpr = 1;
  private laneById = new Map<string, LaneGeom>();

  private dots: Dot[] = [];
  private byDelivery = new Map<string, Dot>();
  /** delivery id → event dot that will spawn it at the workers split */
  private awaitingSplit = new Map<string, Dot>();
  private nextId = 1;
  private now = 0;
  private lastTs = 0;
  private raf = 0;
  private running = false;
  private paused = false;
  private pausedBuffer: { data: FlowData; tenantId: string }[] = [];
  private hoverId: number | null = null;
  private lanePulse = new Map<string, number>();

  private pile = 0;
  private counters: FlowCounters = {
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
  private lastEmit = 0;
  private onCounters: (c: FlowCounters) => void;

  constructor(canvas: HTMLCanvasElement, onCounters: (c: FlowCounters) => void) {
    this.canvas = canvas;
    const ctx = canvas.getContext("2d");
    if (!ctx) throw new Error("2D canvas not available");
    this.ctx = ctx;
    this.bg = document.createElement("canvas");
    this.fg = document.createElement("canvas");
    this.onCounters = onCounters;
  }

  /* ---------------------------------------------------------------- */
  /* Configuration                                                     */
  /* ---------------------------------------------------------------- */

  setLayout(layout: FlowLayout, dpr: number) {
    this.layout = layout;
    this.dpr = dpr;
    this.laneById.clear();
    for (const l of layout.lanes) this.laneById.set(l.id, l);
    // Re-bind dots to the new lane geometry (or drop the lane if it vanished).
    for (const d of this.dots) {
      if (d.endpointId) d.lane = this.laneById.get(d.endpointId) ?? null;
    }
    const w = Math.round(layout.width * dpr);
    const h = Math.round(layout.height * dpr);
    if (this.canvas.width !== w || this.canvas.height !== h) {
      this.canvas.width = w;
      this.canvas.height = h;
    }
    this.canvas.style.width = `${layout.width}px`;
    this.canvas.style.height = `${layout.height}px`;
    this.bg.width = w;
    this.bg.height = h;
    this.fg.width = w;
    this.fg.height = h;
    this.renderBackground();
  }

  setColors(colors: FlowColors) {
    this.colors = colors;
    this.renderBackground();
  }

  hasLane(endpointId: string): boolean {
    return this.laneById.has(endpointId);
  }

  setPaused(p: boolean) {
    this.paused = p;
    if (!p && this.pausedBuffer.length) {
      const buf = this.pausedBuffer;
      this.pausedBuffer = [];
      for (const b of buf) this.ingest(b.data, b.tenantId);
    }
  }

  setHover(id: number | null) {
    this.hoverId = id;
  }

  clear() {
    this.dots.length = 0;
    this.byDelivery.clear();
    this.awaitingSplit.clear();
    this.pausedBuffer = [];
    this.pile = 0;
    this.lanePulse.clear();
    this.counters = {
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
    this.emit(true);
  }

  start() {
    if (this.running) return;
    this.running = true;
    this.lastTs = performance.now();
    const loop = (ts: number) => {
      if (!this.running) return;
      const dt = Math.min(64, Math.max(0, ts - this.lastTs));
      this.lastTs = ts;
      if (!this.paused) this.now += dt;
      this.update();
      this.draw();
      this.raf = requestAnimationFrame(loop);
    };
    this.raf = requestAnimationFrame(loop);
  }

  stop() {
    this.running = false;
    cancelAnimationFrame(this.raf);
  }

  /* ---------------------------------------------------------------- */
  /* Ingest                                                            */
  /* ---------------------------------------------------------------- */

  /** Feed one `flow` batch. Returns endpoint refs of this tenant that have no lane yet. */
  ingest(data: FlowData, tenantId: string): FlowDeliveryRef[] {
    const unknown: FlowDeliveryRef[] = [];
    if (this.paused) {
      if (this.pausedBuffer.length < 200) this.pausedBuffer.push({ data, tenantId });
      return unknown;
    }
    const items = data.items;
    if (!items.length) return unknown;
    let nEvents = 0;
    for (const it of items) if (it.kind === "event" && it.tenant_id === tenantId) nEvents++;
    const stagger = nEvents > 0 ? 100 / nEvents : 0;
    let ei = 0;

    for (const it of items) {
      if (it.kind === "event") {
        if (it.tenant_id !== tenantId) continue;
        for (const dl of it.deliveries) if (!this.laneById.has(dl.endpoint_id)) unknown.push(dl);
        const d = this.spawn("event", this.now + ei * stagger);
        ei++;
        d.eventId = it.event_id;
        d.eventType = it.event_type;
        d.deliveries = it.deliveries;
        d.phase = "queued";
        for (const dl of it.deliveries) if (!this.byDelivery.has(dl.delivery_id)) this.awaitingSplit.set(dl.delivery_id, d);
        this.counters.eventsSeen++;
        this.counters.lastItemAt = Date.now();
      } else {
        const parent = this.awaitingSplit.get(it.delivery_id);
        if (parent) {
          // Its event dot is still travelling the pipeline; hand the attempt over at the split.
          (parent.early ??= []).push(it);
          this.counters.lastItemAt = Date.now();
          continue;
        }
        const existing = this.byDelivery.get(it.delivery_id);
        if (existing) {
          existing.touched = this.now;
          if (
            it.outcome === "deferred" &&
            existing.outcome === "deferred" &&
            (existing.phase === "retrying" || existing.phase === "arcing")
          ) {
            // Still held (circuit open / throttled): stay parked in the retry lane, just refresh the details.
            existing.attempt = it.attempt;
            existing.reason = it.reason;
            existing.statusCode = it.status_code;
            this.counters.lastItemAt = Date.now();
            continue;
          }
          existing.pending.push(it);
          if (existing.phase === "retrying") this.beginReentry(existing);
          this.counters.lastItemAt = Date.now();
          continue;
        }
        const lane = this.laneById.get(it.endpoint_id);
        if (!lane) continue; // another tenant's endpoint, or not yet known
        // During an outage the backend re-defers thousands of held deliveries every second.
        // Don't churn the canvas re-spawning them once it is crowded; held dots already show the backlog.
        if (it.outcome === "deferred" && (this.dots.length >= MAX_DOTS * 0.75 || this.counters.retrying >= MAX_HELD)) {
          this.counters.lastItemAt = Date.now();
          continue;
        }
        // No dot for this delivery (sampling or page load): spawn one at the workers stage.
        const d = this.spawn("delivery", this.now);
        d.eventId = it.event_id;
        d.deliveryId = it.delivery_id;
        d.endpointId = it.endpoint_id;
        d.endpointName = it.endpoint_name;
        d.lane = lane;
        d.attempt = Math.max(0, it.attempt - 1);
        d.pending.push(it);
        this.byDelivery.set(it.delivery_id, d);
        this.beginToLane(d, this.layout ? this.layout.workersX : 0, this.layout ? this.layout.pipeY : 0);
        this.counters.lastItemAt = Date.now();
      }
    }
    return unknown;
  }

  private spawn(kind: "event" | "delivery", spawnAt: number): Dot {
    if (this.dots.length >= MAX_DOTS) this.evictOne();
    const d: Dot = {
      id: this.nextId++,
      kind,
      eventId: "",
      eventType: "",
      deliveryId: null,
      endpointId: null,
      endpointName: "",
      lane: null,
      deliveries: null,
      early: null,
      x: 0,
      y: 0,
      alpha: 1,
      col: Col.Inflight,
      dead: false,
      phase: "queued",
      sx: 0,
      sy: 0,
      cx: 0,
      cy: 0,
      ex: 0,
      ey: 0,
      curved: false,
      t0: 0,
      dur: 1,
      ease: Ease.Linear,
      jitter: Math.random() * 2 - 1,
      jx: Math.random(),
      rx: 0,
      touched: this.now,
      pending: [],
      attempt: 0,
      outcome: null,
      statusCode: null,
      reason: "",
      phaseStart: spawnAt,
      retryAt: null,
      spawnAt,
      born: this.now,
    };
    this.dots.push(d);
    return d;
  }

  private evictOne() {
    // Prefer dots that are already on their way out, then the oldest one.
    let idx = -1;
    let oldest = Infinity;
    for (let i = 0; i < this.dots.length; i++) {
      const p = this.dots[i].phase;
      if (p === "absorbed" || p === "expired" || p === "falling") {
        idx = i;
        break;
      }
      if (this.dots[i].born < oldest) {
        oldest = this.dots[i].born;
        idx = i;
      }
    }
    if (idx >= 0) {
      const d = this.dots[idx];
      if (d.phase === "falling") this.pile++;
      this.removeAt(idx);
      this.counters.evicted++;
    }
  }

  private removeAt(i: number) {
    const d = this.dots[i];
    if (d.deliveryId && this.byDelivery.get(d.deliveryId) === d) this.byDelivery.delete(d.deliveryId);
    if (d.deliveries) {
      for (const ref of d.deliveries)
        if (this.awaitingSplit.get(ref.delivery_id) === d) this.awaitingSplit.delete(ref.delivery_id);
    }
    const last = this.dots.length - 1;
    if (i !== last) this.dots[i] = this.dots[last];
    this.dots.length = last;
  }

  /* ---------------------------------------------------------------- */
  /* Motion helpers                                                    */
  /* ---------------------------------------------------------------- */

  private seg(d: Dot, ex: number, ey: number, dur: number, e: Ease, cx?: number, cy?: number) {
    d.sx = d.x;
    d.sy = d.y;
    d.ex = ex;
    d.ey = ey;
    d.curved = cx !== undefined && cy !== undefined;
    d.cx = cx ?? 0;
    d.cy = cy ?? 0;
    d.t0 = this.now;
    d.dur = Math.max(1, dur);
    d.ease = e;
  }

  /** Advances the segment; returns true when it is complete. */
  private step(d: Dot): boolean {
    const raw = (this.now - d.t0) / d.dur;
    const t = raw >= 1 ? 1 : raw <= 0 ? 0 : raw;
    const k = ease(d.ease, t);
    if (d.curved) {
      const u = 1 - k;
      d.x = u * u * d.sx + 2 * u * k * d.cx + k * k * d.ex;
      d.y = u * u * d.sy + 2 * u * k * d.cy + k * k * d.ey;
    } else {
      d.x = d.sx + (d.ex - d.sx) * k;
      d.y = d.sy + (d.ey - d.sy) * k;
    }
    return raw >= 1;
  }

  private laneY(d: Dot): number {
    const l = d.lane;
    if (!l) return this.layout ? this.layout.pipeY : 0;
    return l.y + d.jitter * (l.h / 2 - R - 3);
  }

  private retryY(d: Dot): number {
    const L = this.layout!;
    return L.retry.y + d.jitter * (L.retry.h / 2 - R - 3);
  }

  private setPhase(d: Dot, p: Phase) {
    d.phase = p;
    d.phaseStart = this.now;
  }

  private beginToLane(d: Dot, fromX: number, fromY: number) {
    const L = this.layout;
    d.x = fromX;
    d.y = fromY;
    if (!L || !d.lane) {
      this.setPhase(d, "expired");
      return;
    }
    d.col = Col.Inflight;
    const ty = this.laneY(d);
    this.seg(d, L.laneStartX, ty, TO_LANE_MS, Ease.Linear, L.laneStartX - 34, ty);
    this.setPhase(d, "toLane");
  }

  private beginReentry(d: Dot) {
    const L = this.layout;
    if (!L) return;
    this.seg(d, L.workersX + L.nodeW / 2 - 6, L.pipeY + (L.nodeH / 2 - 8) * d.jitter, REENTRY_MS, Ease.InOut, L.workersX - 30, (L.retry.y + L.pipeY) / 2 + 30);
    d.col = Col.Inflight;
    this.setPhase(d, "reentry");
  }

  private toRetryLane(d: Dot) {
    const L = this.layout!;
    const ty = this.retryY(d);
    const fromLane = d.lane ? d.lane.y : d.y;
    // Spread entry points and arc bulges so a burst of deferrals reads as a stream, not a bar.
    const span = L.retry.right - L.retry.left - 16;
    d.rx = L.retry.right - span * 0.18 * d.jx;
    const bulge = 30 + 50 * d.jx;
    this.seg(
      d,
      d.rx,
      ty,
      ARC_MS * (0.85 + 0.3 * d.jx),
      Ease.InOut,
      L.laneEndX + bulge,
      (fromLane + L.retry.y) / 2 + 10 + 28 * d.jitter,
    );
    this.setPhase(d, "arcing");
  }

  private toBin(d: Dot) {
    const L = this.layout!;
    const b = L.bin;
    const tx = b.x + 14 + Math.random() * (b.w - 28);
    const ty = b.y + b.h - 14;
    this.seg(d, tx, ty, FALL_MS, Ease.In, (d.x + tx) / 2, Math.min(d.y, b.y) - 30);
    d.dead = true;
    d.col = Col.Critical;
    this.setPhase(d, "falling");
  }

  /** Apply the next queued attempt to a dot that is parked in its lane. */
  private applyAttempt(d: Dot, a: FlowAttemptItem) {
    const L = this.layout!;
    // Consecutive deferrals queued up while travelling: only the latest matters.
    while (a.outcome === "deferred" && d.pending.length && d.pending[0].outcome === "deferred") a = d.pending.shift()!;
    d.attempt = a.attempt;
    d.outcome = a.outcome;
    d.statusCode = a.status_code;
    d.reason = a.reason;
    if (a.outcome === "success") {
      d.col = Col.Good;
      this.seg(d, L.laneEndX + 6, this.laneY(d), FINISH_MS, Ease.Out);
      this.setPhase(d, "finishing");
    } else if (a.outcome === "deferred") {
      // Circuit open or throttled: never reaches the receiver, goes straight to the retry lane.
      d.col = Col.Warning;
      d.retryAt = this.retryAtFrom(a.next_retry_at);
      this.toRetryLane(d);
    } else {
      // failure / dead: finish the trip, then bounce.
      this.seg(d, L.laneEndX + 2, this.laneY(d), FINISH_MS, Ease.Out);
      d.retryAt = a.outcome === "failure" ? this.retryAtFrom(a.next_retry_at) : null;
      this.setPhase(d, "finishing");
    }
  }

  private retryAtFrom(nextRetryAt: string | null): number | null {
    if (!nextRetryAt) return null;
    const t = Date.parse(nextRetryAt);
    if (Number.isNaN(t)) return null;
    const delay = Math.min(MAX_RETRY_DELAY_MS, Math.max(0, t - Date.now()));
    return this.now + delay;
  }

  /* ---------------------------------------------------------------- */
  /* Update                                                            */
  /* ---------------------------------------------------------------- */

  private update() {
    const L = this.layout;
    if (!L) return;
    const now = this.now;
    let inFlight = 0;
    let retrying = 0;

    for (let i = this.dots.length - 1; i >= 0; i--) {
      const d = this.dots[i];
      switch (d.phase) {
        case "queued": {
          if (now < d.spawnAt) break;
          if (d.kind === "event") {
            const y = L.pipeY + d.jitter * (L.nodeH / 2 - 9);
            d.x = L.stages[0].x;
            d.y = y;
            this.seg(d, L.workersX, y, PIPE_MS, Ease.Linear);
            this.setPhase(d, "pipeline");
          }
          break;
        }
        case "pipeline": {
          inFlight++;
          if (this.step(d)) {
            // Split: one dot per delivery heading to its receiver lane.
            const refs = d.deliveries ?? [];
            const early = d.early;
            const fx = d.x;
            const fy = d.y;
            this.removeAt(i);
            for (const ref of refs) {
              if (this.byDelivery.has(ref.delivery_id)) continue;
              const lane = this.laneById.get(ref.endpoint_id);
              if (!lane) continue;
              const c = this.spawn("delivery", now);
              c.eventId = d.eventId;
              c.eventType = d.eventType;
              c.deliveryId = ref.delivery_id;
              c.endpointId = ref.endpoint_id;
              c.endpointName = ref.endpoint_name;
              c.lane = lane;
              if (early) for (const a of early) if (a.delivery_id === ref.delivery_id) c.pending.push(a);
              this.byDelivery.set(ref.delivery_id, c);
              this.beginToLane(c, fx, fy);
            }
            continue;
          }
          break;
        }
        case "toLane": {
          inFlight++;
          if (this.step(d)) {
            this.seg(d, L.waitX - (L.waitX - L.laneStartX) * 0.5 * d.jx, this.laneY(d), APPROACH_MS, Ease.Out);
            this.setPhase(d, "approach");
          }
          break;
        }
        case "approach": {
          inFlight++;
          if (this.step(d)) this.setPhase(d, "waiting");
          break;
        }
        case "waiting": {
          inFlight++;
          if (!d.lane) {
            this.setPhase(d, "expired");
            break;
          }
          const tw = now - d.phaseStart;
          const creepTo = L.creepX - (L.creepX - L.waitX) * 0.9 * d.jx;
          const parkX = L.waitX - (L.waitX - L.laneStartX) * 0.5 * d.jx;
          d.x = parkX + (creepTo - parkX) * (1 - Math.exp(-tw / 5000));
          d.y = this.laneY(d);
          if (d.pending.length) {
            this.applyAttempt(d, d.pending.shift()!);
          } else if (tw > WAIT_TIMEOUT_MS) {
            this.setPhase(d, "expired");
          }
          break;
        }
        case "finishing": {
          if (this.step(d)) {
            if (d.outcome === "success") {
              this.counters.deliveredSeen++;
              if (d.endpointId) this.lanePulse.set(d.endpointId, now);
              this.setPhase(d, "absorbed");
            } else if (d.outcome === "dead") {
              this.counters.failedSeen++;
              this.toBin(d);
            } else {
              this.counters.failedSeen++;
              d.col = Col.Critical;
              this.toRetryLane(d);
            }
          } else inFlight++;
          break;
        }
        case "absorbed": {
          const t = (now - d.phaseStart) / ABSORB_MS;
          d.alpha = 1 - t;
          if (t >= 1) {
            this.removeAt(i);
            continue;
          }
          break;
        }
        case "arcing": {
          retrying++;
          if (this.step(d)) {
            this.setPhase(d, "retrying");
            if (d.retryAt !== null && d.retryAt < now + MIN_RETRY_VISIBLE_MS) d.retryAt = now + MIN_RETRY_VISIBLE_MS;
            if (d.pending.length) this.beginReentry(d);
          }
          break;
        }
        case "retrying": {
          retrying++;
          const start = d.rx || L.retry.right;
          const span = start - L.retry.left - 16;
          if (d.outcome === "deferred") {
            // Held: drift slowly left and wait for a real attempt (success/failure) to pull it back in.
            const tw = now - d.phaseStart;
            d.x = start - span * (0.35 + 0.55 * d.jx) * (1 - Math.exp(-tw / 15_000));
            d.y = this.retryY(d);
            if (d.pending.length) this.beginReentry(d);
            else if (now - d.touched > HELD_TIMEOUT_MS) this.setPhase(d, "expired");
          } else if (d.retryAt !== null) {
            const total = Math.max(1, d.retryAt - d.phaseStart);
            const p = Math.min(1, (now - d.phaseStart) / total);
            d.x = start - span * p;
            d.y = this.retryY(d);
            if (p >= 1) this.beginReentry(d);
          } else {
            // Unknown retry time: drift slowly toward the workers and wait for the next attempt.
            const tw = now - d.phaseStart;
            d.x = start - span * (0.55 + 0.3 * d.jx) * (1 - Math.exp(-tw / 20_000));
            d.y = this.retryY(d);
            if (d.pending.length) this.beginReentry(d);
            else if (tw > RETRY_NULL_TIMEOUT_MS) this.setPhase(d, "expired");
          }
          break;
        }
        case "reentry": {
          retrying++;
          if (this.step(d)) this.beginToLane(d, d.x, d.y);
          break;
        }
        case "falling": {
          if (this.step(d)) {
            this.pile++;
            this.counters.deadSeen++;
            this.removeAt(i);
            continue;
          }
          break;
        }
        case "expired": {
          const t = (now - d.phaseStart) / EXPIRE_MS;
          d.alpha = Math.max(0, 1 - t);
          if (t >= 1) {
            this.removeAt(i);
            continue;
          }
          break;
        }
      }
    }

    this.counters.dots = this.dots.length;
    this.counters.inFlight = inFlight;
    this.counters.retrying = retrying;
    this.emit(false);
  }

  private emit(force: boolean) {
    const t = performance.now();
    if (!force && t - this.lastEmit < 250) return;
    this.lastEmit = t;
    this.onCounters({ ...this.counters });
  }

  /* ---------------------------------------------------------------- */
  /* Drawing                                                           */
  /* ---------------------------------------------------------------- */

  private renderBackground() {
    const L = this.layout;
    const C = this.colors;
    if (!L || !C) return;
    const g = this.bg.getContext("2d");
    if (!g) return;
    g.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    g.clearRect(0, 0, L.width, L.height);
    g.fillStyle = C.surface;
    g.fillRect(0, 0, L.width, L.height);

    const { nodeW, nodeH, pipeY } = L;

    // Pipeline connectors
    g.strokeStyle = C.axis;
    g.lineWidth = 1;
    for (let i = 0; i < L.stages.length - 1; i++) {
      const a = L.stages[i].x + nodeW / 2;
      const b = L.stages[i + 1].x - nodeW / 2;
      g.beginPath();
      g.moveTo(a, pipeY);
      g.lineTo(b, pipeY);
      g.stroke();
      arrowHead(g, b, pipeY, 0, C.axis);
    }

    // Split connectors from workers to each lane
    for (const lane of L.lanes) {
      g.beginPath();
      g.moveTo(L.workersX + nodeW / 2, pipeY);
      g.quadraticCurveTo(L.laneStartX - 34, lane.y, L.laneStartX, lane.y);
      g.strokeStyle = C.axis;
      g.stroke();
    }

    // Receiver lanes
    for (const lane of L.lanes) {
      const top = lane.y - lane.h / 2;
      g.fillStyle = C.surface2;
      g.beginPath();
      g.roundRect(L.laneStartX, top, L.laneEndX - L.laneStartX, lane.h, 8);
      g.fill();
      g.strokeStyle = C.border;
      g.stroke();
      // receiver box
      g.fillStyle = C.surface3;
      g.beginPath();
      g.roundRect(L.laneEndX, top, L.recvW, lane.h, 8);
      g.fill();
      g.strokeStyle = C.border;
      g.stroke();
    }

    // Stage nodes
    for (const s of L.stages) {
      g.fillStyle = C.surface2;
      g.beginPath();
      g.roundRect(s.x - nodeW / 2, pipeY - nodeH / 2, nodeW, nodeH, 10);
      g.fill();
      g.strokeStyle = C.border;
      g.stroke();
    }

    // Retry lane (the boomerang): failures arc down from the receivers, drift right→left, re-enter workers.
    const r = L.retry;
    g.fillStyle = C.surface2;
    g.beginPath();
    g.roundRect(r.left - 10, r.y - r.h / 2, r.right - r.left + 22, r.h, r.h / 2);
    g.fill();
    g.strokeStyle = C.border;
    g.stroke();
    // guide: receivers → retry lane
    g.strokeStyle = C.axis;
    g.beginPath();
    g.moveTo(L.laneEndX, L.pipeBottom - 6);
    g.quadraticCurveTo(L.laneEndX + 54, (L.pipeBottom + r.y) / 2 + 10, r.right + 10, r.y - r.h / 2 - 2);
    g.stroke();
    // guide: retry lane → workers
    g.beginPath();
    g.moveTo(r.left - 10, r.y);
    g.quadraticCurveTo(L.workersX - 40, (r.y + pipeY) / 2 + 30, L.workersX - 6, pipeY + nodeH / 2 + 2);
    g.stroke();
    arrowHead(g, L.workersX - 6, pipeY + nodeH / 2 + 2, -Math.PI / 2 + 0.35, C.axis);
    // direction arrow inside the lane
    g.strokeStyle = C.textMuted;
    g.lineWidth = 1.5;
    const ax = r.left + 18;
    g.beginPath();
    g.moveTo(ax + 22, r.y);
    g.lineTo(ax, r.y);
    g.stroke();
    arrowHead(g, ax, r.y, Math.PI, C.textMuted);
    g.lineWidth = 1;

    // Dead-letter bin: open-top container
    const b = L.bin;
    g.strokeStyle = C.border;
    g.lineWidth = 1.5;
    g.fillStyle = C.surface2;
    g.beginPath();
    g.moveTo(b.x, b.y + BIN_TOP);
    g.lineTo(b.x + 6, b.y + b.h);
    g.lineTo(b.x + b.w - 6, b.y + b.h);
    g.lineTo(b.x + b.w, b.y + BIN_TOP);
    g.closePath();
    g.fill();
    g.stroke();
    g.lineWidth = 1;

    this.renderLabels();
  }

  /** Labels sit on opaque plates in their own layer, composited above the dots. */
  private renderLabels() {
    const L = this.layout;
    const C = this.colors;
    if (!L || !C) return;
    const g = this.fg.getContext("2d");
    if (!g) return;
    g.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    g.clearRect(0, 0, L.width, L.height);
    const { nodeW, pipeY } = L;

    // Stage nodes: title + sub on one plate centred in the node.
    for (const s of L.stages) {
      g.font = `600 12px ${C.font}`;
      const label = truncate(g, s.label, nodeW - 12);
      const w1 = g.measureText(label).width;
      g.font = `500 10px ${C.font}`;
      const sub = truncate(g, s.sub, nodeW - 12);
      const w2 = g.measureText(sub).width;
      plate(g, s.x - Math.max(w1, w2) / 2 - 6, pipeY - 16, Math.max(w1, w2) + 12, 33, C.surface2);
      g.textAlign = "center";
      g.textBaseline = "middle";
      g.fillStyle = C.text;
      g.font = `600 12px ${C.font}`;
      g.fillText(label, s.x, pipeY - 7);
      g.fillStyle = C.textMuted;
      g.font = `500 10px ${C.font}`;
      g.fillText(sub, s.x, pipeY + 9);
    }

    // Receiver names: wrap onto two lines rather than truncating.
    for (const lane of L.lanes) {
      g.font = `600 12px ${C.font}`;
      const maxW = L.recvW - 16;
      const lines = wrap2(g, lane.name, maxW);
      const showSub = lane.h >= 44 + (lines.length - 1) * 14;
      const lineH = 14;
      const blockH = lines.length * lineH + (showSub ? 13 : 0);
      let w = 0;
      for (const ln of lines) w = Math.max(w, g.measureText(ln).width);
      const top = lane.y - blockH / 2;
      plate(g, L.recvCx - w / 2 - 6, top - 3, w + 12, blockH + 6, C.surface3);
      g.textAlign = "center";
      g.textBaseline = "middle";
      g.fillStyle = C.text;
      lines.forEach((ln, i) => g.fillText(ln, L.recvCx, top + lineH * (i + 0.5)));
      if (showSub) {
        g.fillStyle = C.textMuted;
        g.font = `500 10px ${C.font}`;
        g.fillText("receiver", L.recvCx, top + lines.length * lineH + 6);
      }
    }

    // Retry lane caption.
    const r = L.retry;
    const caption = "Retry lane · the boomerang (waits until next_retry_at)";
    g.font = `600 11px ${C.font}`;
    const cw = g.measureText(caption).width;
    const cy = r.y - r.h / 2 - 12;
    plate(g, r.left - 10, cy - 9, cw + 12, 18, C.surface);
    g.fillStyle = C.textMuted;
    g.textAlign = "left";
    g.textBaseline = "middle";
    g.fillText(caption, r.left - 4, cy);
  }

  private draw() {
    const L = this.layout;
    const C = this.colors;
    if (!L || !C) return;
    const g = this.ctx;
    g.setTransform(1, 0, 0, 1, 0, 0);
    g.clearRect(0, 0, this.canvas.width, this.canvas.height);
    g.drawImage(this.bg, 0, 0);
    g.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);

    // Receiver pulses on successful delivery
    for (const lane of L.lanes) {
      const t = this.lanePulse.get(lane.id);
      if (t === undefined) continue;
      const a = 1 - (this.now - t) / 600;
      if (a <= 0) continue;
      g.globalAlpha = a * 0.9;
      g.strokeStyle = C.good;
      g.lineWidth = 2;
      g.beginPath();
      g.roundRect(L.laneEndX, lane.y - lane.h / 2, L.recvW, lane.h, 8);
      g.stroke();
    }
    g.globalAlpha = 1;
    g.lineWidth = 1;

    // DLQ pile (squares fill from the bottom)
    const b = L.bin;
    const cell = 7;
    const cols = Math.max(1, Math.floor((b.w - 24) / cell));
    const rows = Math.max(1, Math.floor((b.h - BIN_TOP - 16) / cell));
    const visible = Math.min(this.pile, cols * rows);
    if (visible > 0) {
      g.fillStyle = C.critical;
      g.globalAlpha = 0.85;
      const x0 = b.x + 12;
      const yb = b.y + b.h - 6 - cell;
      for (let k = 0; k < visible; k++) {
        const cx = k % cols;
        const cy = Math.floor(k / cols);
        g.fillRect(x0 + cx * cell, yb - cy * cell, cell - 2, cell - 2);
      }
      g.globalAlpha = 1;
    }

    // Dots: batched by color, ringed with the surface color so overlaps stay legible.
    this.drawBatch(Col.Inflight, C.inflight, C.surface);
    this.drawBatch(Col.Warning, C.warning, C.surface);
    this.drawBatch(Col.Critical, C.critical, C.surface);
    this.drawBatch(Col.Good, C.good, C.surface);

    // Fading / dead dots individually
    for (const d of this.dots) {
      if (d.phase === "queued") continue;
      if (d.alpha >= 1 && !d.dead) continue;
      g.globalAlpha = Math.max(0, d.alpha);
      g.fillStyle = d.col === Col.Good ? C.good : d.col === Col.Critical ? C.critical : d.col === Col.Warning ? C.warning : C.inflight;
      if (d.dead) {
        g.fillRect(d.x - R, d.y - R, R * 2, R * 2);
      } else {
        g.beginPath();
        g.arc(d.x, d.y, R, 0, Math.PI * 2);
        g.fill();
      }
    }
    g.globalAlpha = 1;

    // Labels above the dots so stage and receiver names stay readable.
    g.setTransform(1, 0, 0, 1, 0, 0);
    g.drawImage(this.fg, 0, 0);
    g.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);

    // Hover ring
    if (this.hoverId !== null) {
      for (const d of this.dots) {
        if (d.id !== this.hoverId) continue;
        g.strokeStyle = C.text;
        g.lineWidth = 2;
        g.beginPath();
        g.arc(d.x, d.y, R + 4, 0, Math.PI * 2);
        g.stroke();
        g.lineWidth = 1;
        break;
      }
    }
  }

  private drawBatch(col: Col, fill: string, ring: string) {
    const g = this.ctx;
    g.beginPath();
    let any = false;
    for (const d of this.dots) {
      if (d.col !== col || d.phase === "queued" || d.alpha < 1 || d.dead) continue;
      g.moveTo(d.x + R, d.y);
      g.arc(d.x, d.y, R, 0, Math.PI * 2);
      any = true;
    }
    if (!any) return;
    g.strokeStyle = ring;
    g.lineWidth = 2;
    g.stroke();
    g.fillStyle = fill;
    g.fill();
    g.lineWidth = 1;
  }

  /* ---------------------------------------------------------------- */
  /* Hit testing                                                       */
  /* ---------------------------------------------------------------- */

  hitTest(x: number, y: number, radius = 12): DotInfo | null {
    let best: Dot | null = null;
    let bestD = radius * radius;
    for (const d of this.dots) {
      if (d.phase === "queued" || d.alpha < 0.3) continue;
      const dx = d.x - x;
      const dy = d.y - y;
      const dd = dx * dx + dy * dy;
      if (dd <= bestD) {
        bestD = dd;
        best = d;
      }
    }
    if (!best) return null;
    return {
      id: best.id,
      eventId: best.eventId,
      eventType: best.eventType,
      endpointName: best.kind === "event" ? "" : best.endpointName,
      state: describe(best),
      attempt: best.attempt,
      statusCode: best.statusCode,
      reason: best.reason,
      x: best.x,
      y: best.y,
    };
  }
}

function describe(d: Dot): string {
  switch (d.phase) {
    case "queued":
    case "pipeline":
      return "Ingesting → Kafka → workers";
    case "toLane":
    case "approach":
    case "waiting":
      return d.attempt > 0 ? `Retrying (attempt ${d.attempt + 1} in flight)` : "Delivering";
    case "finishing":
      return d.outcome === "success" ? "Delivered" : "Failed";
    case "absorbed":
      return "Delivered";
    case "arcing":
    case "retrying":
      return d.outcome === "deferred" ? "Deferred, waiting to retry" : "Failed, waiting to retry";
    case "reentry":
      return "Re-entering workers";
    case "falling":
      return "Dead-lettered";
    case "expired":
      return "No further updates (sampled out)";
  }
}

function arrowHead(g: CanvasRenderingContext2D, x: number, y: number, angle: number, color: string) {
  const s = 5;
  g.save();
  g.translate(x, y);
  g.rotate(angle);
  g.fillStyle = color;
  g.beginPath();
  g.moveTo(0, 0);
  g.lineTo(-s, -s * 0.7);
  g.lineTo(-s, s * 0.7);
  g.closePath();
  g.fill();
  g.restore();
}

function truncate(g: CanvasRenderingContext2D, text: string, max: number): string {
  if (g.measureText(text).width <= max) return text;
  let t = text;
  while (t.length > 1 && g.measureText(`${t}…`).width > max) t = t.slice(0, -1);
  return `${t}…`;
}

function plate(g: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, fill: string) {
  g.fillStyle = fill;
  g.beginPath();
  g.roundRect(x, y, w, h, 5);
  g.fill();
}

/** Word-wrap into at most two lines; the second is truncated if still too long. */
function wrap2(g: CanvasRenderingContext2D, text: string, max: number): string[] {
  if (g.measureText(text).width <= max) return [text];
  const words = text.split(/\s+/);
  let first = words[0];
  let i = 1;
  while (i < words.length && g.measureText(`${first} ${words[i]}`).width <= max) {
    first = `${first} ${words[i]}`;
    i++;
  }
  if (i >= words.length) return [truncate(g, first, max)];
  return [truncate(g, first, max), truncate(g, words.slice(i).join(" "), max)];
}
