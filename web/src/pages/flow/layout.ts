// Pure geometry for the Live Event Flow canvas. Both the canvas engine and the HTML
// overlays (per-lane status strips, counters) read from the same layout object so they align.

export interface LaneDef {
  id: string;
  name: string;
}

export interface StageGeom {
  key: "shopsim" | "ingestion" | "kafka" | "workers";
  label: string;
  sub: string;
  x: number;
}

export interface LaneGeom {
  id: string;
  name: string;
  index: number;
  y: number;
  h: number;
}

export interface FlowLayout {
  width: number;
  height: number;
  pipeY: number;
  pipeTop: number;
  pipeBottom: number;
  nodeW: number;
  nodeH: number;
  stages: StageGeom[];
  workersX: number;
  laneStartX: number;
  laneEndX: number;
  /** where a dot parks while it waits for its attempt */
  waitX: number;
  /** how far a waiting dot creeps before stopping */
  creepX: number;
  recvW: number;
  recvCx: number;
  lanes: LaneGeom[];
  stripX: number;
  stripW: number;
  retry: { y: number; h: number; left: number; right: number };
  bin: { x: number; y: number; w: number; h: number };
}

const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(hi, v));

export function computeLayout(width: number, lanes: LaneDef[]): FlowLayout {
  const W = Math.max(640, width);
  const n = Math.max(1, lanes.length);

  const padL = 20;
  const stripW = clamp(Math.round(W * 0.19), 160, 220);
  const stripX = W - stripW - 12;
  const drawRight = stripX - 16;
  const recvW = clamp(Math.round(W * 0.115), 96, 140);
  const recvCx = drawRight - recvW / 2;
  const laneEndX = drawRight - recvW;
  const laneLen = (laneEndX - padL) * 0.36;
  const laneStartX = laneEndX - laneLen;

  const nodeGuess = 86;
  const x0 = padL + nodeGuess / 2;
  const x3 = laneStartX - 44 - nodeGuess / 2;
  const spacing = (x3 - x0) / 3;
  const nodeW = clamp(Math.round(spacing - 12), 60, 96);
  const nodeH = 54;

  const pipeTop = 24;
  const pipeRegion = Math.max(300, n * 64);
  const pipeBottom = pipeTop + pipeRegion;
  const pipeY = pipeTop + pipeRegion / 2;

  const stages: StageGeom[] = [
    { key: "shopsim", label: "ShopSim", sub: "traffic", x: x0 },
    { key: "ingestion", label: "Ingestion", sub: "/v1/events", x: x0 + spacing },
    { key: "kafka", label: "Kafka", sub: "events topic", x: x0 + spacing * 2 },
    { key: "workers", label: "Workers", sub: "Go delivery", x: x3 },
  ];

  const bandH = pipeRegion / n;
  const laneH = clamp(bandH - 10, 30, 84);
  const laneGeoms: LaneGeom[] = lanes.map((l, i) => ({
    id: l.id,
    name: l.name,
    index: i,
    y: pipeTop + bandH * (i + 0.5),
    h: laneH,
  }));

  const retryY = pipeBottom + 66;
  const retryH = 40;
  const height = pipeBottom + 214;

  return {
    width: W,
    height,
    pipeY,
    pipeTop,
    pipeBottom,
    nodeW,
    nodeH,
    stages,
    workersX: x3,
    laneStartX,
    laneEndX,
    waitX: laneStartX + laneLen * 0.45,
    creepX: laneStartX + laneLen * 0.84,
    recvW,
    recvCx,
    lanes: laneGeoms,
    stripX,
    stripW,
    retry: { y: retryY, h: retryH, left: x3, right: laneEndX - 24 },
    bin: { x: stripX, y: retryY - 22, w: stripW, h: height - (retryY - 22) - 16 },
  };
}
