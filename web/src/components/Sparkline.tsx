// Lightweight SVG sparkline with crosshair + tooltip. Multi-series on one shared y-scale.
import { useMemo, useState, type PointerEvent as RPointerEvent } from "react";
import { useElementSize } from "../lib/hooks";
import { fmtTime } from "../lib/format";

export interface SparkSeries {
  key: string;
  label: string;
  color: string;
}

export function Sparkline<T extends { t: number }>({
  data,
  series,
  height = 40,
  area = false,
  format = (v: number) => String(v),
  minY,
  ariaLabel,
}: {
  data: T[];
  series: SparkSeries[];
  height?: number;
  area?: boolean;
  format?: (v: number) => string;
  /** Lower bound of the y-domain (default 0). */
  minY?: number;
  ariaLabel?: string;
}) {
  const [ref, { width }] = useElementSize<HTMLDivElement>();
  const [hover, setHover] = useState<number | null>(null);
  const pad = 3;

  const geom = useMemo(() => {
    if (data.length < 2 || width <= 0) return null;
    const t0 = data[0].t;
    const t1 = data[data.length - 1].t;
    let max = -Infinity;
    let min = minY ?? 0;
    for (const d of data)
      for (const s of series) {
        const v = (d as unknown as Record<string, number | null>)[s.key];
        if (typeof v === "number" && Number.isFinite(v)) {
          if (v > max) max = v;
          if (minY === undefined && v < min) min = v;
        }
      }
    if (!Number.isFinite(max) || max <= min) max = min + 1;
    const x = (t: number) => (t1 === t0 ? 0 : ((t - t0) / (t1 - t0)) * width);
    const y = (v: number) => pad + (1 - (v - min) / (max - min)) * (height - pad * 2);
    const paths = series.map((s) => {
      let d = "";
      let started = false;
      let firstX = 0;
      let lastX = 0;
      for (const p of data) {
        const v = (p as unknown as Record<string, number | null>)[s.key];
        if (typeof v !== "number" || !Number.isFinite(v)) {
          started = false;
          continue;
        }
        const px = x(p.t);
        const py = y(v);
        if (!started) firstX = px;
        d += `${started ? "L" : "M"}${px.toFixed(1)},${py.toFixed(1)}`;
        lastX = px;
        started = true;
      }
      const areaD = d ? `${d}L${lastX.toFixed(1)},${height}L${firstX.toFixed(1)},${height}Z` : "";
      return { key: s.key, color: s.color, d, areaD };
    });
    return { x, y, paths, t0, t1 };
  }, [data, series, width, height, minY]);

  const onMove = (e: RPointerEvent<HTMLDivElement>) => {
    if (!geom) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const px = e.clientX - rect.left;
    const t = geom.t0 + (px / Math.max(1, width)) * (geom.t1 - geom.t0);
    let best = 0;
    let bestD = Infinity;
    for (let i = 0; i < data.length; i++) {
      const dd = Math.abs(data[i].t - t);
      if (dd < bestD) {
        bestD = dd;
        best = i;
      }
    }
    setHover(best);
  };

  const hp = hover !== null && geom ? data[hover] : null;
  const last = data[data.length - 1];

  return (
    <div
      ref={ref}
      className="spark"
      style={{ height }}
      onPointerMove={onMove}
      onPointerLeave={() => setHover(null)}
      role="img"
      aria-label={
        ariaLabel ??
        (last
          ? series
              .map((s) => `${s.label} ${format((last as unknown as Record<string, number>)[s.key] ?? 0)}`)
              .join(", ")
          : "No data yet")
      }
    >
      {geom ? (
        <svg width={width} height={height}>
          {area &&
            geom.paths.map((p) => (
              <path key={`a-${p.key}`} d={p.areaD} fill={p.color} fillOpacity={0.1} stroke="none" />
            ))}
          {geom.paths.map((p) => (
            <path
              key={p.key}
              d={p.d}
              fill="none"
              stroke={p.color}
              strokeWidth={2}
              strokeLinejoin="round"
              strokeLinecap="round"
            />
          ))}
          {hp && (
            <>
              <line x1={geom.x(hp.t)} x2={geom.x(hp.t)} y1={0} y2={height} stroke="var(--axis)" strokeWidth={1} />
              {series.map((s) => {
                const v = (hp as unknown as Record<string, number | null>)[s.key];
                if (typeof v !== "number") return null;
                return (
                  <circle
                    key={s.key}
                    cx={geom.x(hp.t)}
                    cy={geom.y(v)}
                    r={4}
                    fill={s.color}
                    stroke="var(--surface-1)"
                    strokeWidth={2}
                  />
                );
              })}
            </>
          )}
        </svg>
      ) : (
        <svg width={width || 0} height={height}>
          <line
            x1={0}
            x2={width}
            y1={height - 1}
            y2={height - 1}
            stroke="var(--grid)"
            strokeWidth={1}
          />
        </svg>
      )}
      {hp && geom && (
        <div className="chart-tip spark-tip" style={{ left: Math.min(Math.max(geom.x(hp.t), 70), width - 70) }}>
          <div className="chart-tip-title">{fmtTime(hp.t)}</div>
          {series.map((s) => {
            const v = (hp as unknown as Record<string, number | null>)[s.key];
            return (
              <div className="chart-tip-row" key={s.key}>
                <span className="legend-line" style={{ background: s.color }} />
                <strong>{typeof v === "number" ? format(v) : "—"}</strong>
                <span className="label">{s.label}</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
