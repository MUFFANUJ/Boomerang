import type { ReactNode } from "react";

/** Stat tile: label, value (proportional figures), optional sub line and trend. */
export function StatTile({
  label,
  value,
  unit,
  sub,
  trend,
  icon,
  empty,
}: {
  label: ReactNode;
  value: ReactNode;
  unit?: ReactNode;
  sub?: ReactNode;
  trend?: ReactNode;
  icon?: ReactNode;
  /** Render the value muted (no data yet). */
  empty?: boolean;
}) {
  return (
    <div className="tile">
      <div className="tile-label">
        {icon}
        {label}
      </div>
      <div className={`tile-value${empty ? " is-empty" : ""}`}>
        {value}
        {unit && !empty && <span className="tile-unit">{unit}</span>}
      </div>
      <div className="tile-sub">{sub}</div>
      {trend && <div className="tile-spark">{trend}</div>}
    </div>
  );
}
