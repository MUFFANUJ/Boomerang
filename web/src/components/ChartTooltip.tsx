// Recharts tooltip content: values lead, labels follow, keyed by short line strokes.
import type { TooltipProps } from "recharts";

export function ChartTooltip({
  active,
  payload,
  label,
  labelFormatter,
  valueFormatter = (v: number) => v.toLocaleString(),
  keyKind = "line",
}: TooltipProps<number, string> & {
  labelFormatter?: (l: unknown) => string;
  valueFormatter?: (v: number, name: string) => string;
  keyKind?: "line" | "rect";
}) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="chart-tip">
      <div className="chart-tip-title">{labelFormatter ? labelFormatter(label) : String(label)}</div>
      {payload.map((p) => {
        const color = (p.color as string) || (p.payload?.fill as string) || "var(--text-muted)";
        return (
          <div className="chart-tip-row" key={String(p.dataKey ?? p.name)}>
            <span
              className={keyKind === "rect" ? "legend-rect" : "legend-line"}
              style={{ background: color }}
            />
            <strong>
              {typeof p.value === "number" ? valueFormatter(p.value, String(p.name ?? "")) : "—"}
            </strong>
            <span className="label">{p.name}</span>
          </div>
        );
      })}
    </div>
  );
}
