import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { api, isAbort } from "../api/client";
import type { IntegrityData, StatsEndpoint } from "../api/types";
import { ChartTooltip } from "../components/ChartTooltip";
import { IconAlert, IconShield } from "../components/Icons";
import { Sparkline } from "../components/Sparkline";
import { StatTile } from "../components/StatTile";
import {
  Card,
  CircuitBadge,
  EmptyState,
  EndpointStatusBadge,
  Legend,
  Meter,
  PageHead,
  SegBar,
  successTone,
} from "../components/ui";
import { C } from "../lib/colors";
import { fmtAxis, fmtInt, fmtMs, fmtNum, fmtPct, fmtRate, fmtTime } from "../lib/format";
import { useLive, useLiveStore, WINDOW_MS } from "../lib/live";
import { useNow } from "../lib/hooks";
import { useTenant } from "../lib/tenant";
import "./overview.css";

export function OverviewPage() {
  const live = useLive();
  const store = useLiveStore();
  const { tenantId } = useTenant();
  const { stats, integrity, series, endpointSeries, status } = live;
  const [seedError, setSeedError] = useState(false);

  // Seed the 5-minute throughput chart from history, then live ticks append.
  useEffect(() => {
    const ctrl = new AbortController();
    api
      .getTimeseries(5, undefined, ctrl.signal)
      .then((r) => store.seedThroughput(r, "global-5m"))
      .catch((err) => {
        if (!isAbort(err)) setSeedError(true);
      });
    return () => ctrl.abort();
  }, [store]);

  const tenantEndpoints = useMemo(
    () => (stats?.endpoints ?? []).filter((e) => e.tenant_id === tenantId),
    [stats, tenantId],
  );

  const recentSpark = useMemo(() => series.filter((p) => !p.seeded).slice(-60), [series]);

  return (
    <>
      <PageHead
        title="Overview"
        sub="Live delivery health across the platform. Counters and throughput cover all tenants; endpoint health is scoped to the selected tenant."
      />

      {!stats && (
        <div className="notice notice-info">
          <span className="spinner" />
          <div>
            {status === "open"
              ? "Connected. Waiting for the first stats tick…"
              : "Waiting for the live stream. Numbers appear as soon as the backend sends its first snapshot."}
          </div>
        </div>
      )}

      <div className="grid grid-4">
        <StatTile
          label="Received"
          value={fmtNum(stats?.totals.received)}
          empty={!stats}
          sub={stats ? `${fmtRate(stats.rates.received_eps)} events/s now` : "No data yet"}
          trend={
            <Sparkline
              data={recentSpark}
              series={[{ key: "received", label: "received/s", color: C.accent }]}
              height={32}
              area
              format={(v) => `${fmtRate(v)}/s`}
            />
          }
        />
        <StatTile
          label="Delivered"
          value={fmtNum(stats?.totals.delivered)}
          empty={!stats}
          sub={stats ? `${fmtRate(stats.rates.delivered_eps)} deliveries/s now` : "No data yet"}
          trend={
            <Sparkline
              data={recentSpark}
              series={[{ key: "delivered", label: "delivered/s", color: C.accent }]}
              height={32}
              area
              format={(v) => `${fmtRate(v)}/s`}
            />
          }
        />
        <StatTile
          label="Retrying"
          value={fmtNum(stats?.totals.retrying)}
          empty={!stats}
          sub={
            stats
              ? `${fmtNum(stats.totals.failed_attempts)} failed attempts · ${fmtRate(stats.rates.deferred_eps)} deferred/s`
              : "No data yet"
          }
          trend={
            <Sparkline
              data={recentSpark}
              series={[{ key: "failed", label: "failed/s", color: C.accent }]}
              height={32}
              area
              format={(v) => `${fmtRate(v)}/s`}
            />
          }
        />
        <StatTile
          label="Dead-letter queue"
          value={fmtNum(stats?.totals.dlq)}
          empty={!stats}
          sub={
            stats ? (
              stats.totals.dlq > 0 ? (
                <Link to="/endpoints">Replay from Endpoints →</Link>
              ) : (
                "Nothing dead-lettered"
              )
            ) : (
              "No data yet"
            )
          }
        />
      </div>

      <div className="ov-main">
        <ThroughputCard seedError={seedError} />
        <IntegrityPanel data={integrity} />
      </div>

      <div className="grid grid-4">
        <StatTile
          label="Success rate (60s)"
          value={fmtPct(stats?.success_rate)}
          empty={!stats || stats.success_rate === null}
          sub={
            stats?.success_rate === null || !stats ? (
              "No attempts in the last minute"
            ) : (
              <Meter
                value={stats.success_rate}
                tone={successTone(stats.success_rate) === "good" ? "good" : successTone(stats.success_rate) === "warning" ? "warning" : "critical"}
                label="Success rate"
              />
            )
          }
        />
        <StatTile
          label="Latency p50"
          value={fmtMs(stats?.latency_ms?.p50)}
          empty={!stats?.latency_ms}
          sub="Delivery latency, last 60s"
        />
        <StatTile
          label="Latency p95"
          value={fmtMs(stats?.latency_ms?.p95)}
          empty={!stats?.latency_ms}
          sub="Delivery latency, last 60s"
        />
        <StatTile
          label="Latency p99"
          value={fmtMs(stats?.latency_ms?.p99)}
          empty={!stats?.latency_ms}
          sub="Delivery latency, last 60s"
        />
      </div>

      <Card
        title="Endpoint health"
        sub="Live from the delivery workers, per endpoint of the selected tenant"
        actions={
          <>
            <Legend
              items={[
                { label: "Delivered/s", color: C.accent },
                { label: "Failed/s", color: C.critical },
              ]}
            />
            <Link className="btn btn-sm" to="/endpoints">
              Manage endpoints
            </Link>
          </>
        }
      >
        {!stats ? (
          <EmptyState compact title="Waiting for live data" />
        ) : tenantEndpoints.length === 0 ? (
          <EmptyState compact title="No endpoints for this tenant">
            <Link to="/endpoints">Create an endpoint</Link> to start receiving webhooks.
          </EmptyState>
        ) : (
          <div className="grid grid-auto">
            {tenantEndpoints.map((ep) => (
              <EndpointHealthCard key={ep.id} ep={ep} series={endpointSeries[ep.id] ?? []} />
            ))}
          </div>
        )}
      </Card>
    </>
  );
}

function ThroughputCard({ seedError }: { seedError: boolean }) {
  const series = useLive().series;
  const now = useNow(1000);
  const lastT = series.length ? series[series.length - 1].t : now;
  const domain: [number, number] = [lastT - WINDOW_MS, lastT];
  const hasData = series.length > 1;

  return (
    <Card
      title="Throughput"
      sub={`Last 5 minutes, per second${seedError ? " · history unavailable, live ticks only" : ""}`}
      actions={
        <Legend
          items={[
            { label: "Delivered", color: C.accent },
            { label: "Received", color: C.context },
            { label: "Failed attempts", color: C.critical },
          ]}
        />
      }
    >
      <div className="chart-wrap" style={{ height: 260 }}>
        {hasData ? (
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={series} margin={{ top: 8, right: 12, bottom: 0, left: -8 }}>
              <CartesianGrid stroke={C.grid} vertical={false} />
              <XAxis
                dataKey="t"
                type="number"
                scale="time"
                domain={domain}
                tickFormatter={(t: number) => fmtTime(t)}
                stroke={C.axis}
                tickLine={false}
                minTickGap={60}
              />
              <YAxis tickFormatter={fmtAxis} stroke="transparent" tickLine={false} width={52} allowDecimals={false} />
              <Tooltip
                isAnimationActive={false}
                cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
                content={
                  <ChartTooltip
                    labelFormatter={(l) => fmtTime(Number(l))}
                    valueFormatter={(v) => `${fmtRate(v)}/s`}
                  />
                }
              />
              <Line
                dataKey="received"
                name="Received"
                stroke={C.context}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
                connectNulls={false}
              />
              <Line
                dataKey="failed"
                name="Failed attempts"
                stroke={C.critical}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
              />
              <Line
                dataKey="delivered"
                name="Delivered"
                stroke={C.accent}
                strokeWidth={2}
                dot={false}
                isAnimationActive={false}
                activeDot={{ r: 4, stroke: "var(--surface-1)", strokeWidth: 2 }}
              />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <EmptyState title="No throughput yet">
            The chart fills from metrics history and live ticks. Start traffic in{" "}
            <Link to="/traffic">Traffic Control</Link>.
          </EmptyState>
        )}
      </div>
    </Card>
  );
}

function IntegrityPanel({ data }: { data: IntegrityData | null }) {
  if (!data) {
    return (
      <Card title="Integrity" sub="Exactly what was created, delivered and confirmed">
        <EmptyState title="Waiting for the integrity check">The audit runs every 3 seconds.</EmptyState>
      </Card>
    );
  }
  const ok = data.lost === 0;
  return (
    <Card
      title="Integrity"
      sub={`Audited ${fmtTime(data.ts)} · all tenants`}
      className={`integrity ${ok ? "is-ok" : "is-lost"}`}
    >
      <div className="integrity-hero">
        <div className={`integrity-icon ${ok ? "ok" : "bad"}`}>{ok ? <IconShield size={26} /> : <IconAlert size={26} />}</div>
        <div>
          <div className="integrity-label">Lost</div>
          <div className="integrity-value">{fmtInt(data.lost)}</div>
        </div>
        <div className="integrity-claim">
          {ok ? (
            <>
              <strong>No events lost.</strong> Every delivery marked successful was confirmed by its receiver.
            </>
          ) : (
            <>
              <strong>{fmtInt(data.lost)} deliveries unconfirmed.</strong> Marked delivered but not seen by the
              receivers.
            </>
          )}
        </div>
      </div>

      <div className="stack-sm" style={{ marginTop: 16 }}>
        <SegBar
          label="Deliveries created"
          parts={[
            { name: "Delivered", value: data.delivered, color: "var(--good)" },
            { name: "In progress", value: data.in_progress, color: "var(--series-1)" },
            { name: "Dead", value: data.dead, color: "var(--critical)" },
          ]}
        />
        <Legend
          items={[
            { label: "Delivered", color: "var(--good)", kind: "rect" },
            { label: "In progress", color: "var(--series-1)", kind: "rect" },
            { label: "Dead", color: "var(--critical)", kind: "rect" },
          ]}
        />
      </div>

      <dl className="integrity-rows">
        <div>
          <dt>Deliveries created</dt>
          <dd>{fmtInt(data.deliveries_created)}</dd>
        </div>
        <div>
          <dt>Delivered</dt>
          <dd>{fmtInt(data.delivered)}</dd>
        </div>
        <div>
          <dt>Dead-lettered</dt>
          <dd>{fmtInt(data.dead)}</dd>
        </div>
        <div>
          <dt>In progress</dt>
          <dd>{fmtInt(data.in_progress)}</dd>
        </div>
        <div>
          <dt>Confirmed by receivers</dt>
          <dd>{fmtInt(data.receivers_unique)}</dd>
        </div>
        <div>
          <dt title="Extra copies received: at-least-once delivery in action, deduplicated by Boomerang-Id">
            Duplicates received
          </dt>
          <dd>{fmtInt(data.receivers_duplicates)}</dd>
        </div>
      </dl>
    </Card>
  );
}

function EndpointHealthCard({
  ep,
  series,
}: {
  ep: StatsEndpoint;
  series: { t: number; delivered: number; failed: number }[];
}) {
  return (
    <div className="ov-ep">
      <div className="row row-between" style={{ alignItems: "flex-start" }}>
        <div className="grow">
          <div className="ov-ep-name">{ep.name}</div>
          <div className="row row-wrap" style={{ marginTop: 6, gap: 6 }}>
            <EndpointStatusBadge status={ep.status} small />
            {ep.status !== "circuit_open" && <CircuitBadge state={ep.circuit} small />}
          </div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div className="ov-ep-rate">{fmtPct(ep.success_rate)}</div>
          <div className="muted" style={{ fontSize: 11 }}>
            success
          </div>
        </div>
      </div>
      <Sparkline
        data={series.slice(-90)}
        series={[
          { key: "delivered", label: "delivered/s", color: C.accent },
          { key: "failed", label: "failed/s", color: C.critical },
        ]}
        height={36}
        format={(v) => `${fmtRate(v)}/s`}
      />
      <div className="ov-ep-stats">
        <span>
          <strong>{fmtRate(ep.delivered_eps)}</strong> delivered/s
        </span>
        <span>
          <strong>{fmtRate(ep.failed_eps)}</strong> failed/s
        </span>
        <span>
          p95 <strong>{fmtMs(ep.p95_ms)}</strong>
        </span>
        {ep.rate_limit_effective_rps !== null && (
          <span>
            limit <strong>{fmtRate(ep.rate_limit_effective_rps)}</strong>/s
          </span>
        )}
      </div>
    </div>
  );
}
