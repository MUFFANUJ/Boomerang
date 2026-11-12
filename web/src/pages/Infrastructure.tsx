import { useEffect, useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { ApiError, api } from "../api/client";
import type { ComponentHealth, ConsumerGroup, InfraSnapshot, InfraWorker, PartitionLag } from "../api/types";
import { IconInfo, IconInfra, IconMinus, IconPlus, IconSkull, IconStop } from "../components/Icons";
import { Sparkline } from "../components/Sparkline";
import { StatTile } from "../components/StatTile";
import {
  Card,
  ComponentStatusBadge,
  EmptyState,
  Legend,
  Loading,
  PageHead,
  WorkerStatusBadge,
  type LegendEntry,
} from "../components/ui";
import { C, SERIES_OTHER, useStableSlots } from "../lib/colors";
import { fmtAxis, fmtDuration, fmtInt, fmtMs, fmtNum, fmtRate, fmtRelative } from "../lib/format";
import { useAction, useNow } from "../lib/hooks";
import { DELIVERY_GROUP, useLiveSelector } from "../lib/live";
import { useConfirm } from "../lib/confirm";
import { useToast } from "../lib/toast";
import "./infra.css";

const MIN_REPLICAS = 1;
const MAX_REPLICAS = 10;

function scalerError(e: unknown): string {
  if (e instanceof ApiError && (e.code === "scaler_unavailable" || e.status === 501)) {
    return "Scaler unavailable: the API is not running with the Docker socket or in-cluster.";
  }
  return e instanceof Error ? e.message : String(e);
}

export function InfraPage() {
  const infra = useLiveSelector((s) => s.infra);
  const lagSeries = useLiveSelector((s) => s.lagSeries);
  const wsStatus = useLiveSelector((s) => s.status);

  // Worker colors: stable per worker id, shared by worker cards and the lag chart.
  const workerIds = useMemo(() => (infra ? [...infra.workers.map((w) => w.id)].sort() : []), [infra]);
  const deliveryGroup = infra?.consumer_groups.find((g) => g.group === DELIVERY_GROUP) ?? null;
  const ownerIds = useMemo(() => {
    const s = new Set<string>();
    for (const p of deliveryGroup?.partitions ?? []) if (p.owner) s.add(p.owner);
    return [...s].sort();
  }, [deliveryGroup]);
  const allIds = useMemo(() => [...new Set([...workerIds, ...ownerIds])], [workerIds, ownerIds]);
  const colors = useStableSlots(allIds, allIds);

  if (!infra) {
    return (
      <>
        <PageHead title="Infrastructure" sub="Workers, Kafka consumer lag, retry tiers and backing services." />
        <Card>
          {wsStatus === "open" || wsStatus === "connecting" ? (
            <Loading label="Waiting for the first infra snapshot (sent every 2 s)…" />
          ) : (
            <EmptyState title="No infrastructure snapshot" icon={<IconInfra size={18} />}>
              The realtime stream is offline and <code>/api/infra</code> did not answer. This page fills in as soon as
              either responds.
            </EmptyState>
          )}
        </Card>
      </>
    );
  }

  const otherGroups = infra.consumer_groups.filter((g) => g.group !== DELIVERY_GROUP);

  return (
    <>
      <PageHead
        title="Infrastructure"
        sub="Workers, Kafka consumer lag, retry tiers and backing services. Kill a worker and watch its partitions move."
      />

      <WorkersSection infra={infra} colors={colors} />

      <div className="infra-lag-layout">
        <LagChart group={deliveryGroup} colors={colors} />
        <div className="stack">
          <Card title="Total delivery lag" sub={`${DELIVERY_GROUP} · last 5 min`}>
            <div className="lag-total">{deliveryGroup ? fmtInt(deliveryGroup.total_lag) : "—"}</div>
            <div className="muted" style={{ fontSize: 12, marginBottom: 8 }}>
              messages behind
              {deliveryGroup ? ` · ${deliveryGroup.members} member${deliveryGroup.members === 1 ? "" : "s"}` : ""}
            </div>
            <Sparkline
              data={lagSeries}
              series={[{ key: "lag", label: "Total lag", color: C.accent }]}
              area
              height={64}
              format={(v) => `${fmtInt(v)} msgs`}
            />
          </Card>
          <OtherGroups groups={otherGroups} />
        </div>
      </div>

      <div className="grid grid-2">
        <RetryTiers infra={infra} />
        <Components components={infra.components} />
      </div>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* Workers + scaling                                                   */
/* ------------------------------------------------------------------ */

function WorkersSection({ infra, colors }: { infra: InfraSnapshot; colors: Map<string, string> }) {
  const toast = useToast();
  const confirm = useConfirm();
  const now = useNow(2000);
  const { scaler } = infra;
  const current = scaler.desired_replicas ?? infra.workers.length;

  const [replicas, setReplicas] = useState<number>(Math.min(MAX_REPLICAS, Math.max(MIN_REPLICAS, current)));
  const [touched, setTouched] = useState(false);
  useEffect(() => {
    if (!touched) setReplicas(Math.min(MAX_REPLICAS, Math.max(MIN_REPLICAS, current || MIN_REPLICAS)));
  }, [current, touched]);

  const [applyScale, scaling] = useAction(async () => {
    try {
      const r = await api.scaleWorkers(replicas);
      toast.success(`Scaling workers to ${r.replicas}`);
      setTouched(false);
    } catch (e) {
      toast.error(scalerError(e));
    }
  });

  const [kill, killing] = useAction(async (w: InfraWorker) => {
    const ok = await confirm({
      title: `Kill ${w.id}?`,
      body: (
        <>
          Sends <strong>SIGKILL</strong> with no graceful shutdown. In-flight deliveries are not committed, so Kafka
          will reassign partitions {w.partitions.length ? w.partitions.join(", ") : ""} and other workers will
          redeliver them. Receivers dedupe on <code>Boomerang-Id</code>.
        </>
      ),
      confirmLabel: "Kill worker",
      danger: true,
    });
    if (!ok) return;
    try {
      const r = await api.killWorker(w.id);
      toast.success(`Killed ${r.killed}`);
    } catch (e) {
      toast.error(scalerError(e));
    }
  });

  const [stop, stopping] = useAction(async (w: InfraWorker) => {
    const ok = await confirm({
      title: `Stop ${w.id}?`,
      body: "Sends SIGTERM. The worker finishes in-flight deliveries, commits offsets and leaves the consumer group.",
      confirmLabel: "Stop worker",
    });
    if (!ok) return;
    try {
      await api.stopWorker(w.id);
      toast.success(`Stopping ${w.id}`);
    } catch (e) {
      toast.error(scalerError(e));
    }
  });

  const disabled = !scaler.enabled;
  const workers = [...infra.workers].sort((a, b) => a.id.localeCompare(b.id));

  return (
    <Card
      title="Delivery workers"
      sub={`${infra.workers.length} reporting · scaler: ${scaler.kind}${
        scaler.desired_replicas !== null ? ` · desired ${scaler.desired_replicas}` : ""
      }`}
      actions={
        <div className="scale-ctl">
          <span className="muted" style={{ fontSize: 12 }}>
            Replicas
          </span>
          <div className="stepper">
            <button
              className="btn btn-sm btn-icon"
              aria-label="Fewer replicas"
              disabled={disabled || replicas <= MIN_REPLICAS}
              onClick={() => {
                setTouched(true);
                setReplicas((r) => Math.max(MIN_REPLICAS, r - 1));
              }}
            >
              <IconMinus size={13} />
            </button>
            <span className="stepper-value tnum">{replicas}</span>
            <button
              className="btn btn-sm btn-icon"
              aria-label="More replicas"
              disabled={disabled || replicas >= MAX_REPLICAS}
              onClick={() => {
                setTouched(true);
                setReplicas((r) => Math.min(MAX_REPLICAS, r + 1));
              }}
            >
              <IconPlus size={13} />
            </button>
          </div>
          <button
            className="btn btn-primary btn-sm"
            disabled={disabled || scaling || replicas === current}
            onClick={() => void applyScale()}
          >
            Apply
          </button>
        </div>
      }
    >
      <div className="stack">
        {disabled && (
          <div className="notice notice-info">
            <IconInfo />
            <div>
              Scaling and kill/stop controls are disabled: no scaler is available (kind <code>{scaler.kind}</code>). The
              API needs the Docker socket mounted, or must run in-cluster on Kubernetes, to manage worker replicas.
            </div>
          </div>
        )}
        {workers.length === 0 ? (
          <EmptyState compact title="No workers reporting" icon={<IconInfra size={18} />}>
            Deliveries queue up in Kafka until a worker joins the consumer group.
          </EmptyState>
        ) : (
          <div className="worker-grid">
            {workers.map((w) => (
              <div className={`worker-card worker-${w.status}`} key={w.id}>
                <div className="worker-head">
                  <span className="legend-rect" style={{ background: colors.get(w.id) ?? SERIES_OTHER }} />
                  <span className="mono worker-id" title={w.id}>
                    {w.id}
                  </span>
                  <WorkerStatusBadge status={w.status} />
                </div>
                <div className="muted worker-host mono" title={w.host}>
                  {w.host}
                </div>
                <div className="worker-metrics">
                  <div>
                    <span className="muted">eps</span>
                    <strong>{fmtRate(w.eps)}</strong>
                  </div>
                  <div>
                    <span className="muted">in-flight</span>
                    <strong>{fmtInt(w.inflight)}</strong>
                  </div>
                  <div>
                    <span className="muted">processed</span>
                    <strong>{fmtNum(w.processed_total)}</strong>
                  </div>
                </div>
                <div>
                  <div className="worker-sub muted">
                    {w.partitions.length} partition{w.partitions.length === 1 ? "" : "s"}
                  </div>
                  <div className="chips">
                    {w.partitions.length ? (
                      [...w.partitions]
                        .sort((a, b) => a - b)
                        .map((p) => (
                          <span className="chip" key={p}>
                            p{p}
                          </span>
                        ))
                    ) : (
                      <span className="muted" style={{ fontSize: 12 }}>
                        none assigned
                      </span>
                    )}
                  </div>
                </div>
                <div className="worker-foot">
                  <span className="muted" title={`started ${w.started_at}`}>
                    seen {fmtRelative(w.last_seen, now)}
                  </span>
                  <div className="row">
                    <button
                      className="btn btn-sm"
                      disabled={disabled || stopping || w.status === "stopping"}
                      onClick={() => void stop(w)}
                      title={disabled ? "Scaler unavailable" : "Graceful stop (SIGTERM)"}
                    >
                      <IconStop size={12} /> Stop
                    </button>
                    <button
                      className="btn btn-sm btn-danger"
                      disabled={disabled || killing}
                      onClick={() => void kill(w)}
                      title={disabled ? "Scaler unavailable" : "Kill immediately (SIGKILL)"}
                    >
                      <IconSkull size={12} /> Kill (SIGKILL)
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </Card>
  );
}

/* ------------------------------------------------------------------ */
/* Lag per partition                                                   */
/* ------------------------------------------------------------------ */

interface LagRow extends PartitionLag {
  label: string;
  color: string;
}

function LagTooltip({ active, payload }: { active?: boolean; payload?: { payload: LagRow }[] }) {
  if (!active || !payload?.length) return null;
  const r = payload[0].payload;
  return (
    <div className="chart-tip">
      <div className="chart-tip-title">Partition {r.partition}</div>
      <div className="chart-tip-row">
        <span className="legend-rect" style={{ background: r.color }} />
        <strong>{fmtInt(r.lag)}</strong>
        <span className="label">lag</span>
      </div>
      <div className="chart-tip-row">
        <span className="label">owner</span>
        <strong className="mono" style={{ fontWeight: 500 }}>
          {r.owner ?? "unassigned"}
        </strong>
      </div>
      <div className="chart-tip-row">
        <span className="label">committed / end</span>
        <strong>
          {fmtInt(r.committed)} / {fmtInt(r.end)}
        </strong>
      </div>
    </div>
  );
}

function LagChart({ group, colors }: { group: ConsumerGroup | null; colors: Map<string, string> }) {
  const rows: LagRow[] = useMemo(
    () =>
      [...(group?.partitions ?? [])]
        .sort((a, b) => a.partition - b.partition)
        .map((p) => ({
          ...p,
          label: `p${p.partition}`,
          color: p.owner ? (colors.get(p.owner) ?? SERIES_OTHER) : SERIES_OTHER,
        })),
    [group, colors],
  );

  const legend: LegendEntry[] = useMemo(() => {
    const owners = [...new Set(rows.map((r) => r.owner))];
    const items: LegendEntry[] = owners
      .filter((o): o is string => !!o)
      .sort()
      .map((o) => ({ label: <span className="mono">{o}</span>, color: colors.get(o) ?? SERIES_OTHER, kind: "rect" }));
    if (owners.includes(null)) items.push({ label: "Unassigned", color: SERIES_OTHER, kind: "rect" });
    return items;
  }, [rows, colors]);

  return (
    <Card
      title="Consumer lag per partition"
      sub={group ? `${group.group} on ${group.topic} · bars colored by owning worker` : DELIVERY_GROUP}
      actions={legend.length > 0 ? <Legend items={legend} /> : undefined}
    >
      {!group ? (
        <EmptyState compact title={`Consumer group ${DELIVERY_GROUP} not reported`} icon={<IconInfra size={18} />} />
      ) : rows.length === 0 ? (
        <EmptyState compact title="No partitions reported" />
      ) : (
        <>
          <div className="chart-wrap" style={{ height: 260 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }} barCategoryGap={2}>
                <CartesianGrid stroke={C.grid} vertical={false} />
                <XAxis dataKey="label" stroke={C.axis} tickLine={false} interval="preserveStartEnd" />
                <YAxis tickFormatter={fmtAxis} stroke={C.axis} tickLine={false} axisLine={false} width={48} allowDecimals={false} />
                <Tooltip cursor={{ fill: "var(--neutral-soft)" }} content={<LagTooltip />} />
                <Bar dataKey="lag" name="Lag" maxBarSize={24} radius={[4, 4, 0, 0]} isAnimationActive={false} minPointSize={1}>
                  {rows.map((r) => (
                    <Cell key={r.partition} fill={r.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <details className="collapsible" style={{ marginTop: 8 }}>
            <summary>Partition table</summary>
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Partition</th>
                    <th>Owner</th>
                    <th className="num">Lag</th>
                    <th className="num">Committed</th>
                    <th className="num">End</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r) => (
                    <tr key={r.partition}>
                      <td>{r.partition}</td>
                      <td>
                        <span className="row">
                          <span className="legend-rect" style={{ background: r.color }} />
                          <span className="mono">{r.owner ?? "unassigned"}</span>
                        </span>
                      </td>
                      <td className="num">{fmtInt(r.lag)}</td>
                      <td className="num">{fmtInt(r.committed)}</td>
                      <td className="num">{fmtInt(r.end)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
        </>
      )}
    </Card>
  );
}

function OtherGroups({ groups }: { groups: ConsumerGroup[] }) {
  return (
    <Card title="Other consumer groups" bodyClassName="card-body flush-table">
      {groups.length === 0 ? (
        <EmptyState compact title="No other groups reported" />
      ) : (
        <div className="table-wrap">
          <table className="table compact-table">
            <thead>
              <tr>
                <th>Group</th>
                <th className="num">Members</th>
                <th className="num">Lag</th>
              </tr>
            </thead>
            <tbody>
              {groups.map((g) => (
                <tr key={`${g.group}:${g.topic}`}>
                  <td>
                    <div className="mono">{g.group}</div>
                    <div className="muted mono" style={{ fontSize: 11 }}>
                      {g.topic}
                    </div>
                  </td>
                  <td className="num">{fmtInt(g.members)}</td>
                  <td className="num">{fmtInt(g.total_lag)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

/* ------------------------------------------------------------------ */
/* Retry tiers, DLQ, components                                        */
/* ------------------------------------------------------------------ */

function RetryTiers({ infra }: { infra: InfraSnapshot }) {
  const tiers = [...infra.retry_tiers].sort((a, b) => a.tier - b.tier);
  const max = Math.max(1, ...tiers.map((t) => t.depth), infra.dlq.depth);
  return (
    <Card title="Retry tiers & dead-letter queue" sub="Messages waiting in each delayed-retry topic">
      <div className="stack">
        <div className="grid grid-2">
          <StatTile
            label="DLQ depth"
            icon={<IconSkull size={13} />}
            value={fmtNum(infra.dlq.depth)}
            sub="dead deliveries awaiting replay"
          />
          <StatTile
            label="DLQ topic total"
            value={fmtNum(infra.dlq.topic_total)}
            sub="messages ever written to the DLQ topic"
          />
        </div>
        {tiers.length === 0 ? (
          <EmptyState compact title="No retry tiers reported" />
        ) : (
          <div className="tier-list">
            {tiers.map((t) => (
              <div className="tier-row" key={t.tier}>
                <div className="tier-name">
                  <strong>Tier {t.tier}</strong>
                  <span className="muted">
                    ~{fmtDuration(t.delay_hint_ms)} · <span className="mono">{t.topic}</span>
                  </span>
                </div>
                <div className="tier-bar" aria-hidden="true">
                  <span style={{ width: `${(t.depth / max) * 100}%` }} />
                </div>
                <div className="tier-depth tnum">{fmtInt(t.depth)}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </Card>
  );
}

const COMPONENT_LABEL: Record<ComponentHealth["name"], string> = {
  kafka: "Kafka",
  redis: "Redis",
  postgres: "PostgreSQL",
  clickhouse: "ClickHouse",
  s3: "Object storage (S3)",
};

function Components({ components }: { components: ComponentHealth[] }) {
  return (
    <Card title="Component health" sub="Backing services probed by the realtime service">
      {components.length === 0 ? (
        <EmptyState compact title="No component health reported" />
      ) : (
        <div className="component-grid">
          {components.map((c) => (
            <div className={`component component-${c.status}`} key={c.name}>
              <div className="row row-between">
                <strong>{COMPONENT_LABEL[c.name] ?? c.name}</strong>
                <ComponentStatusBadge status={c.status} />
              </div>
              <div className="component-latency tnum">{c.latency_ms === null ? "—" : fmtMs(c.latency_ms)}</div>
              <div className="muted component-detail" title={c.detail}>
                {c.detail || " "}
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
