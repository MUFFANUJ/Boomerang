import { useEffect, useMemo, useState, type FormEvent } from "react";
import { api } from "../api/client";
import type { ApiKey, CreatedApiKey, Tenant } from "../api/types";
import { IconAlert, IconKey, IconMinus, IconPlus, IconTrash } from "../components/Icons";
import { Modal } from "../components/Modal";
import { Badge, Card, CopyButton, EmptyState, ErrorNote, Loading, PageHead } from "../components/ui";
import { useConfirm } from "../lib/confirm";
import { fmtDateTime, fmtDuration, fmtRelative } from "../lib/format";
import { useAsync } from "../lib/hooks";
import { useTenant } from "../lib/tenant";
import { useToast } from "../lib/toast";
import "./settings.css";

export function SettingsPage() {
  const { tenantId, tenant, loading, error, refresh } = useTenant();
  // Fetch the tenant directly too, so settings are fresh even if the cached list is stale.
  const fresh = useAsync((s) => api.getTenant(tenantId, s), [tenantId]);
  const t = fresh.data ?? tenant;

  return (
    <>
      <PageHead title="Settings" sub="Tenant configuration: API keys, ingestion rate limits and the retry policy." />
      <ErrorNote error={fresh.error ?? error} onRetry={() => (fresh.reload(), void refresh())} />
      <TenantCard tenant={t} />
      <ApiKeysCard tenantId={tenantId} />
      {!t ? (
        loading || fresh.loading ? (
          <div className="card">
            <Loading label="Loading tenant…" />
          </div>
        ) : null
      ) : (
        <div className="set-grid">
          <RateLimitCard key={`rl-${t.id}`} tenant={t} onSaved={fresh.setData} />
          <RetryPolicyCard key={`rp-${t.id}`} tenant={t} onSaved={fresh.setData} />
        </div>
      )}
    </>
  );
}

/* ------------------------------------------------------------------ */

function TenantCard({ tenant }: { tenant: Tenant | null }) {
  const { upsert, setTenantId, tenants } = useTenant();
  const toast = useToast();
  const [name, setName] = useState("");
  const [busy, setBusy] = useState(false);

  const create = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    setBusy(true);
    try {
      const t = await api.createTenant(name.trim());
      upsert(t);
      setTenantId(t.id);
      setName("");
      toast.success(`Tenant ${t.name} created and selected`);
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card title="Tenant" sub={`${tenants.length} tenant${tenants.length === 1 ? "" : "s"} on this deployment`}>
      <div className="set-tenant">
        <div>
          {tenant ? (
            <dl className="kv">
              <dt>Name</dt>
              <dd>
                <strong>{tenant.name}</strong>
              </dd>
              <dt>ID</dt>
              <dd className="row">
                <code>{tenant.id}</code>
                <CopyButton text={tenant.id} />
              </dd>
              <dt>Created</dt>
              <dd>{fmtDateTime(tenant.created_at)}</dd>
            </dl>
          ) : (
            <span className="muted">Tenant details unavailable.</span>
          )}
        </div>
        <form className="field set-newtenant" onSubmit={create}>
          <label htmlFor="new-tenant">Create a new tenant</label>
          <div className="row">
            <input
              id="new-tenant"
              className="input grow"
              placeholder="Tenant name"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
            <button className="btn btn-primary" disabled={busy || !name.trim()}>
              {busy ? <span className="spinner" /> : <IconPlus size={14} />} Create
            </button>
          </div>
          <span className="field-hint">The new tenant becomes the selected tenant in the top bar.</span>
        </form>
      </div>
    </Card>
  );
}

/* ------------------------------------------------------------------ */

function ApiKeysCard({ tenantId }: { tenantId: string }) {
  const keys = useAsync((s) => api.listKeys(tenantId, s), [tenantId]);
  const toast = useToast();
  const confirm = useConfirm();
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState("");
  const [created, setCreated] = useState<CreatedApiKey | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const create = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    setBusy("create");
    try {
      const k = await api.createKey(tenantId, name.trim());
      setCreated(k);
      setCreating(false);
      setName("");
      const { key: _secret, ...rest } = k;
      keys.setData((prev) => [rest as ApiKey, ...(prev ?? [])]);
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(null);
    }
  };

  const revoke = async (k: ApiKey) => {
    const ok = await confirm({
      title: `Revoke "${k.name}"?`,
      body: (
        <>
          Requests using <code>{k.prefix}…</code> will be rejected with 401 immediately. This cannot be undone.
        </>
      ),
      confirmLabel: "Revoke key",
      danger: true,
    });
    if (!ok) return;
    setBusy(k.id);
    try {
      const updated = await api.revokeKey(tenantId, k.id);
      keys.setData((prev) => (prev ?? []).map((x) => (x.id === k.id ? updated : x)));
      toast.success(`Key "${k.name}" revoked`);
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(null);
    }
  };

  const list = keys.data ?? [];
  return (
    <Card
      title="API keys"
      sub="Bearer keys for the public ingestion API (POST /v1/events)"
      actions={
        <button className="btn btn-sm btn-primary" onClick={() => setCreating(true)}>
          <IconPlus size={14} /> New key
        </button>
      }
      bodyClassName=""
    >
      {keys.error ? (
        <div style={{ padding: "12px 20px" }}>
          <ErrorNote error={keys.error} onRetry={keys.reload} />
        </div>
      ) : null}
      {keys.data === undefined && keys.loading ? (
        <Loading compact label="Loading keys…" />
      ) : list.length === 0 && !keys.error ? (
        <EmptyState compact icon={<IconKey size={18} />} title="No API keys">
          Create a key to send events with <code>Authorization: Bearer bmr_…</code>.
        </EmptyState>
      ) : list.length > 0 ? (
        <div className="table-wrap" style={{ marginTop: 8 }}>
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Key</th>
                <th>Created</th>
                <th>Last used</th>
                <th>Status</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {list.map((k) => (
                <tr key={k.id} className={k.revoked_at ? "set-revoked" : ""}>
                  <td>
                    <strong>{k.name}</strong>
                  </td>
                  <td>
                    <code>{k.prefix}…</code>
                  </td>
                  <td title={fmtDateTime(k.created_at)}>{fmtRelative(k.created_at)}</td>
                  <td title={k.last_used_at ? fmtDateTime(k.last_used_at) : undefined}>
                    {k.last_used_at ? fmtRelative(k.last_used_at) : <span className="muted">never</span>}
                  </td>
                  <td>
                    {k.revoked_at ? (
                      <Badge tone="critical" small>
                        Revoked {fmtRelative(k.revoked_at)}
                      </Badge>
                    ) : (
                      <Badge tone="good" small>
                        Active
                      </Badge>
                    )}
                  </td>
                  <td style={{ textAlign: "right" }}>
                    {!k.revoked_at && (
                      <button className="btn btn-sm btn-danger" onClick={() => revoke(k)} disabled={busy !== null}>
                        {busy === k.id ? <span className="spinner" /> : <IconTrash size={14} />} Revoke
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      {creating && (
        <Modal
          title="New API key"
          onClose={() => setCreating(false)}
          footer={
            <>
              <button className="btn" onClick={() => setCreating(false)}>
                Cancel
              </button>
              <button className="btn btn-primary" type="submit" form="key-form" disabled={busy === "create" || !name.trim()}>
                {busy === "create" && <span className="spinner" />} Create key
              </button>
            </>
          }
        >
          <form id="key-form" className="field" onSubmit={create}>
            <label htmlFor="key-name">Name</label>
            <input
              id="key-name"
              className="input"
              placeholder="e.g. checkout-service production"
              value={name}
              onChange={(e) => setName(e.target.value)}
              autoFocus
            />
          </form>
        </Modal>
      )}

      {created && (
        <Modal
          title="Copy your new API key"
          onClose={() => setCreated(null)}
          footer={
            <button className="btn btn-primary" onClick={() => setCreated(null)}>
              I've stored it
            </button>
          }
        >
          <div className="notice notice-warning">
            <IconAlert size={14} />
            <div>This is the only time the full key is shown. Store it in your secret manager now.</div>
          </div>
          <div className="field">
            <span className="field-label">{created.name}</span>
            <div className="secret-box">
              <code title={created.key}>{created.key}</code>
              <CopyButton text={created.key} label="Copy" />
            </div>
          </div>
          <pre className="code wrap">{`curl -X POST http://localhost:8080/v1/events \\
  -H "Authorization: Bearer ${created.key}" \\
  -H "Content-Type: application/json" \\
  -d '{"type":"order.created","data":{"order_id":"ord_123"}}'`}</pre>
        </Modal>
      )}
    </Card>
  );
}

/* ------------------------------------------------------------------ */

function RateLimitCard({ tenant, onSaved }: { tenant: Tenant; onSaved: (t: Tenant) => void }) {
  const { upsert } = useTenant();
  const toast = useToast();
  const [rps, setRps] = useState(String(tenant.rate_limit_rps));
  const [burst, setBurst] = useState(String(tenant.rate_limit_burst));
  const [busy, setBusy] = useState(false);
  const dirty = rps !== String(tenant.rate_limit_rps) || burst !== String(tenant.rate_limit_burst);
  const rpsN = Number(rps);
  const burstN = Number(burst);
  const valid = Number.isFinite(rpsN) && rpsN > 0 && Number.isFinite(burstN) && burstN > 0;

  useEffect(() => {
    setRps(String(tenant.rate_limit_rps));
    setBurst(String(tenant.rate_limit_burst));
  }, [tenant.rate_limit_rps, tenant.rate_limit_burst]);

  const save = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      const t = await api.patchTenant(tenant.id, { rate_limit_rps: rpsN, rate_limit_burst: burstN });
      onSaved(t);
      upsert(t);
      toast.success("Rate limit saved");
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card title="Ingestion rate limit" sub="Token bucket applied per tenant on POST /v1/events. Over the limit returns 429 with Retry-After.">
      <form className="stack" onSubmit={save}>
        <div className="grid grid-2">
          <div className="field">
            <label htmlFor="rl-rps">Sustained rate</label>
            <div className="input-group">
              <input id="rl-rps" className="input" type="number" min={1} value={rps} onChange={(e) => setRps(e.target.value)} />
              <span className="input-suffix">req/s</span>
            </div>
          </div>
          <div className="field">
            <label htmlFor="rl-burst">Burst</label>
            <div className="input-group">
              <input
                id="rl-burst"
                className="input"
                type="number"
                min={1}
                value={burst}
                onChange={(e) => setBurst(e.target.value)}
              />
              <span className="input-suffix">requests</span>
            </div>
          </div>
        </div>
        {!valid && <span className="field-error">Both values must be positive numbers.</span>}
        <div className="row">
          <button className="btn btn-primary" disabled={!dirty || !valid || busy}>
            {busy && <span className="spinner" />} Save rate limit
          </button>
          {dirty && (
            <button
              type="button"
              className="btn btn-ghost"
              onClick={() => {
                setRps(String(tenant.rate_limit_rps));
                setBurst(String(tenant.rate_limit_burst));
              }}
            >
              Reset
            </button>
          )}
        </div>
      </form>
    </Card>
  );
}

/* ------------------------------------------------------------------ */

type Unit = "ms" | "s" | "min" | "h";
const UNIT_MS: Record<Unit, number> = { ms: 1, s: 1000, min: 60_000, h: 3_600_000 };

interface DelayRow {
  id: number;
  value: string;
  unit: Unit;
}

function toRow(ms: number, id: number): DelayRow {
  const units: Unit[] = ["h", "min", "s"];
  for (const u of units) {
    if (ms >= UNIT_MS[u] && ms % UNIT_MS[u] === 0) return { id, value: String(ms / UNIT_MS[u]), unit: u };
  }
  if (ms >= 1000) return { id, value: String(ms / 1000), unit: "s" };
  return { id, value: String(ms), unit: "ms" };
}

function rowMs(r: DelayRow): number {
  const v = Number(r.value);
  return Number.isFinite(v) ? Math.round(v * UNIT_MS[r.unit]) : NaN;
}

function RetryPolicyCard({ tenant, onSaved }: { tenant: Tenant; onSaved: (t: Tenant) => void }) {
  const { upsert } = useTenant();
  const toast = useToast();
  const [maxAttempts, setMaxAttempts] = useState(String(tenant.retry_max_attempts));
  const [rows, setRows] = useState<DelayRow[]>(() => tenant.retry_schedule_ms.map((ms, i) => toRow(ms, i)));
  const [nextId, setNextId] = useState(tenant.retry_schedule_ms.length);
  const [busy, setBusy] = useState(false);

  const reset = () => {
    setMaxAttempts(String(tenant.retry_max_attempts));
    setRows(tenant.retry_schedule_ms.map((ms, i) => toRow(ms, i)));
    setNextId(tenant.retry_schedule_ms.length);
  };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(reset, [tenant.retry_max_attempts, tenant.retry_schedule_ms.join(",")]);

  const schedule = rows.map(rowMs);
  const max = Number(maxAttempts);
  const scheduleValid = schedule.length > 0 && schedule.every((v) => Number.isFinite(v) && v >= 0);
  const maxValid = Number.isInteger(max) && max >= 1 && max <= 100;
  const dirty =
    max !== tenant.retry_max_attempts ||
    schedule.length !== tenant.retry_schedule_ms.length ||
    schedule.some((v, i) => v !== tenant.retry_schedule_ms[i]);

  const plan = useMemo(() => {
    if (!scheduleValid || !maxValid) return [];
    const out: { attempt: number; delay: number; at: number }[] = [{ attempt: 1, delay: 0, at: 0 }];
    let at = 0;
    for (let a = 2; a <= max; a++) {
      const d = schedule[Math.min(a - 2, schedule.length - 1)];
      at += d;
      out.push({ attempt: a, delay: d, at });
    }
    return out;
  }, [schedule.join(","), max, scheduleValid, maxValid]); // eslint-disable-line react-hooks/exhaustive-deps

  const update = (id: number, patch: Partial<DelayRow>) =>
    setRows((rs) => rs.map((r) => (r.id === id ? { ...r, ...patch } : r)));

  const save = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    try {
      const t = await api.patchTenant(tenant.id, { retry_max_attempts: max, retry_schedule_ms: schedule });
      onSaved(t);
      upsert(t);
      toast.success("Retry policy saved");
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(false);
    }
  };

  const shownPlan = plan.length > 12 ? [...plan.slice(0, 10), null, plan[plan.length - 1]] : plan;

  return (
    <Card
      title="Retry policy"
      sub="Delays before attempt 2, 3, … The last delay repeats until the attempt budget runs out, then the delivery is dead-lettered."
    >
      <form className="stack" onSubmit={save}>
        <div className="field" style={{ maxWidth: 260 }}>
          <label htmlFor="rp-max">Max attempts (including the first)</label>
          <input
            id="rp-max"
            className={`input${maxValid ? "" : " invalid"}`}
            type="number"
            min={1}
            max={100}
            value={maxAttempts}
            onChange={(e) => setMaxAttempts(e.target.value)}
          />
        </div>

        <div className="field">
          <span className="field-label">Backoff schedule</span>
          <div className="rp-rows">
            {rows.map((r, i) => (
              <div className="rp-row" key={r.id}>
                <span className="rp-idx muted">Before attempt {i + 2}</span>
                <input
                  className={`input input-sm${Number.isFinite(rowMs(r)) && rowMs(r) >= 0 ? "" : " invalid"}`}
                  type="number"
                  min={0}
                  step="any"
                  value={r.value}
                  onChange={(e) => update(r.id, { value: e.target.value })}
                  aria-label={`Delay before attempt ${i + 2}`}
                />
                <select
                  className="select select-sm"
                  value={r.unit}
                  onChange={(e) => update(r.id, { unit: e.target.value as Unit })}
                  aria-label="Unit"
                >
                  <option value="ms">ms</option>
                  <option value="s">seconds</option>
                  <option value="min">minutes</option>
                  <option value="h">hours</option>
                </select>
                <button
                  type="button"
                  className="btn btn-sm btn-ghost btn-icon"
                  onClick={() => setRows((rs) => rs.filter((x) => x.id !== r.id))}
                  disabled={rows.length <= 1}
                  title="Remove"
                >
                  <IconMinus size={14} />
                </button>
                {i === rows.length - 1 && <span className="muted rp-repeat">repeats</span>}
              </div>
            ))}
          </div>
          <div>
            <button
              type="button"
              className="btn btn-sm"
              onClick={() => {
                const last = rows[rows.length - 1];
                setRows((rs) => [...rs, last ? { ...last, id: nextId } : { id: nextId, value: "1", unit: "s" }]);
                setNextId((n) => n + 1);
              }}
            >
              <IconPlus size={14} /> Add delay
            </button>
          </div>
        </div>

        {plan.length > 0 && (
          <div className="rp-plan">
            <div className="section-title" style={{ marginBottom: 6 }}>
              Resulting timeline
            </div>
            <ol>
              {shownPlan.map((p, i) =>
                p === null ? (
                  <li key={`gap-${i}`} className="muted">
                    …
                  </li>
                ) : (
                  <li key={p.attempt}>
                    <span>Attempt {p.attempt}</span>
                    <span className="muted">{p.attempt === 1 ? "immediately" : `+${fmtDuration(p.delay)}`}</span>
                    <span className="tnum">t = {fmtDuration(p.at)}</span>
                  </li>
                ),
              )}
              <li className="rp-dlq">
                <span>Dead-letter queue</span>
                <span className="muted">after attempt {max} fails</span>
                <span className="tnum">t ≈ {fmtDuration(plan[plan.length - 1].at)}</span>
              </li>
            </ol>
          </div>
        )}

        {(!scheduleValid || !maxValid) && (
          <span className="field-error">Max attempts must be 1–100 and every delay a non-negative number.</span>
        )}
        <div className="row">
          <button className="btn btn-primary" disabled={!dirty || !scheduleValid || !maxValid || busy}>
            {busy && <span className="spinner" />} Save retry policy
          </button>
          {dirty && (
            <button type="button" className="btn btn-ghost" onClick={reset}>
              Reset
            </button>
          )}
        </div>
      </form>
    </Card>
  );
}
