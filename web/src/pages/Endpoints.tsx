import { useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";
import { api, ApiError } from "../api/client";
import type { CreateEndpointRequest, Endpoint, EndpointSecret, StatsEndpoint } from "../api/types";
import {
  IconEdit,
  IconEndpoints,
  IconEye,
  IconEyeOff,
  IconKey,
  IconPause,
  IconPlay,
  IconPlus,
  IconReplay,
  IconRotate,
  IconTrash,
  IconX,
} from "../components/Icons";
import { Modal } from "../components/Modal";
import {
  CircuitBadge,
  CopyButton,
  EmptyState,
  EndpointStatusBadge,
  ErrorNote,
  Loading,
  Meter,
  PageHead,
} from "../components/ui";
import { useConfirm } from "../lib/confirm";
import { fmtDateTime, fmtInt, fmtMs, fmtPct, fmtRate, fmtRelative, parseTs } from "../lib/format";
import { useAsync, useNow } from "../lib/hooks";
import { useEndpointChange, useLiveSelector } from "../lib/live";
import { useTenant } from "../lib/tenant";
import { useToast } from "../lib/toast";
import "./endpoints.css";

export function EndpointsPage() {
  const { tenantId } = useTenant();
  const list = useAsync((s) => api.listEndpoints(tenantId, s), [tenantId], { pollMs: 5000 });
  const stats = useLiveSelector((s) => s.stats);
  const [editing, setEditing] = useState<Endpoint | "new" | null>(null);
  const [secretFor, setSecretFor] = useState<Endpoint | null>(null);

  useEndpointChange(() => list.reload());

  const liveById = useMemo(() => {
    const m = new Map<string, StatsEndpoint>();
    for (const e of stats?.endpoints ?? []) m.set(e.id, e);
    return m;
  }, [stats]);

  const endpoints = list.data ?? [];

  return (
    <>
      <PageHead
        title="Endpoints"
        sub="Where webhooks go. Each endpoint subscribes to event types, has its own signing secret, rate limit and circuit breaker."
        actions={
          <button className="btn btn-primary" onClick={() => setEditing("new")}>
            <IconPlus size={14} /> New endpoint
          </button>
        }
      />

      <ErrorNote error={list.error} onRetry={list.reload} />

      {list.data === undefined && list.loading ? (
        <div className="card">
          <Loading label="Loading endpoints…" />
        </div>
      ) : endpoints.length === 0 && !list.error ? (
        <div className="card">
          <EmptyState icon={<IconEndpoints size={18} />} title="No endpoints yet">
            <div style={{ marginBottom: 12 }}>Create one to start delivering this tenant's events.</div>
            <button className="btn btn-primary" onClick={() => setEditing("new")}>
              <IconPlus size={14} /> New endpoint
            </button>
          </EmptyState>
        </div>
      ) : (
        <div className="ep-list">
          {endpoints.map((ep) => (
            <EndpointCard
              key={ep.id}
              ep={ep}
              live={liveById.get(ep.id)}
              onEdit={() => setEditing(ep)}
              onSecret={() => setSecretFor(ep)}
              onChanged={(updated) =>
                list.setData((prev) =>
                  updated === null
                    ? (prev ?? []).filter((x) => x.id !== ep.id)
                    : (prev ?? []).map((x) => (x.id === updated.id ? updated : x)),
                )
              }
            />
          ))}
        </div>
      )}

      {editing && (
        <EndpointForm
          tenantId={tenantId}
          endpoint={editing === "new" ? null : editing}
          onClose={() => setEditing(null)}
          onSaved={(saved) => {
            list.setData((prev) => {
              const arr = prev ?? [];
              return arr.some((x) => x.id === saved.id)
                ? arr.map((x) => (x.id === saved.id ? saved : x))
                : [...arr, saved];
            });
            setEditing(null);
          }}
        />
      )}
      {secretFor && <SecretModal tenantId={tenantId} endpoint={secretFor} onClose={() => setSecretFor(null)} />}
    </>
  );
}

function EndpointCard({
  ep,
  live,
  onEdit,
  onSecret,
  onChanged,
}: {
  ep: Endpoint;
  live: StatsEndpoint | undefined;
  onEdit: () => void;
  onSecret: () => void;
  onChanged: (ep: Endpoint | null) => void;
}) {
  const { tenantId } = useTenant();
  const toast = useToast();
  const confirm = useConfirm();
  const [busy, setBusy] = useState<string | null>(null);
  const h = ep.health;
  const status = live?.status ?? h.status;
  const circuit = live?.circuit ?? h.circuit;
  const effective = live?.rate_limit_effective_rps ?? h.rate_limit_effective_rps;
  const scoreTone = h.score >= 80 ? "good" : h.score >= 50 ? "warning" : "critical";

  const run = async (key: string, fn: () => Promise<void>) => {
    setBusy(key);
    try {
      await fn();
    } catch (err) {
      toast.error(err);
    } finally {
      setBusy(null);
    }
  };

  const togglePause = () =>
    run("pause", async () => {
      const updated = ep.paused ? await api.resumeEndpoint(tenantId, ep.id) : await api.pauseEndpoint(tenantId, ep.id);
      onChanged(updated);
      toast.success(updated.paused ? `${ep.name} paused. Deliveries queue up until resumed.` : `${ep.name} resumed`);
    });

  const replayDlq = async () => {
    const ok = await confirm({
      title: `Replay dead letters for ${ep.name}?`,
      body: "Every dead-lettered delivery for this endpoint is re-enqueued with a fresh attempt budget.",
      confirmLabel: "Replay DLQ",
    });
    if (!ok) return;
    await run("dlq", async () => {
      const r = await api.replayDlq(tenantId, ep.id);
      toast.success(
        r.replayed === 0 ? "Nothing in the DLQ for this endpoint" : `Replayed ${fmtInt(r.replayed)} dead-lettered deliveries`,
      );
    });
  };

  const remove = async () => {
    const ok = await confirm({
      title: `Delete ${ep.name}?`,
      body: (
        <>
          Pending deliveries to <code>{ep.url}</code> will no longer be attempted. This cannot be undone.
        </>
      ),
      confirmLabel: "Delete endpoint",
      danger: true,
    });
    if (!ok) return;
    await run("delete", async () => {
      await api.deleteEndpoint(tenantId, ep.id);
      onChanged(null);
      toast.success(`${ep.name} deleted`);
    });
  };

  return (
    <article className={`card ep-card${ep.paused ? " is-paused" : ""}`}>
      <div className="ep-top">
        <div className="grow">
          <div className="row row-wrap" style={{ gap: 8 }}>
            <h3 className="ep-name">{ep.name}</h3>
            <EndpointStatusBadge status={ep.paused ? "paused" : status} small />
            {/* circuit_open status already says "Circuit open"; don't repeat it */}
            {(ep.paused || status !== "circuit_open") && <CircuitBadge state={circuit} small />}
          </div>
          <div className="ep-url mono" title={ep.url}>
            {ep.url}
          </div>
          {ep.description && <div className="ep-desc">{ep.description}</div>}
        </div>
        <div className="ep-actions">
          <button className="btn btn-sm" onClick={togglePause} disabled={busy !== null}>
            {ep.paused ? <IconPlay size={14} /> : <IconPause size={14} />}
            {ep.paused ? "Resume" : "Pause"}
          </button>
          <button className="btn btn-sm" onClick={onEdit}>
            <IconEdit size={14} /> Edit
          </button>
          <button className="btn btn-sm" onClick={onSecret}>
            <IconKey size={14} /> Secret
          </button>
          <button className="btn btn-sm" onClick={replayDlq} disabled={busy !== null}>
            {busy === "dlq" ? <span className="spinner" /> : <IconReplay size={14} />} Replay DLQ
          </button>
          <button className="btn btn-sm btn-ghost btn-icon" onClick={remove} disabled={busy !== null} title="Delete">
            <IconTrash size={14} />
          </button>
        </div>
      </div>

      <div className="ep-metrics">
        <div className="ep-metric">
          <div className="ep-metric-label">Health score</div>
          <div className="ep-metric-value">
            {Math.round(h.score)}
            <span className="muted"> / 100</span>
          </div>
          <Meter value={h.score / 100} tone={scoreTone} label="Health score" />
        </div>
        <div className="ep-metric">
          <div className="ep-metric-label">Success rate</div>
          <div className="ep-metric-value">{fmtPct(live?.success_rate ?? h.success_rate)}</div>
          <div className="ep-metric-sub">
            {fmtInt(h.failures_5m)} failed of {fmtInt(h.attempts_5m)} attempts (5m)
          </div>
        </div>
        <div className="ep-metric">
          <div className="ep-metric-label">Latency p95</div>
          <div className="ep-metric-value">{fmtMs(live?.p95_ms ?? h.p95_latency_ms)}</div>
          <div className="ep-metric-sub">
            {live ? `${fmtRate(live.delivered_eps)}/s delivered · ${fmtRate(live.failed_eps)}/s failed` : "No live data"}
          </div>
        </div>
        <div className="ep-metric">
          <div className="ep-metric-label">Rate limit</div>
          <div className="ep-metric-value">
            {ep.rate_limit_rps > 0 ? (
              <>
                {fmtInt(ep.rate_limit_rps)}
                <span className="muted"> /s</span>
              </>
            ) : (
              "Unlimited"
            )}
          </div>
          <div className="ep-metric-sub">
            {effective !== null ? `Adaptive: ${fmtRate(effective)}/s now` : "Adaptive limiter idle"}
          </div>
        </div>
        <div className="ep-metric">
          <div className="ep-metric-label">Circuit</div>
          <div className="ep-metric-value" style={{ fontSize: 15 }}>
            {h.consecutive_failures} consecutive failure{h.consecutive_failures === 1 ? "" : "s"}
          </div>
          <div className="ep-metric-sub">
            {h.circuit_opened_at ? `Opened ${fmtRelative(h.circuit_opened_at)}` : "Not tripped"}
          </div>
        </div>
      </div>

      <div className="ep-bottom">
        <div className="chips">
          {ep.event_types.length === 0 ? (
            <span className="muted" style={{ fontSize: 12 }}>
              No subscriptions
            </span>
          ) : (
            ep.event_types.map((t) => (
              <span key={t} className={`chip${t.includes("*") ? " chip-wild" : ""}`}>
                {t}
              </span>
            ))
          )}
        </div>
        <span className="grow" />
        <span className="muted" style={{ fontSize: 12 }}>
          Secret <code>{ep.secret_prefix}</code> · <code>{ep.id}</code> ·{" "}
          <Link to={`/events?endpoint_id=${encodeURIComponent(ep.id)}`}>View events</Link>
        </span>
      </div>
    </article>
  );
}

/* ------------------------------------------------------------------ */
/* Create / edit form                                                  */
/* ------------------------------------------------------------------ */

function EndpointForm({
  tenantId,
  endpoint,
  onClose,
  onSaved,
}: {
  tenantId: string;
  endpoint: Endpoint | null;
  onClose: () => void;
  onSaved: (ep: Endpoint) => void;
}) {
  const toast = useToast();
  const knownTypes = useAsync((s) => api.listEventTypes(s), []);
  const [name, setName] = useState(endpoint?.name ?? "");
  const [url, setUrl] = useState(endpoint?.url ?? "");
  const [description, setDescription] = useState(endpoint?.description ?? "");
  const [types, setTypes] = useState<string[]>(endpoint?.event_types ?? []);
  const [custom, setCustom] = useState("");
  const [rps, setRps] = useState(String(endpoint?.rate_limit_rps ?? 0));
  const [saving, setSaving] = useState(false);
  const [urlError, setUrlError] = useState<string | null>(null);
  const [formError, setFormError] = useState<unknown>(null);

  const known = knownTypes.data ?? [];
  const extraTypes = types.filter((t) => !known.includes(t));

  const toggle = (t: string) => setTypes((prev) => (prev.includes(t) ? prev.filter((x) => x !== t) : [...prev, t]));

  const addCustom = () => {
    const v = custom.trim();
    if (!v) return;
    if (!types.includes(v)) setTypes((prev) => [...prev, v]);
    setCustom("");
  };

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setUrlError(null);
    setFormError(null);
    const rpsNum = Number(rps);
    if (!Number.isFinite(rpsNum) || rpsNum < 0) {
      setFormError(new Error("Rate limit must be 0 (unlimited) or a positive number"));
      return;
    }
    const body: CreateEndpointRequest = {
      name: name.trim(),
      url: url.trim(),
      description: description.trim(),
      event_types: types,
      rate_limit_rps: rpsNum,
    };
    setSaving(true);
    try {
      const saved = endpoint
        ? await api.patchEndpoint(tenantId, endpoint.id, body)
        : await api.createEndpoint(tenantId, body);
      toast.success(endpoint ? `${saved.name} updated` : `${saved.name} created`);
      onSaved(saved);
    } catch (err) {
      if (err instanceof ApiError && (err.code === "ssrf_blocked" || /url/i.test(err.code))) setUrlError(err.message);
      else setFormError(err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      wide
      title={endpoint ? `Edit ${endpoint.name}` : "New endpoint"}
      onClose={onClose}
      footer={
        <>
          <button className="btn" type="button" onClick={onClose}>
            Cancel
          </button>
          <button className="btn btn-primary" type="submit" form="ep-form" disabled={saving || !name.trim() || !url.trim()}>
            {saving && <span className="spinner" />}
            {endpoint ? "Save changes" : "Create endpoint"}
          </button>
        </>
      }
    >
      <form id="ep-form" className="stack" onSubmit={submit} noValidate>
        {formError ? <ErrorNote error={formError} /> : null}
        <div className="grid grid-2">
          <div className="field">
            <label htmlFor="ep-name">Name</label>
            <input
              id="ep-name"
              className="input"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Inventory Service"
              autoFocus
              required
            />
          </div>
          <div className="field">
            <label htmlFor="ep-rps">Rate limit</label>
            <div className="input-group">
              <input
                id="ep-rps"
                className="input"
                type="number"
                min={0}
                value={rps}
                onChange={(e) => setRps(e.target.value)}
              />
              <span className="input-suffix">deliveries/s</span>
            </div>
            <span className="field-hint">0 means unlimited</span>
          </div>
        </div>
        <div className="field">
          <label htmlFor="ep-url">URL</label>
          <input
            id="ep-url"
            className={`input mono${urlError ? " invalid" : ""}`}
            value={url}
            onChange={(e) => {
              setUrl(e.target.value);
              setUrlError(null);
            }}
            placeholder="https://example.com/webhooks"
            aria-invalid={!!urlError}
            aria-describedby={urlError ? "ep-url-err" : undefined}
            required
          />
          {urlError ? (
            <span className="field-error" id="ep-url-err">
              <IconX size={14} /> {urlError}
            </span>
          ) : (
            <span className="field-hint">Validated against SSRF rules: private and internal addresses are rejected.</span>
          )}
        </div>
        <div className="field">
          <label htmlFor="ep-desc">Description</label>
          <input
            id="ep-desc"
            className="input"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Optional"
          />
        </div>
        <div className="field">
          <span className="field-label">Subscribed event types</span>
          {knownTypes.loading && !knownTypes.data ? (
            <span className="muted">Loading event types…</span>
          ) : (
            <div className="ep-typegrid">
              {known.map((t) => (
                <label key={t} className={`ep-typeopt${types.includes(t) ? " on" : ""}`}>
                  <input type="checkbox" checked={types.includes(t)} onChange={() => toggle(t)} />
                  <span className="mono">{t}</span>
                </label>
              ))}
            </div>
          )}
          <div className="row" style={{ marginTop: 6 }}>
            <input
              className="input mono grow"
              value={custom}
              onChange={(e) => setCustom(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault();
                  addCustom();
                }
              }}
              placeholder='Add a type or wildcard, e.g. "order.*" or "*"'
            />
            <button type="button" className="btn" onClick={addCustom} disabled={!custom.trim()}>
              Add
            </button>
          </div>
          {extraTypes.length > 0 && (
            <div className="chips" style={{ marginTop: 6 }}>
              {extraTypes.map((t) => (
                <span key={t} className="chip chip-wild">
                  {t}
                  <button type="button" onClick={() => toggle(t)} aria-label={`Remove ${t}`}>
                    <IconX size={12} />
                  </button>
                </span>
              ))}
            </div>
          )}
          {types.length === 0 && (
            <span className="field-hint">No types selected: this endpoint will not receive any events.</span>
          )}
        </div>
      </form>
    </Modal>
  );
}

/* ------------------------------------------------------------------ */
/* Secret reveal / rotate                                              */
/* ------------------------------------------------------------------ */

function SecretModal({ tenantId, endpoint, onClose }: { tenantId: string; endpoint: Endpoint; onClose: () => void }) {
  const toast = useToast();
  const confirm = useConfirm();
  const secret = useAsync((_s) => api.getSecret(tenantId, endpoint.id), [tenantId, endpoint.id]);
  const [reveal, setReveal] = useState(false);
  const [rotating, setRotating] = useState(false);
  const now = useNow(1000);

  const rotate = async () => {
    const ok = await confirm({
      title: "Rotate signing secret?",
      body: "A new secret signs every webhook from now on. The current one keeps signing alongside it until it expires, so receivers can roll over without dropping requests.",
      confirmLabel: "Rotate secret",
    });
    if (!ok) return;
    setRotating(true);
    try {
      const s = await api.rotateSecret(tenantId, endpoint.id);
      secret.setData(s);
      setReveal(true);
      toast.success("Secret rotated");
    } catch (err) {
      toast.error(err);
    } finally {
      setRotating(false);
    }
  };

  const s: EndpointSecret | undefined = secret.data;
  const expiresAt = parseTs(s?.previous_expires_at ?? null);

  return (
    <Modal
      title={`Signing secret · ${endpoint.name}`}
      onClose={onClose}
      footer={
        <>
          <button className="btn" onClick={onClose}>
            Close
          </button>
          <button className="btn btn-primary" onClick={rotate} disabled={rotating || !s}>
            {rotating ? <span className="spinner" /> : <IconRotate size={14} />} Rotate secret
          </button>
        </>
      }
    >
      {secret.error ? <ErrorNote error={secret.error} onRetry={secret.reload} /> : null}
      {!s ? (
        !secret.error && <Loading compact label="Fetching secret…" />
      ) : (
        <>
          <div className="field">
            <span className="field-label">Current secret</span>
            <div className="secret-box">
              <code>{reveal ? s.secret : maskSecret(s.secret)}</code>
              <button className="btn btn-sm btn-icon" onClick={() => setReveal((r) => !r)} title={reveal ? "Hide" : "Reveal"}>
                {reveal ? <IconEyeOff size={14} /> : <IconEye size={14} />}
              </button>
              <CopyButton text={s.secret} />
            </div>
            <span className="field-hint">
              Verify the <code>Boomerang-Signature</code> header with HMAC-SHA256 over{" "}
              <code>{"<Boomerang-Id>.<timestamp>.<body>"}</code>.
            </span>
          </div>
          {s.previous_secret && (
            <div className="field">
              <span className="field-label">Previous secret (still signing)</span>
              <div className="secret-box">
                <code>{reveal ? s.previous_secret : maskSecret(s.previous_secret)}</code>
                <CopyButton text={s.previous_secret} />
              </div>
              {expiresAt !== null && (
                <div className={`notice ${expiresAt > now ? "notice-warning" : ""}`}>
                  <IconRotate size={14} />
                  <div>
                    {expiresAt > now ? (
                      <>
                        The previous secret expires at <strong>{fmtDateTime(expiresAt)}</strong> (
                        {fmtRelative(expiresAt, now)}). Until then webhooks carry both signatures.
                      </>
                    ) : (
                      <>The previous secret expired {fmtRelative(expiresAt, now)}.</>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </Modal>
  );
}

function maskSecret(s: string): string {
  const i = s.indexOf("_");
  const prefix = i >= 0 ? s.slice(0, Math.min(s.length, i + 5)) : s.slice(0, 4);
  return `${prefix}${"•".repeat(Math.max(8, Math.min(28, s.length - prefix.length)))}`;
}
