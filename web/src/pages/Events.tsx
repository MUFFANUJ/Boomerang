import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { api, isAbort } from "../api/client";
import type { DeliveryState, EventListQuery, EventSummary } from "../api/types";
import { EventDetail } from "../components/EventDetail";
import { IconArrowLeft, IconEvents, IconRefresh } from "../components/Icons";
import { Card, DeliveryStateBadge, EmptyState, ErrorNote, Loading, PageHead, Segmented, Toggle } from "../components/ui";
import { useAsync, useDebounced } from "../lib/hooks";
import { fmtDateTime, fmtRelative } from "../lib/format";
import { useTenant } from "../lib/tenant";
import "./events.css";

type RangeKey = "15m" | "1h" | "24h" | "all" | "custom";
const RANGE_MS: Record<Exclude<RangeKey, "custom" | "all">, number> = {
  "15m": 15 * 60_000,
  "1h": 60 * 60_000,
  "24h": 24 * 60 * 60_000,
};
const STATES: DeliveryState[] = ["pending", "delivered", "retrying", "dead", "paused"];

/** datetime-local value <-> RFC 3339 */
function localToIso(v: string): string | undefined {
  if (!v) return undefined;
  const t = new Date(v);
  return Number.isNaN(t.getTime()) ? undefined : t.toISOString();
}

export function EventsPage() {
  const { eventId } = useParams();
  const { tenantId } = useTenant();
  const [sp] = useSearchParams();
  const backHref = `/events${sp.toString() ? `?${sp.toString()}` : ""}`;

  if (eventId) {
    return (
      <>
        <div className="row">
          <Link to={backHref} className="btn btn-ghost btn-sm">
            <IconArrowLeft size={14} /> Back to events
          </Link>
        </div>
        <EventDetail key={`${tenantId}:${eventId}`} tenantId={tenantId} eventId={eventId} />
      </>
    );
  }
  return <EventList />;
}

function EventList() {
  const { tenantId } = useTenant();
  const navigate = useNavigate();
  const [sp, setSp] = useSearchParams();

  const type = sp.get("type") ?? "";
  const state = (sp.get("state") ?? "") as DeliveryState | "";
  const endpointId = sp.get("endpoint_id") ?? "";
  const range = (sp.get("range") ?? "24h") as RangeKey;
  const customFrom = sp.get("from") ?? "";
  const customTo = sp.get("to") ?? "";
  const qParam = sp.get("q") ?? "";

  const [qInput, setQInput] = useState(qParam);
  const q = useDebounced(qInput, 350);
  const [auto, setAuto] = useState(false);

  const setParam = useCallback(
    (patch: Record<string, string | null>) => {
      const next = new URLSearchParams(sp);
      for (const [k, v] of Object.entries(patch)) {
        if (v === null || v === "") next.delete(k);
        else next.set(k, v);
      }
      setSp(next, { replace: true });
    },
    [sp, setSp],
  );

  useEffect(() => {
    if (q !== qParam) setParam({ q });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);

  const eventTypes = useAsync((s) => api.listEventTypes(s), []);
  const endpoints = useAsync((s) => api.listEndpoints(tenantId, s), [tenantId]);

  const buildQuery = useCallback((): EventListQuery => {
    let from: string | undefined;
    let to: string | undefined;
    if (range === "custom") {
      from = localToIso(customFrom);
      to = localToIso(customTo);
    } else if (range !== "all") {
      from = new Date(Date.now() - RANGE_MS[range]).toISOString();
    }
    return { type, state, endpoint_id: endpointId, from, to, q: qParam, limit: 50 };
  }, [type, state, endpointId, range, customFrom, customTo, qParam]);

  const filterKey = JSON.stringify([tenantId, type, state, endpointId, range, customFrom, customTo, qParam]);

  const [events, setEvents] = useState<EventSummary[]>([]);
  const [nextBefore, setNextBefore] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [loadedOnce, setLoadedOnce] = useState(false);
  const ctrlRef = useRef<AbortController | null>(null);
  const [nonce, setNonce] = useState(0);

  // First page (on filter change / refresh)
  useEffect(() => {
    ctrlRef.current?.abort();
    const ctrl = new AbortController();
    ctrlRef.current = ctrl;
    setLoading(true);
    api
      .listEvents(tenantId, buildQuery(), ctrl.signal)
      .then((r) => {
        setEvents(r.events);
        setNextBefore(r.next_before);
        setError(null);
        setLoadedOnce(true);
      })
      .catch((err) => {
        if (!isAbort(err)) setError(err);
      })
      .finally(() => {
        if (!ctrl.signal.aborted) setLoading(false);
      });
    return () => ctrl.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterKey, nonce]);

  // Optional auto refresh of the first page.
  const pagedRef = useRef(false);
  pagedRef.current = events.length > 50;
  useEffect(() => {
    if (!auto) return;
    const t = window.setInterval(() => {
      if (!pagedRef.current) setNonce((n) => n + 1);
    }, 4000);
    return () => window.clearInterval(t);
  }, [auto]);

  const loadMore = async () => {
    if (!nextBefore) return;
    setLoadingMore(true);
    try {
      const r = await api.listEvents(tenantId, { ...buildQuery(), before: nextBefore });
      setEvents((prev) => {
        const seen = new Set(prev.map((e) => e.event_id));
        return [...prev, ...r.events.filter((e) => !seen.has(e.event_id))];
      });
      setNextBefore(r.next_before);
    } catch (err) {
      setError(err);
    } finally {
      setLoadingMore(false);
    }
  };

  const anyFilter = type || state || endpointId || qParam || range !== "24h";
  const search = sp.toString();

  const endpointOptions = useMemo(() => endpoints.data ?? [], [endpoints.data]);

  return (
    <>
      <PageHead
        title="Event Explorer"
        sub="Search every event this tenant ingested, inspect its payload and the full attempt history of each delivery, and replay what failed."
        actions={
          <>
            <Toggle accent checked={auto} onChange={setAuto} label="Auto-refresh" />
            <button className="btn" onClick={() => setNonce((n) => n + 1)} disabled={loading}>
              <IconRefresh size={14} /> Refresh
            </button>
          </>
        }
      />

      <div className="ex-filters">
        <Segmented<RangeKey>
          value={range}
          onChange={(v) => setParam({ range: v === "24h" ? null : v })}
          options={[
            { value: "15m", label: "15m" },
            { value: "1h", label: "1h" },
            { value: "24h", label: "24h" },
            { value: "all", label: "All" },
            { value: "custom", label: "Custom" },
          ]}
        />
        {range === "custom" && (
          <>
            <input
              type="datetime-local"
              className="input input-sm"
              value={customFrom}
              onChange={(e) => setParam({ from: e.target.value })}
              aria-label="From"
            />
            <span className="muted">to</span>
            <input
              type="datetime-local"
              className="input input-sm"
              value={customTo}
              onChange={(e) => setParam({ to: e.target.value })}
              aria-label="To"
            />
          </>
        )}
        <select
          className="select select-sm"
          value={type}
          onChange={(e) => setParam({ type: e.target.value })}
          aria-label="Event type"
        >
          <option value="">All types</option>
          {(eventTypes.data ?? []).map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        <select
          className="select select-sm"
          value={state}
          onChange={(e) => setParam({ state: e.target.value })}
          aria-label="Delivery state"
        >
          <option value="">Any delivery state</option>
          {STATES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <select
          className="select select-sm"
          value={endpointId}
          onChange={(e) => setParam({ endpoint_id: e.target.value })}
          aria-label="Endpoint"
        >
          <option value="">All endpoints</option>
          {endpointOptions.map((ep) => (
            <option key={ep.id} value={ep.id}>
              {ep.name}
            </option>
          ))}
        </select>
        <input
          className="input input-sm ex-search"
          placeholder="Search event id prefix or payload text…"
          value={qInput}
          onChange={(e) => setQInput(e.target.value)}
          aria-label="Search"
        />
        {anyFilter && (
          <button
            className="btn btn-ghost btn-sm"
            onClick={() => {
              setQInput("");
              setSp(new URLSearchParams(), { replace: true });
            }}
          >
            Clear filters
          </button>
        )}
      </div>

      <ErrorNote error={error} onRetry={() => setNonce((n) => n + 1)} />

      <Card bodyClassName="" className={loading && loadedOnce ? "is-stale-card" : ""}>
        {!loadedOnce && loading ? (
          <Loading label="Loading events…" />
        ) : events.length === 0 ? (
          <EmptyState icon={<IconEvents size={18} />} title={anyFilter ? "No events match these filters" : "No events yet"}>
            {anyFilter
              ? "Widen the time range or clear a filter."
              : "Send traffic from Traffic Control and events will show up here."}
          </EmptyState>
        ) : (
          <div className="table-wrap">
            <table className="table ex-table">
              <thead>
                <tr>
                  <th>Event</th>
                  <th>Type</th>
                  <th>Created</th>
                  <th>Deliveries</th>
                  <th className="num">Attempts</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e) => (
                  <tr
                    key={e.event_id}
                    className="clickable"
                    tabIndex={0}
                    onClick={() => navigate(`/events/${encodeURIComponent(e.event_id)}${search ? `?${search}` : ""}`)}
                    onKeyDown={(k) => {
                      if (k.key === "Enter")
                        navigate(`/events/${encodeURIComponent(e.event_id)}${search ? `?${search}` : ""}`);
                    }}
                  >
                    <td>
                      <code className="ex-id">{e.event_id}</code>
                    </td>
                    <td>
                      <span className="chip">{e.event_type}</span>
                    </td>
                    <td title={fmtDateTime(e.created_at)} className="tnum">
                      <div>{fmtRelative(e.created_at)}</div>
                      <div className="muted" style={{ fontSize: 11 }}>
                        {fmtDateTime(e.created_at)}
                      </div>
                    </td>
                    <td>
                      {e.deliveries.length === 0 ? (
                        <span className="muted">no subscribers</span>
                      ) : (
                        <div className="ex-deliveries">
                          {e.deliveries.map((d) => (
                            <span key={d.delivery_id} className="ex-delivery" title={d.delivery_id}>
                              <span className="ex-ep">{d.endpoint_name}</span>
                              <DeliveryStateBadge state={d.state} small />
                            </span>
                          ))}
                        </div>
                      )}
                    </td>
                    <td className="num">{e.deliveries.reduce((a, d) => a + d.attempts, 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {events.length > 0 && (
          <div className="card-foot">
            <span className="muted" style={{ fontSize: 12 }}>
              Showing {events.length} event{events.length === 1 ? "" : "s"}
              {nextBefore ? "" : " · end of results"}
            </span>
            <span className="grow" />
            {nextBefore && (
              <button className="btn btn-sm" onClick={loadMore} disabled={loadingMore}>
                {loadingMore && <span className="spinner" />} Load older
              </button>
            )}
          </div>
        )}
      </Card>
    </>
  );
}
