// Typed fetch client for the Boomerang management + ShopSim API (docs/API.md).
import type {
  ApiErrorBody,
  ApiKey,
  ChaosPresetResponse,
  ChaosResponse,
  CreateEndpointRequest,
  CreatedApiKey,
  Endpoint,
  EndpointSecret,
  EventDetail,
  EventListQuery,
  EventListResponse,
  InfraSnapshot,
  IntegrityData,
  KillResponse,
  PatchEndpointRequest,
  PatchTenantRequest,
  Receiver,
  ReceiverChaos,
  ReceiverName,
  ReplayResponse,
  ScaleResponse,
  SimBurstRequest,
  SimLoadTestRequest,
  SimSingleResponse,
  SimStatus,
  SimStreamRequest,
  StatsData,
  Tenant,
  TimeseriesResponse,
} from "./types";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly retryAfter: string | null;
  constructor(status: number, message: string, code: string, retryAfter: string | null = null) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.retryAfter = retryAfter;
  }
}

type Method = "GET" | "POST" | "PUT" | "PATCH" | "DELETE";

interface RequestOptions {
  body?: unknown;
  signal?: AbortSignal;
}

function isErrorBody(v: unknown): v is ApiErrorBody {
  return typeof v === "object" && v !== null && typeof (v as ApiErrorBody).error === "string";
}

async function request<T>(method: Method, path: string, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = { Accept: "application/json" };
  let body: string | undefined;
  if (opts.body !== undefined) {
    headers["Content-Type"] = "application/json";
    body = JSON.stringify(opts.body);
  }
  let res: Response;
  try {
    res = await fetch(path, { method, headers, body, signal: opts.signal });
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") throw err;
    throw new ApiError(0, "Network error: the API is unreachable", "network_error");
  }
  const text = await res.text();
  let parsed: unknown = undefined;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = undefined;
    }
  }
  if (!res.ok) {
    if (isErrorBody(parsed)) {
      throw new ApiError(res.status, parsed.error, parsed.code ?? "error", res.headers.get("Retry-After"));
    }
    throw new ApiError(
      res.status,
      `${res.status} ${res.statusText || "Request failed"}`,
      "http_error",
      res.headers.get("Retry-After"),
    );
  }
  return parsed as T;
}

function qs(params: Record<string, string | number | undefined | null>): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === "") continue;
    sp.set(k, String(v));
  }
  const s = sp.toString();
  return s ? `?${s}` : "";
}

const enc = encodeURIComponent;

export const api = {
  /* Tenants */
  listTenants: (signal?: AbortSignal) => request<Tenant[]>("GET", "/api/tenants", { signal }),
  createTenant: (name: string) => request<Tenant>("POST", "/api/tenants", { body: { name } }),
  getTenant: (tid: string, signal?: AbortSignal) => request<Tenant>("GET", `/api/tenants/${enc(tid)}`, { signal }),
  patchTenant: (tid: string, body: PatchTenantRequest) =>
    request<Tenant>("PATCH", `/api/tenants/${enc(tid)}`, { body }),

  /* API keys */
  listKeys: (tid: string, signal?: AbortSignal) =>
    request<ApiKey[]>("GET", `/api/tenants/${enc(tid)}/keys`, { signal }),
  createKey: (tid: string, name: string) =>
    request<CreatedApiKey>("POST", `/api/tenants/${enc(tid)}/keys`, { body: { name } }),
  revokeKey: (tid: string, kid: string) =>
    request<ApiKey>("DELETE", `/api/tenants/${enc(tid)}/keys/${enc(kid)}`),

  /* Endpoints */
  listEndpoints: (tid: string, signal?: AbortSignal) =>
    request<Endpoint[]>("GET", `/api/tenants/${enc(tid)}/endpoints`, { signal }),
  getEndpoint: (tid: string, eid: string, signal?: AbortSignal) =>
    request<Endpoint>("GET", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}`, { signal }),
  createEndpoint: (tid: string, body: CreateEndpointRequest) =>
    request<Endpoint>("POST", `/api/tenants/${enc(tid)}/endpoints`, { body }),
  patchEndpoint: (tid: string, eid: string, body: PatchEndpointRequest) =>
    request<Endpoint>("PATCH", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}`, { body }),
  deleteEndpoint: (tid: string, eid: string) =>
    request<unknown>("DELETE", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}`),
  pauseEndpoint: (tid: string, eid: string) =>
    request<Endpoint>("POST", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}/pause`),
  resumeEndpoint: (tid: string, eid: string) =>
    request<Endpoint>("POST", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}/resume`),
  getSecret: (tid: string, eid: string) =>
    request<EndpointSecret>("GET", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}/secret`),
  rotateSecret: (tid: string, eid: string) =>
    request<EndpointSecret>("POST", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}/rotate-secret`),
  replayDlq: (tid: string, eid: string) =>
    request<ReplayResponse>("POST", `/api/tenants/${enc(tid)}/endpoints/${enc(eid)}/replay-dlq`),
  listEventTypes: (signal?: AbortSignal) => request<string[]>("GET", "/api/event-types", { signal }),

  /* Events */
  listEvents: (tid: string, q: EventListQuery, signal?: AbortSignal) =>
    request<EventListResponse>(
      "GET",
      `/api/tenants/${enc(tid)}/events${qs({
        type: q.type,
        state: q.state,
        endpoint_id: q.endpoint_id,
        from: q.from,
        to: q.to,
        q: q.q,
        limit: q.limit ?? 50,
        before: q.before,
      })}`,
      { signal },
    ),
  getEvent: (tid: string, eventId: string, signal?: AbortSignal) =>
    request<EventDetail>("GET", `/api/tenants/${enc(tid)}/events/${enc(eventId)}`, { signal }),
  /** With no delivery ids, replays every delivery that is not delivered. */
  replayEvent: (tid: string, eventId: string, deliveryIds?: string[]) =>
    request<ReplayResponse>("POST", `/api/tenants/${enc(tid)}/events/${enc(eventId)}/replay`, {
      body: deliveryIds ? { delivery_ids: deliveryIds } : undefined,
    }),
  replayDelivery: (tid: string, did: string) =>
    request<ReplayResponse>("POST", `/api/tenants/${enc(tid)}/deliveries/${enc(did)}/replay`),

  /* Chaos */
  getChaos: (signal?: AbortSignal) => request<ChaosResponse>("GET", "/api/chaos", { signal }),
  putChaos: (name: ReceiverName, body: Partial<ReceiverChaos>) =>
    request<Receiver>("PUT", `/api/chaos/${enc(name)}`, { body }),
  applyChaosPreset: (id: string) => request<ChaosPresetResponse>("POST", `/api/chaos/presets/${enc(id)}`),
  resetChaosStats: () => request<{ ok: boolean }>("POST", "/api/chaos/reset-stats"),

  /* Infra */
  getInfra: (signal?: AbortSignal) => request<InfraSnapshot>("GET", "/api/infra", { signal }),
  scaleWorkers: (replicas: number) =>
    request<ScaleResponse>("POST", "/api/infra/workers/scale", { body: { replicas } }),
  killWorker: (workerId: string) =>
    request<KillResponse>("POST", `/api/infra/workers/${enc(workerId)}/kill`),
  stopWorker: (workerId: string) => request<unknown>("POST", `/api/infra/workers/${enc(workerId)}/stop`),

  /* Metrics */
  getTimeseries: (minutes: number, tenantId?: string, signal?: AbortSignal) =>
    request<TimeseriesResponse>("GET", `/api/metrics/timeseries${qs({ minutes, tenant_id: tenantId })}`, {
      signal,
    }),
  getOverview: (signal?: AbortSignal) => request<StatsData>("GET", "/api/overview", { signal }),
  getIntegrity: (signal?: AbortSignal) => request<IntegrityData>("GET", "/api/integrity", { signal }),

  /* ShopSim */
  simStatus: (signal?: AbortSignal) => request<SimStatus>("GET", "/api/sim/status", { signal }),
  simSingle: (type: string) => request<SimSingleResponse>("POST", "/api/sim/single", { body: { type } }),
  simStream: (body: SimStreamRequest) => request<SimStatus>("POST", "/api/sim/stream", { body }),
  simBurst: (body: SimBurstRequest) => request<SimStatus>("POST", "/api/sim/burst", { body }),
  simLoadTest: (body: SimLoadTestRequest) => request<SimStatus>("POST", "/api/sim/loadtest", { body }),
  simStop: () => request<SimStatus>("POST", "/api/sim/stop"),
};

export function errorMessage(err: unknown): string {
  if (err instanceof ApiError) return err.message;
  if (err instanceof Error) return err.message;
  return String(err);
}

export function isAbort(err: unknown): boolean {
  return err instanceof DOMException && err.name === "AbortError";
}
