# Boomerang API Contract

Everything the dashboard shows comes from these endpoints. All traffic goes through the
NGINX gateway on `http://localhost:8080`:

| Path prefix | Upstream service | Notes |
|---|---|---|
| `/v1/*` | `api` (N replicas) | Public ingestion API, API-key auth, per-tenant rate limits |
| `/api/sim/*` | `shopsim` | ShopSim traffic generator control |
| `/api/*` | `api` | Management API for the dashboard |
| `/ws` | `realtime` | WebSocket stream of live data |
| `/` | `web` | Dashboard static files (SPA, fall back to `index.html`) |

Conventions:

- JSON everywhere. Timestamps are RFC 3339 strings in UTC with millisecond precision
  (`2026-11-23T10:15:30.123Z`). Durations are integer milliseconds (`*_ms`).
- Errors: non-2xx with body `{"error": "human readable message", "code": "snake_case"}`.
- IDs are opaque strings: tenants `ten_…`, API keys `key_…`, endpoints `ep_…`,
  events `evt_…`, deliveries `dlv_…`.
- The demo tenant is created at startup with id `ten_shopsim` and name `ShopSim`.
  The dashboard should default to this tenant and offer a tenant switcher (from `GET /api/tenants`).

---

## 1. Public ingestion API

### `POST /v1/events`

Headers: `Authorization: Bearer bmr_…` (required), `Idempotency-Key: <string>` (optional).

```json
{ "type": "order.created", "data": { "order_id": "ord_123", "amount": 4200 }, "ordering_key": "ord_123" }
```

- `202 Accepted` → `{"id": "evt_…", "status": "accepted"}`
- `200 OK` (duplicate idempotency key) → `{"id": "evt_<original>", "status": "duplicate"}`
- `401` bad key, `413` payload too large (> 1 MiB), `422` validation, `429` tenant rate limit
  (with `Retry-After` header).

### `POST /v1/events/batch`

`{"events": [ <event as above, plus optional "idempotency_key"> , … ]}` (max 500) →
`202 {"ids": ["evt_…", …]}` (duplicates return the original id in place).

---

## 2. Management API (dashboard)

No auth in demo mode.

### Tenants and settings

`GET /api/tenants` → `Tenant[]`

```ts
interface Tenant {
  id: string; name: string; created_at: string;
  rate_limit_rps: number;      // ingestion requests per second
  rate_limit_burst: number;
  retry_max_attempts: number;  // total attempts including the first one
  retry_schedule_ms: number[]; // delay before attempt 2, 3, …; last value repeats
}
```

- `POST /api/tenants` `{name}` → `Tenant`
- `GET /api/tenants/{tid}` → `Tenant`
- `PATCH /api/tenants/{tid}` `{name?, rate_limit_rps?, rate_limit_burst?, retry_max_attempts?, retry_schedule_ms?}` → `Tenant`

### API keys

```ts
interface ApiKey { id: string; name: string; prefix: string; created_at: string; last_used_at: string | null; revoked_at: string | null; }
```

- `GET /api/tenants/{tid}/keys` → `ApiKey[]`
- `POST /api/tenants/{tid}/keys` `{name}` → `ApiKey & { key: string }`. The full key is shown only once.
- `DELETE /api/tenants/{tid}/keys/{kid}` → `ApiKey`. This revokes the key.

### Endpoints and subscriptions

```ts
type CircuitState = "closed" | "open" | "half_open";
interface EndpointHealth {
  circuit: CircuitState;
  consecutive_failures: number;
  circuit_opened_at: string | null;
  success_rate: number | null;   // 0..1 over the last 5 minutes, null if there were no attempts
  attempts_5m: number; failures_5m: number;
  p95_latency_ms: number | null;
  score: number;                 // 0..100 health score
  status: "healthy" | "degraded" | "failing" | "paused" | "circuit_open";
  rate_limit_effective_rps: number | null; // adaptive limiter's current rate
}
interface Endpoint {
  id: string; tenant_id: string; name: string; url: string; description: string;
  event_types: string[];         // exact types, "order.*" style wildcards, or "*"
  paused: boolean;
  rate_limit_rps: number;        // max deliveries/s to this endpoint, 0 means unlimited
  secret_prefix: string;         // first chars of the current signing secret, e.g. "whsec_Ab12…"
  created_at: string; updated_at: string;
  health: EndpointHealth;
}
```

- `GET /api/tenants/{tid}/endpoints` → `Endpoint[]`
- `POST /api/tenants/{tid}/endpoints` `{name, url, description?, event_types, rate_limit_rps?}` → `Endpoint`.
  The URL is validated against SSRF rules. Private or internal IPs return a `422` with code `ssrf_blocked`.
- `GET|PATCH|DELETE /api/tenants/{tid}/endpoints/{eid}` (PATCH takes the same fields as POST, all optional)
- `POST /api/tenants/{tid}/endpoints/{eid}/pause` → `Endpoint`, and `/resume` → `Endpoint`
- `GET /api/tenants/{tid}/endpoints/{eid}/secret` →
  `{"secret": "whsec_…", "previous_secret": "whsec_…" | null, "previous_expires_at": string | null}`
- `POST /api/tenants/{tid}/endpoints/{eid}/rotate-secret` → same shape. The old secret keeps
  signing alongside the new one until `previous_expires_at`, which is 24h away (5 minutes in demo mode).
- `POST /api/tenants/{tid}/endpoints/{eid}/replay-dlq` → `{"replayed": number}`
- `GET /api/event-types` → `string[]` (known event types, e.g. ShopSim's five)

### Event Explorer

```ts
type DeliveryState = "pending" | "delivered" | "retrying" | "dead" | "paused";
interface DeliverySummary {
  delivery_id: string; endpoint_id: string; endpoint_name: string;
  state: DeliveryState; attempts: number; updated_at: string;
}
interface EventSummary { event_id: string; event_type: string; created_at: string; deliveries: DeliverySummary[]; }
```

`GET /api/tenants/{tid}/events?type=&state=&endpoint_id=&from=&to=&q=&limit=50&before=`
→ `{"events": EventSummary[], "next_before": string | null}`

- `state` filters on delivery state. `q` matches an event id prefix or a substring of the payload.
  `before` is a cursor (pass `next_before` back).

`GET /api/tenants/{tid}/events/{event_id}` →

```ts
interface Attempt {
  delivery_id: string; endpoint_id: string; endpoint_name: string;
  attempt: number;                    // 1-based
  outcome: "success" | "failure" | "deferred" | "dead";
  status_code: number;                // 0 if there was no HTTP response (timeout, connection refused, circuit open)
  response_body: string;              // truncated to 1 KiB
  latency_ms: number;
  error: string;                      // transport error, if any
  reason: string;                     // why it was retried, deferred, or dead-lettered (human readable)
  next_retry_at: string | null;
  signature: string;                  // value of the Boomerang-Signature header sent
  worker_id: string;
  ts: string;
}
interface EventDetail {
  event: {
    event_id: string; tenant_id: string; event_type: string; created_at: string;
    payload: unknown;                 // parsed JSON
    payload_ref: string | null;       // set when the payload was offloaded to object storage
    payload_bytes: number;
    idempotency_key: string | null; ordering_key: string | null;
  };
  deliveries: DeliverySummary[];
  attempts: Attempt[];                // ordered by ts ascending
}
```

- `POST /api/tenants/{tid}/events/{event_id}/replay` `{"delivery_ids"?: string[]}` → `{"replayed": number}`.
  With no body, it replays every delivery that is not `delivered`.
- `POST /api/tenants/{tid}/deliveries/{did}/replay` → `{"replayed": 1}`

### Chaos Playground (mock receivers)

```ts
interface ReceiverChaos { failure_rate: number; delay_ms: number; down: boolean; }  // failure_rate 0..1
interface ReceiverStats { received: number; unique: number; duplicates: number; invalid_signatures: number; injected_failures: number; }
interface Receiver { name: "inventory" | "email" | "analytics"; display_name: string; url: string; chaos: ReceiverChaos; mode: "healthy" | "slow" | "flaky" | "down"; stats: ReceiverStats; }
```

- `GET /api/chaos` → `{"receivers": Receiver[], "presets": {id: string, name: string, description: string}[]}`
- `PUT /api/chaos/{name}` `ReceiverChaos` (partial allowed) → `Receiver`
- `POST /api/chaos/presets/{id}` → `{"receivers": Receiver[]}`. Preset ids: `all-healthy`, `email-outage`,
  `slow-inventory`, `flaky-email`, `random-network-failures`, `everything-on-fire`.
- `POST /api/chaos/reset-stats` → `{"ok": true}`

### Infrastructure and scaling

- `GET /api/infra` → `InfraSnapshot` (the same shape as the WS `infra` message)
- `POST /api/infra/workers/scale` `{"replicas": number}` (1..10) → `{"replicas": number}`
- `POST /api/infra/workers/{worker_id}/kill` → `{"killed": worker_id}`. Sends SIGKILL with no graceful shutdown.
- `POST /api/infra/workers/{worker_id}/stop` → graceful stop (SIGTERM)
- When no scaler is available (not running with the Docker socket or in-cluster), these return `501`
  with code `scaler_unavailable`, and `InfraSnapshot.scaler.enabled` is `false`.

### Metrics history

- `GET /api/metrics/timeseries?minutes=15&tenant_id=` →
  `{"bucket_s": 5, "points": [{"ts": string, "delivered": number, "failed": number, "p95_ms": number}]}`
- `GET /api/overview` → `StatsMessage["data"]` (latest snapshot, so the page renders before the first WS tick)
- `GET /api/integrity` → `IntegrityMessage["data"]`

---

## 3. ShopSim control (`/api/sim/*`)

```ts
interface SimStatus {
  mode: "idle" | "stream" | "burst" | "loadtest";
  running: boolean;
  rate_eps: number;                   // target rate (stream/loadtest)
  event_types: string[];
  sent: number; accepted: number; duplicates: number; errors: number; rate_limited: number;
  current_eps: number;                // measured over the last second
  latency_ms: { p50: number; p95: number; p99: number };  // ingestion API latency seen by ShopSim
  burst: { total: number; done: number } | null;
  loadtest: { start_eps: number; end_eps: number; duration_s: number; elapsed_s: number;
              steps: { t_s: number; target_eps: number; achieved_eps: number; p95_ms: number; errors: number }[] } | null;
  last_event: { id: string; type: string; ts: string } | null;
}
```

- `GET /api/sim/status` → `SimStatus`
- `POST /api/sim/single` `{"type": "order.created"}` → `{"id": "evt_…", "type": …}`
- `POST /api/sim/stream` `{"rate_eps": 50, "event_types": [...]}` → `SimStatus`
- `POST /api/sim/burst` `{"count": 10000, "event_types": [...], "concurrency"?: 64}` → `SimStatus`
- `POST /api/sim/loadtest` `{"start_eps": 100, "end_eps": 3000, "duration_s": 60, "event_types": [...]}` → `SimStatus`
- `POST /api/sim/stop` → `SimStatus`

ShopSim event types: `order.created`, `payment.succeeded`, `payment.failed`, `order.shipped`, `order.refunded`.

---

## 4. WebSocket `/ws`

Server → client JSON messages `{"type": string, "data": …}`. The client sends nothing (it may send
`{"type":"ping"}`). Reconnect with backoff on close.

### `stats` (every 1s)

```ts
interface StatsMessage { type: "stats"; data: {
  ts: string;
  totals: { received: number; delivered: number; failed_attempts: number; retrying: number; dlq: number };
  rates: { received_eps: number; delivered_eps: number; failed_eps: number; deferred_eps: number }; // last 1s
  success_rate: number | null;        // 0..1 over the last 60s
  latency_ms: { p50: number; p95: number; p99: number } | null;  // delivery latency over the last 60s
  endpoints: { id: string; name: string; tenant_id: string; circuit: CircuitState; status: EndpointHealth["status"];
               success_rate: number | null; delivered_eps: number; failed_eps: number; p95_ms: number | null;
               rate_limit_effective_rps: number | null; }[];
  sim: SimStatus | null;
}}
```

### `flow` (every ~100ms, sampled to at most ~80 events/s)

```ts
type FlowItem =
  | { kind: "event"; event_id: string; event_type: string; tenant_id: string; ts: string;
      deliveries: { delivery_id: string; endpoint_id: string; endpoint_name: string }[] }
  | { kind: "attempt"; event_id: string; delivery_id: string; endpoint_id: string; endpoint_name: string;
      attempt: number; outcome: "success" | "failure" | "deferred" | "dead"; status_code: number;
      latency_ms: number; next_retry_at: string | null; reason: string; ts: string };
interface FlowMessage { type: "flow"; data: { items: FlowItem[]; sample_rate: number } }  // sample_rate 0..1
```

Sampling is by event id, so a sampled event's later attempts (retries, DLQ) are also sampled.

### `infra` (every 2s)

```ts
interface InfraSnapshot {
  ts: string;
  workers: { id: string; host: string; status: "running" | "stopping" | "stale";
             started_at: string; last_seen: string; partitions: number[];
             processed_total: number; inflight: number; eps: number; }[];
  consumer_groups: { group: string; topic: string; total_lag: number; members: number;
                     partitions: { partition: number; lag: number; committed: number; end: number; owner: string | null }[] }[];
  retry_tiers: { tier: number; topic: string; delay_hint_ms: number; depth: number }[];
  dlq: { depth: number; topic_total: number };
  components: { name: "kafka" | "redis" | "postgres" | "clickhouse" | "s3"; status: "up" | "down" | "degraded";
                latency_ms: number | null; detail: string }[];
  scaler: { enabled: boolean; kind: "docker" | "kubernetes" | "none"; desired_replicas: number | null };
}
interface InfraMessage { type: "infra"; data: InfraSnapshot }
```

`consumer_groups` always includes `boomerang-delivery` on topic `boomerang.deliveries`, which drives
the "consumer lag per partition" chart. It also includes `boomerang-fanout` / `boomerang.events`,
`boomerang-recorder` / `boomerang.status`, and one entry per retry tier.

### `integrity` (every 3s)

```ts
interface IntegrityMessage { type: "integrity"; data: {
  ts: string;
  deliveries_created: number;         // unique deliveries fanned out
  delivered: number;                  // unique deliveries with a successful attempt
  dead: number; in_progress: number;  // in_progress = created - delivered - dead
  receivers_unique: number;           // unique delivery ids confirmed by the mock receivers
  receivers_duplicates: number;       // extra copies (at-least-once in action)
  lost: number;                       // max(0, delivered - receivers_unique) (should be 0)
}}
```

### `endpoint_change` (on demand)

`{"type": "endpoint_change", "data": {"endpoint_id": string}}`: refetch endpoints.

---

## 5. Webhook request format (what receivers get)

```
POST <endpoint url>
Content-Type: application/json
User-Agent: Boomerang-Webhooks/1.0
Boomerang-Id: dlv_…              (stable across retries, use it for dedupe)
Boomerang-Event-Id: evt_…
Boomerang-Event-Type: order.created
Boomerang-Attempt: 1
Boomerang-Timestamp: 1763892930  (unix seconds)
Boomerang-Signature: v1,<base64 HMAC-SHA256(secret, "<Boomerang-Id>.<timestamp>.<body>")> [v1,<sig with previous secret>]

{"id":"evt_…","type":"order.created","created_at":"…","data":{…}}
```

Receivers reject timestamps more than 5 minutes off (replay protection) and accept the request if any
listed signature verifies. The secret is the endpoint's `whsec_…` value, base64 decoded after the prefix.
