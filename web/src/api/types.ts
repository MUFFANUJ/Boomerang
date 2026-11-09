// Types for the Boomerang REST + WebSocket contract (docs/API.md).
// Keep this file a 1:1 mirror of the contract; UI-only types live next to their components.

/* ------------------------------------------------------------------ */
/* Common                                                              */
/* ------------------------------------------------------------------ */

/** Error body for every non-2xx response. */
export interface ApiErrorBody {
  error: string;
  code: string;
}

/* ------------------------------------------------------------------ */
/* 1. Public ingestion API                                             */
/* ------------------------------------------------------------------ */

export interface IngestEventRequest {
  type: string;
  data: unknown;
  ordering_key?: string;
}

export interface IngestEventResponse {
  id: string;
  status: "accepted" | "duplicate";
}

export interface IngestBatchRequest {
  events: (IngestEventRequest & { idempotency_key?: string })[];
}

export interface IngestBatchResponse {
  ids: string[];
}

/* ------------------------------------------------------------------ */
/* 2. Management API                                                   */
/* ------------------------------------------------------------------ */

export interface Tenant {
  id: string;
  name: string;
  created_at: string;
  /** ingestion requests per second */
  rate_limit_rps: number;
  rate_limit_burst: number;
  /** total attempts including the first one */
  retry_max_attempts: number;
  /** delay before attempt 2, 3, …; last value repeats */
  retry_schedule_ms: number[];
}

export interface CreateTenantRequest {
  name: string;
}

export interface PatchTenantRequest {
  name?: string;
  rate_limit_rps?: number;
  rate_limit_burst?: number;
  retry_max_attempts?: number;
  retry_schedule_ms?: number[];
}

export interface ApiKey {
  id: string;
  name: string;
  prefix: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
}

export type CreatedApiKey = ApiKey & { key: string };

export type CircuitState = "closed" | "open" | "half_open";

export type EndpointStatus = "healthy" | "degraded" | "failing" | "paused" | "circuit_open";

export interface EndpointHealth {
  circuit: CircuitState;
  consecutive_failures: number;
  circuit_opened_at: string | null;
  /** 0..1 over the last 5 minutes, null if there were no attempts */
  success_rate: number | null;
  attempts_5m: number;
  failures_5m: number;
  p95_latency_ms: number | null;
  /** 0..100 health score */
  score: number;
  status: EndpointStatus;
  /** adaptive limiter's current rate */
  rate_limit_effective_rps: number | null;
}

export interface Endpoint {
  id: string;
  tenant_id: string;
  name: string;
  url: string;
  description: string;
  /** exact types, "order.*" style wildcards, or "*" */
  event_types: string[];
  paused: boolean;
  /** max deliveries/s to this endpoint, 0 means unlimited */
  rate_limit_rps: number;
  /** first chars of the current signing secret, e.g. "whsec_Ab12…" */
  secret_prefix: string;
  created_at: string;
  updated_at: string;
  health: EndpointHealth;
}

export interface CreateEndpointRequest {
  name: string;
  url: string;
  description?: string;
  event_types: string[];
  rate_limit_rps?: number;
}

export type PatchEndpointRequest = Partial<CreateEndpointRequest>;

export interface EndpointSecret {
  secret: string;
  previous_secret: string | null;
  previous_expires_at: string | null;
}

export interface ReplayResponse {
  replayed: number;
}

/* Event Explorer */

export type DeliveryState = "pending" | "delivered" | "retrying" | "dead" | "paused";

export interface DeliverySummary {
  delivery_id: string;
  endpoint_id: string;
  endpoint_name: string;
  state: DeliveryState;
  attempts: number;
  updated_at: string;
}

export interface EventSummary {
  event_id: string;
  event_type: string;
  created_at: string;
  deliveries: DeliverySummary[];
}

export interface EventListResponse {
  events: EventSummary[];
  next_before: string | null;
}

export interface EventListQuery {
  type?: string;
  state?: DeliveryState | "";
  endpoint_id?: string;
  from?: string;
  to?: string;
  q?: string;
  limit?: number;
  before?: string;
}

export type AttemptOutcome = "success" | "failure" | "deferred" | "dead";

export interface Attempt {
  delivery_id: string;
  endpoint_id: string;
  endpoint_name: string;
  /** 1-based */
  attempt: number;
  outcome: AttemptOutcome;
  /** 0 if there was no HTTP response (timeout, connection refused, circuit open) */
  status_code: number;
  /** truncated to 1 KiB */
  response_body: string;
  latency_ms: number;
  /** transport error, if any */
  error: string;
  /** why it was retried, deferred, or dead-lettered (human readable) */
  reason: string;
  next_retry_at: string | null;
  /** value of the Boomerang-Signature header sent */
  signature: string;
  worker_id: string;
  ts: string;
}

export interface EventDetail {
  event: {
    event_id: string;
    tenant_id: string;
    event_type: string;
    created_at: string;
    /** parsed JSON */
    payload: unknown;
    /** set when the payload was offloaded to object storage */
    payload_ref: string | null;
    payload_bytes: number;
    idempotency_key: string | null;
    ordering_key: string | null;
  };
  deliveries: DeliverySummary[];
  /** ordered by ts ascending */
  attempts: Attempt[];
}

export interface ReplayEventRequest {
  delivery_ids?: string[];
}

/* Chaos Playground */

export type ReceiverName = "inventory" | "email" | "analytics";

export interface ReceiverChaos {
  /** 0..1 */
  failure_rate: number;
  delay_ms: number;
  down: boolean;
}

export interface ReceiverStats {
  received: number;
  unique: number;
  duplicates: number;
  invalid_signatures: number;
  injected_failures: number;
}

export interface Receiver {
  name: ReceiverName;
  display_name: string;
  url: string;
  chaos: ReceiverChaos;
  mode: "healthy" | "slow" | "flaky" | "down";
  stats: ReceiverStats;
}

export interface ChaosPreset {
  id: string;
  name: string;
  description: string;
}

export interface ChaosResponse {
  receivers: Receiver[];
  presets: ChaosPreset[];
}

export interface ChaosPresetResponse {
  receivers: Receiver[];
}

/* Infrastructure */

export interface ScaleResponse {
  replicas: number;
}

export interface KillResponse {
  killed: string;
}

/* Metrics history */

export interface TimeseriesPoint {
  ts: string;
  delivered: number;
  failed: number;
  p95_ms: number;
}

export interface TimeseriesResponse {
  bucket_s: number;
  points: TimeseriesPoint[];
}

/* ------------------------------------------------------------------ */
/* 3. ShopSim control                                                  */
/* ------------------------------------------------------------------ */

export interface LatencyPercentiles {
  p50: number;
  p95: number;
  p99: number;
}

export interface LoadTestStep {
  t_s: number;
  target_eps: number;
  achieved_eps: number;
  p95_ms: number;
  errors: number;
}

export interface SimStatus {
  mode: "idle" | "stream" | "burst" | "loadtest";
  running: boolean;
  /** target rate (stream/loadtest) */
  rate_eps: number;
  event_types: string[];
  sent: number;
  accepted: number;
  duplicates: number;
  errors: number;
  rate_limited: number;
  /** measured over the last second */
  current_eps: number;
  /** ingestion API latency seen by ShopSim */
  latency_ms: LatencyPercentiles;
  burst: { total: number; done: number } | null;
  loadtest: {
    start_eps: number;
    end_eps: number;
    duration_s: number;
    elapsed_s: number;
    steps: LoadTestStep[];
  } | null;
  last_event: { id: string; type: string; ts: string } | null;
}

export interface SimSingleRequest {
  type: string;
}

export interface SimSingleResponse {
  id: string;
  type: string;
}

export interface SimStreamRequest {
  rate_eps: number;
  event_types: string[];
}

export interface SimBurstRequest {
  count: number;
  event_types: string[];
  concurrency?: number;
}

export interface SimLoadTestRequest {
  start_eps: number;
  end_eps: number;
  duration_s: number;
  event_types: string[];
}

/* ------------------------------------------------------------------ */
/* 4. WebSocket /ws                                                    */
/* ------------------------------------------------------------------ */

export interface StatsEndpoint {
  id: string;
  name: string;
  tenant_id: string;
  circuit: CircuitState;
  status: EndpointHealth["status"];
  success_rate: number | null;
  delivered_eps: number;
  failed_eps: number;
  p95_ms: number | null;
  rate_limit_effective_rps: number | null;
}

export interface StatsData {
  ts: string;
  totals: { received: number; delivered: number; failed_attempts: number; retrying: number; dlq: number };
  /** last 1s */
  rates: { received_eps: number; delivered_eps: number; failed_eps: number; deferred_eps: number };
  /** 0..1 over the last 60s */
  success_rate: number | null;
  /** delivery latency over the last 60s */
  latency_ms: LatencyPercentiles | null;
  endpoints: StatsEndpoint[];
  sim: SimStatus | null;
}

export interface StatsMessage {
  type: "stats";
  data: StatsData;
}

export interface FlowDeliveryRef {
  delivery_id: string;
  endpoint_id: string;
  endpoint_name: string;
}

export interface FlowEventItem {
  kind: "event";
  event_id: string;
  event_type: string;
  tenant_id: string;
  ts: string;
  deliveries: FlowDeliveryRef[];
}

export interface FlowAttemptItem {
  kind: "attempt";
  event_id: string;
  delivery_id: string;
  endpoint_id: string;
  endpoint_name: string;
  attempt: number;
  outcome: AttemptOutcome;
  status_code: number;
  latency_ms: number;
  next_retry_at: string | null;
  reason: string;
  ts: string;
}

export type FlowItem = FlowEventItem | FlowAttemptItem;

export interface FlowData {
  items: FlowItem[];
  /** 0..1 */
  sample_rate: number;
}

export interface FlowMessage {
  type: "flow";
  data: FlowData;
}

export type WorkerStatus = "running" | "stopping" | "stale";

export interface InfraWorker {
  id: string;
  host: string;
  status: WorkerStatus;
  started_at: string;
  last_seen: string;
  partitions: number[];
  processed_total: number;
  inflight: number;
  eps: number;
}

export interface PartitionLag {
  partition: number;
  lag: number;
  committed: number;
  end: number;
  owner: string | null;
}

export interface ConsumerGroup {
  group: string;
  topic: string;
  total_lag: number;
  members: number;
  partitions: PartitionLag[];
}

export interface RetryTier {
  tier: number;
  topic: string;
  delay_hint_ms: number;
  depth: number;
}

export type ComponentName = "kafka" | "redis" | "postgres" | "clickhouse" | "s3";

export interface ComponentHealth {
  name: ComponentName;
  status: "up" | "down" | "degraded";
  latency_ms: number | null;
  detail: string;
}

export interface InfraSnapshot {
  ts: string;
  workers: InfraWorker[];
  consumer_groups: ConsumerGroup[];
  retry_tiers: RetryTier[];
  dlq: { depth: number; topic_total: number };
  components: ComponentHealth[];
  scaler: { enabled: boolean; kind: "docker" | "kubernetes" | "none"; desired_replicas: number | null };
}

export interface InfraMessage {
  type: "infra";
  data: InfraSnapshot;
}

export interface IntegrityData {
  ts: string;
  /** unique deliveries fanned out */
  deliveries_created: number;
  /** unique deliveries with a successful attempt */
  delivered: number;
  dead: number;
  /** created - delivered - dead */
  in_progress: number;
  /** unique delivery ids confirmed by the mock receivers */
  receivers_unique: number;
  /** extra copies (at-least-once in action) */
  receivers_duplicates: number;
  /** max(0, delivered - receivers_unique) (should be 0) */
  lost: number;
}

export interface IntegrityMessage {
  type: "integrity";
  data: IntegrityData;
}

export interface EndpointChangeMessage {
  type: "endpoint_change";
  data: { endpoint_id: string };
}

export type ServerMessage =
  | StatsMessage
  | FlowMessage
  | InfraMessage
  | IntegrityMessage
  | EndpointChangeMessage;

export type ClientMessage = { type: "ping" };
