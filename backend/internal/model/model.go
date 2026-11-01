// Package model holds the types that travel between services (Kafka messages) and the
// configuration entities stored in Postgres.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Kafka topics.
const (
	TopicEvents     = "boomerang.events"
	TopicDeliveries = "boomerang.deliveries"
	TopicDLQ        = "boomerang.dlq"
	TopicStatus     = "boomerang.status"

	GroupFanout   = "boomerang-fanout"
	GroupDelivery = "boomerang-delivery"
	GroupRecorder = "boomerang-recorder"
	GroupRetryFmt = "boomerang-retry-%d"
)

// RetryTier is one tiered retry topic. Messages in a tier carry their own due time; the tier
// only bounds how long a message may wait, which keeps each tier roughly FIFO by due time.
type RetryTier struct {
	Tier     int
	Topic    string
	MaxDelay time.Duration // tasks whose delay is <= MaxDelay go to this tier
}

// RetryTiers lists tiers from shortest to longest.
var RetryTiers = []RetryTier{
	{1, "boomerang.retry.1", 15 * time.Second},
	{2, "boomerang.retry.2", 2 * time.Minute},
	{3, "boomerang.retry.3", 10 * time.Minute},
	{4, "boomerang.retry.4", 1<<63 - 1},
}

// TierFor picks the retry tier for a delay.
func TierFor(d time.Duration) RetryTier {
	for _, t := range RetryTiers {
		if d <= t.MaxDelay {
			return t
		}
	}
	return RetryTiers[len(RetryTiers)-1]
}

// Tenant is an account that sends events.
type Tenant struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	CreatedAt        time.Time `json:"created_at"`
	RateLimitRPS     int       `json:"rate_limit_rps"`
	RateLimitBurst   int       `json:"rate_limit_burst"`
	RetryMaxAttempts int       `json:"retry_max_attempts"`
	RetryScheduleMs  []int     `json:"retry_schedule_ms"`
}

// RetryDelay returns the delay before attempt `next` (2-based: the delay before attempt 2 is schedule[0]).
func (t *Tenant) RetryDelay(next int) time.Duration {
	if len(t.RetryScheduleMs) == 0 {
		return 10 * time.Second
	}
	i := next - 2
	if i < 0 {
		i = 0
	}
	if i >= len(t.RetryScheduleMs) {
		i = len(t.RetryScheduleMs) - 1
	}
	return time.Duration(t.RetryScheduleMs[i]) * time.Millisecond
}

// APIKey is a tenant credential. Only a SHA-256 hash of the key is stored.
type APIKey struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"-"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
}

// Endpoint is a subscriber URL.
type Endpoint struct {
	ID                string     `json:"id"`
	TenantID          string     `json:"tenant_id"`
	Name              string     `json:"name"`
	URL               string     `json:"url"`
	Description       string     `json:"description"`
	EventTypes        []string   `json:"event_types"`
	Paused            bool       `json:"paused"`
	RateLimitRPS      int        `json:"rate_limit_rps"`
	Secret            string     `json:"-"`
	PreviousSecret    string     `json:"-"`
	PreviousExpiresAt *time.Time `json:"-"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

// Subscribes reports whether the endpoint wants an event type. Supports "*" and "prefix.*".
func (e *Endpoint) Subscribes(eventType string) bool {
	for _, p := range e.EventTypes {
		if MatchEventType(p, eventType) {
			return true
		}
	}
	return false
}

// MatchEventType matches a subscription pattern against an event type.
func MatchEventType(pattern, eventType string) bool {
	switch {
	case pattern == "*":
		return true
	case strings.HasSuffix(pattern, ".*"):
		return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
	default:
		return pattern == eventType
	}
}

// Secrets returns the active signing secrets, newest first.
func (e *Endpoint) Secrets(now time.Time) []string {
	s := []string{e.Secret}
	if e.PreviousSecret != "" && e.PreviousExpiresAt != nil && now.Before(*e.PreviousExpiresAt) {
		s = append(s, e.PreviousSecret)
	}
	return s
}

// EventEnvelope is what the ingestion service writes to TopicEvents.
// Body is the exact JSON delivered to receivers; it is built once at ingestion so every
// retry sends byte-identical content.
type EventEnvelope struct {
	EventID        string          `json:"event_id"`
	TenantID       string          `json:"tenant_id"`
	EventType      string          `json:"event_type"`
	Body           json.RawMessage `json:"body,omitempty"`
	BodyRef        string          `json:"body_ref,omitempty"` // object storage key when the body is large
	BodyBytes      int             `json:"body_bytes"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	OrderingKey    string          `json:"ordering_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	TraceParent    string          `json:"-"`
}

// DeliveryTask is one event-to-endpoint delivery. It travels through TopicDeliveries and the
// retry tiers.
type DeliveryTask struct {
	DeliveryID    string          `json:"delivery_id"`
	EventID       string          `json:"event_id"`
	TenantID      string          `json:"tenant_id"`
	EndpointID    string          `json:"endpoint_id"`
	EventType     string          `json:"event_type"`
	OrderingKey   string          `json:"ordering_key,omitempty"`
	Attempt       int             `json:"attempt"`      // the attempt about to be made, 1-based
	MaxAttempts   int             `json:"max_attempts"` // absolute attempt number after which the task is dead
	NotBefore     time.Time       `json:"not_before,omitempty"`
	EventAt       time.Time       `json:"event_at"`
	DeferredSince *time.Time      `json:"deferred_since,omitempty"`
	DeferReason   string          `json:"defer_reason,omitempty"`      // last reported deferral reason
	DeferReported *time.Time      `json:"defer_reported_at,omitempty"` // when a deferral was last published
	Body          json.RawMessage `json:"body,omitempty"`
	BodyRef       string          `json:"body_ref,omitempty"`
	Replay        bool            `json:"replay,omitempty"`
}

// PartitionKey keeps an endpoint's deliveries for one ordering key on one partition.
func (t *DeliveryTask) PartitionKey() []byte {
	k := t.OrderingKey
	if k == "" {
		k = t.EventID
	}
	return []byte(t.EndpointID + "|" + k)
}

// DeliveryID is deterministic so that re-running fan-out after a crash produces the same
// delivery (receivers can dedupe on Boomerang-Id).
func DeliveryID(eventID, endpointID string) string {
	h := sha256.Sum256([]byte(eventID + "|" + endpointID))
	return "dlv_" + hex.EncodeToString(h[:12])
}

// NewID returns a time-ordered prefixed id.
func NewID(prefix string) string {
	u, err := uuid.NewV7()
	if err != nil {
		u = uuid.New()
	}
	return prefix + "_" + strings.ReplaceAll(u.String(), "-", "")
}

// Attempt outcomes.
const (
	OutcomeSuccess  = "success"
	OutcomeFailure  = "failure"
	OutcomeDeferred = "deferred"
	OutcomeDead     = "dead"
)

// Delivery states.
const (
	StatePending   = "pending"
	StateDelivered = "delivered"
	StateRetrying  = "retrying"
	StateDead      = "dead"
	StatePaused    = "paused"
)

// StatusMsg is published to TopicStatus for every state change. The recorder writes it to
// ClickHouse and the realtime service streams it to dashboards.
type StatusMsg struct {
	Kind    string         `json:"kind"` // "event" | "attempt"
	Event   *EventRecord   `json:"event,omitempty"`
	Attempt *AttemptRecord `json:"attempt,omitempty"`
}

// EventRecord is published by fan-out once per event.
type EventRecord struct {
	EventID        string          `json:"event_id"`
	TenantID       string          `json:"tenant_id"`
	EventType      string          `json:"event_type"`
	CreatedAt      time.Time       `json:"created_at"`
	Body           json.RawMessage `json:"body,omitempty"`
	BodyRef        string          `json:"body_ref,omitempty"`
	BodyBytes      int             `json:"body_bytes"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	OrderingKey    string          `json:"ordering_key,omitempty"`
	Deliveries     []DeliveryRef   `json:"deliveries"`
}

// DeliveryRef names a delivery created by fan-out.
type DeliveryRef struct {
	DeliveryID   string `json:"delivery_id"`
	EndpointID   string `json:"endpoint_id"`
	EndpointName string `json:"endpoint_name"`
}

// AttemptRecord describes one delivery attempt (or a deferral / dead-lettering decision).
type AttemptRecord struct {
	DeliveryID   string     `json:"delivery_id"`
	EventID      string     `json:"event_id"`
	TenantID     string     `json:"tenant_id"`
	EndpointID   string     `json:"endpoint_id"`
	EndpointName string     `json:"endpoint_name"`
	EventType    string     `json:"event_type"`
	EventAt      time.Time  `json:"event_at"`
	Attempt      int        `json:"attempt"`
	Attempted    bool       `json:"attempted"` // false when no HTTP request was made (deferral)
	Outcome      string     `json:"outcome"`
	State        string     `json:"state"` // resulting delivery state
	StatusCode   int        `json:"status_code"`
	ResponseBody string     `json:"response_body"`
	LatencyMs    int        `json:"latency_ms"`
	Error        string     `json:"error"`
	Reason       string     `json:"reason"`
	NextRetryAt  *time.Time `json:"next_retry_at"`
	Signature    string     `json:"signature"`
	WorkerID     string     `json:"worker_id"`
	TS           time.Time  `json:"ts"`
}

// ShopSimEventTypes are the demo event types.
var ShopSimEventTypes = []string{"order.created", "payment.succeeded", "payment.failed", "order.shipped", "order.refunded"}
