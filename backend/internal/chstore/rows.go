package chstore

import (
	"time"

	"github.com/boomerang-dev/boomerang/internal/model"
)

// chTime formats times for JSONEachRow input (best_effort parsing accepts this).
type chTime string

func ts(t time.Time) chTime { return chTime(t.UTC().Format("2006-01-02 15:04:05.000")) }

// EventRow is a row of the events table.
type EventRow struct {
	TenantID       string `json:"tenant_id"`
	EventID        string `json:"event_id"`
	EventType      string `json:"event_type"`
	CreatedAt      chTime `json:"created_at"`
	Body           string `json:"body"`
	BodyRef        string `json:"body_ref"`
	BodyBytes      int    `json:"body_bytes"`
	IdempotencyKey string `json:"idempotency_key"`
	OrderingKey    string `json:"ordering_key"`
}

// DeliveryRow is a row of the deliveries table (latest version wins).
type DeliveryRow struct {
	TenantID     string `json:"tenant_id"`
	DeliveryID   string `json:"delivery_id"`
	EventID      string `json:"event_id"`
	EndpointID   string `json:"endpoint_id"`
	EndpointName string `json:"endpoint_name"`
	EventType    string `json:"event_type"`
	EventAt      chTime `json:"event_at"`
	State        string `json:"state"`
	Attempts     int    `json:"attempts"`
	UpdatedAt    chTime `json:"updated_at"`
	Version      uint64 `json:"version"`
}

// AttemptRow is a row of the attempts table.
type AttemptRow struct {
	TenantID     string  `json:"tenant_id"`
	DeliveryID   string  `json:"delivery_id"`
	EventID      string  `json:"event_id"`
	EndpointID   string  `json:"endpoint_id"`
	EndpointName string  `json:"endpoint_name"`
	EventType    string  `json:"event_type"`
	EventAt      chTime  `json:"event_at"`
	Attempt      int     `json:"attempt"`
	Attempted    int     `json:"attempted"`
	Outcome      string  `json:"outcome"`
	State        string  `json:"state"`
	StatusCode   int     `json:"status_code"`
	ResponseBody string  `json:"response_body"`
	LatencyMs    int     `json:"latency_ms"`
	Error        string  `json:"error"`
	Reason       string  `json:"reason"`
	NextRetryAt  *chTime `json:"next_retry_at"`
	Signature    string  `json:"signature"`
	WorkerID     string  `json:"worker_id"`
	TS           chTime  `json:"ts"`
}

// RowsFromEvent converts an event status message to rows.
func RowsFromEvent(e *model.EventRecord) (EventRow, []DeliveryRow) {
	er := EventRow{
		TenantID: e.TenantID, EventID: e.EventID, EventType: e.EventType, CreatedAt: ts(e.CreatedAt),
		Body: string(e.Body), BodyRef: e.BodyRef, BodyBytes: e.BodyBytes,
		IdempotencyKey: e.IdempotencyKey, OrderingKey: e.OrderingKey,
	}
	var drs []DeliveryRow
	for _, d := range e.Deliveries {
		drs = append(drs, DeliveryRow{
			TenantID: e.TenantID, DeliveryID: d.DeliveryID, EventID: e.EventID, EndpointID: d.EndpointID,
			EndpointName: d.EndpointName, EventType: e.EventType, EventAt: ts(e.CreatedAt),
			State: model.StatePending, Attempts: 0, UpdatedAt: ts(e.CreatedAt),
			// Version 0-ish so any attempt row (later timestamps) supersedes the pending row,
			// even if fan-out is re-run after a crash.
			Version: 1,
		})
	}
	return er, drs
}

// RowsFromAttempt converts an attempt status message to rows.
func RowsFromAttempt(a *model.AttemptRecord) (AttemptRow, DeliveryRow) {
	attempts := a.Attempt
	if !a.Attempted {
		attempts = a.Attempt - 1
	}
	if attempts < 0 {
		attempts = 0
	}
	ar := AttemptRow{
		TenantID: a.TenantID, DeliveryID: a.DeliveryID, EventID: a.EventID, EndpointID: a.EndpointID,
		EndpointName: a.EndpointName, EventType: a.EventType, EventAt: ts(a.EventAt), Attempt: a.Attempt,
		Outcome: a.Outcome, State: a.State, StatusCode: a.StatusCode, ResponseBody: a.ResponseBody,
		LatencyMs: a.LatencyMs, Error: a.Error, Reason: a.Reason, Signature: a.Signature,
		WorkerID: a.WorkerID, TS: ts(a.TS),
	}
	if a.Attempted {
		ar.Attempted = 1
	}
	if a.NextRetryAt != nil {
		t := ts(*a.NextRetryAt)
		ar.NextRetryAt = &t
	}
	// Version: timestamp in ms, plus a small bias so a terminal state written in the same
	// millisecond as a deferral wins.
	v := uint64(a.TS.UnixMilli()) * 4
	switch a.State {
	case model.StateDelivered:
		v += 3
	case model.StateDead:
		v += 2
	case model.StateRetrying, model.StatePaused:
		v += 1
	}
	dr := DeliveryRow{
		TenantID: a.TenantID, DeliveryID: a.DeliveryID, EventID: a.EventID, EndpointID: a.EndpointID,
		EndpointName: a.EndpointName, EventType: a.EventType, EventAt: ts(a.EventAt), State: a.State,
		Attempts: attempts, UpdatedAt: ts(a.TS), Version: v,
	}
	return ar, dr
}
