package chstore

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// DeliverySummary is the explorer's per-delivery view.
type DeliverySummary struct {
	DeliveryID   string    `json:"delivery_id"`
	EndpointID   string    `json:"endpoint_id"`
	EndpointName string    `json:"endpoint_name"`
	State        string    `json:"state"`
	Attempts     int       `json:"attempts"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// EventSummary is one row in the explorer list.
type EventSummary struct {
	EventID    string            `json:"event_id"`
	EventType  string            `json:"event_type"`
	CreatedAt  time.Time         `json:"created_at"`
	Deliveries []DeliverySummary `json:"deliveries"`
}

// SearchFilter filters the explorer.
type SearchFilter struct {
	TenantID   string
	Type       string
	State      string
	EndpointID string
	From, To   time.Time
	Before     *time.Time
	Q          string
	Limit      int
}

type searchRow struct {
	EventID   string   `json:"event_id"`
	EventType string   `json:"event_type"`
	CreatedMs int64    `json:"created_ms"`
	DIDs      []string `json:"d_ids"`
	EPIDs     []string `json:"ep_ids"`
	EPNames   []string `json:"ep_names"`
	States    []string `json:"states"`
	Attempts  []int    `json:"attempts"`
	Updated   []int64  `json:"updated"`
}

// SearchEvents lists events with their delivery states, newest first.
func (c *Client) SearchEvents(ctx context.Context, f SearchFilter) ([]EventSummary, error) {
	p := map[string]string{
		"tenant": f.TenantID, "from": FormatTime(f.From), "to": FormatTime(f.To),
		"limit": strconv.Itoa(f.Limit),
	}
	var where []string
	where = append(where, "tenant_id = {tenant:String}", "event_at >= {from:DateTime64(3)}", "event_at <= {to:DateTime64(3)}")
	if f.Type != "" {
		where = append(where, "event_type = {type:String}")
		p["type"] = f.Type
	}
	if f.EndpointID != "" {
		where = append(where, "endpoint_id = {endpoint:String}")
		p["endpoint"] = f.EndpointID
	}
	if f.State != "" {
		where = append(where, "state = {state:String}")
		p["state"] = f.State
	}
	if f.Before != nil {
		where = append(where, "event_at < {before:DateTime64(3)}")
		p["before"] = FormatTime(*f.Before)
	}
	if f.Q != "" {
		p["q"] = f.Q
		where = append(where, `(startsWith(event_id, {q:String}) OR event_id IN (
			SELECT event_id FROM events WHERE tenant_id = {tenant:String}
			AND created_at >= {from:DateTime64(3)} AND created_at <= {to:DateTime64(3)}
			AND positionCaseInsensitive(body, {q:String}) > 0))`)
	}
	sql := `SELECT event_id, any(event_type) AS event_type, toUnixTimestamp64Milli(min(event_at)) AS created_ms,
		groupArray(delivery_id) AS d_ids, groupArray(endpoint_id) AS ep_ids, groupArray(endpoint_name) AS ep_names,
		groupArray(state) AS states, groupArray(attempts) AS attempts, groupArray(toUnixTimestamp64Milli(updated_at)) AS updated
		FROM deliveries FINAL WHERE ` + strings.Join(where, " AND ") + `
		GROUP BY event_id ORDER BY created_ms DESC, event_id DESC LIMIT {limit:UInt32}`
	rows, err := Query[searchRow](ctx, c, sql, p)
	if err != nil {
		return nil, err
	}
	out := make([]EventSummary, 0, len(rows))
	for _, r := range rows {
		es := EventSummary{EventID: r.EventID, EventType: r.EventType, CreatedAt: time.UnixMilli(r.CreatedMs).UTC()}
		for i := range r.DIDs {
			es.Deliveries = append(es.Deliveries, DeliverySummary{
				DeliveryID: r.DIDs[i], EndpointID: r.EPIDs[i], EndpointName: r.EPNames[i],
				State: r.States[i], Attempts: r.Attempts[i], UpdatedAt: time.UnixMilli(r.Updated[i]).UTC(),
			})
		}
		out = append(out, es)
	}
	return out, nil
}

// EventDetailRow is an event as stored.
type EventDetailRow struct {
	EventID        string `json:"event_id"`
	TenantID       string `json:"tenant_id"`
	EventType      string `json:"event_type"`
	CreatedMs      int64  `json:"created_ms"`
	Body           string `json:"body"`
	BodyRef        string `json:"body_ref"`
	BodyBytes      int    `json:"body_bytes"`
	IdempotencyKey string `json:"idempotency_key"`
	OrderingKey    string `json:"ordering_key"`
}

// GetEvent returns one event.
func (c *Client) GetEvent(ctx context.Context, tenantID, eventID string) (*EventDetailRow, error) {
	rows, err := Query[EventDetailRow](ctx, c, `SELECT event_id, tenant_id, event_type, toUnixTimestamp64Milli(created_at) AS created_ms,
		body, body_ref, body_bytes, idempotency_key, ordering_key
		FROM events FINAL WHERE tenant_id = {tenant:String} AND event_id = {event:String} LIMIT 1`,
		map[string]string{"tenant": tenantID, "event": eventID})
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return &rows[0], nil
}

// GetEvents returns several events (used by bulk replay).
func (c *Client) GetEvents(ctx context.Context, tenantID string, ids []string) (map[string]EventDetailRow, error) {
	out := map[string]EventDetailRow{}
	for start := 0; start < len(ids); start += 1000 {
		end := min(start+1000, len(ids))
		arr, _ := json.Marshal(ids[start:end])
		rows, err := Query[EventDetailRow](ctx, c, `SELECT event_id, tenant_id, event_type, toUnixTimestamp64Milli(created_at) AS created_ms,
			body, body_ref, body_bytes, idempotency_key, ordering_key
			FROM events FINAL WHERE tenant_id = {tenant:String} AND event_id IN {ids:Array(String)}`,
			map[string]string{"tenant": tenantID, "ids": strings.ReplaceAll(string(arr), `"`, `'`)})
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			out[r.EventID] = r
		}
	}
	return out, nil
}

type deliveryRow struct {
	DeliveryID   string `json:"delivery_id"`
	EventID      string `json:"event_id"`
	EndpointID   string `json:"endpoint_id"`
	EndpointName string `json:"endpoint_name"`
	EventType    string `json:"event_type"`
	State        string `json:"state"`
	Attempts     int    `json:"attempts"`
	UpdatedMs    int64  `json:"updated_ms"`
}

// DeliveryInfo is a delivery with its event id, for replay.
type DeliveryInfo struct {
	DeliverySummary
	EventID   string
	EventType string
}

func (c *Client) deliveries(ctx context.Context, where string, p map[string]string, limit int) ([]DeliveryInfo, error) {
	p["limit"] = strconv.Itoa(limit)
	rows, err := Query[deliveryRow](ctx, c, `SELECT delivery_id, event_id, endpoint_id, endpoint_name, event_type, state, attempts,
		toUnixTimestamp64Milli(updated_at) AS updated_ms FROM deliveries FINAL WHERE `+where+` ORDER BY endpoint_name LIMIT {limit:UInt32}`, p)
	if err != nil {
		return nil, err
	}
	out := make([]DeliveryInfo, 0, len(rows))
	for _, r := range rows {
		out = append(out, DeliveryInfo{
			DeliverySummary: DeliverySummary{DeliveryID: r.DeliveryID, EndpointID: r.EndpointID, EndpointName: r.EndpointName,
				State: r.State, Attempts: r.Attempts, UpdatedAt: time.UnixMilli(r.UpdatedMs).UTC()},
			EventID: r.EventID, EventType: r.EventType,
		})
	}
	return out, nil
}

// EventDeliveries returns all deliveries of an event.
func (c *Client) EventDeliveries(ctx context.Context, tenantID, eventID string) ([]DeliveryInfo, error) {
	return c.deliveries(ctx, "tenant_id = {tenant:String} AND event_id = {event:String}",
		map[string]string{"tenant": tenantID, "event": eventID}, 1000)
}

// GetDelivery returns one delivery.
func (c *Client) GetDelivery(ctx context.Context, tenantID, deliveryID string) (*DeliveryInfo, error) {
	ds, err := c.deliveries(ctx, "tenant_id = {tenant:String} AND delivery_id = {d:String}",
		map[string]string{"tenant": tenantID, "d": deliveryID}, 1)
	if err != nil || len(ds) == 0 {
		return nil, err
	}
	return &ds[0], nil
}

// DeadDeliveries returns dead-lettered deliveries for an endpoint.
func (c *Client) DeadDeliveries(ctx context.Context, tenantID, endpointID string, limit int) ([]DeliveryInfo, error) {
	return c.deliveries(ctx, "tenant_id = {tenant:String} AND endpoint_id = {ep:String} AND state = 'dead'",
		map[string]string{"tenant": tenantID, "ep": endpointID}, limit)
}

// AttemptView is an attempt as returned to the dashboard.
type AttemptView struct {
	DeliveryID   string     `json:"delivery_id"`
	EndpointID   string     `json:"endpoint_id"`
	EndpointName string     `json:"endpoint_name"`
	Attempt      int        `json:"attempt"`
	Outcome      string     `json:"outcome"`
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

type attemptRow struct {
	DeliveryID   string `json:"delivery_id"`
	EndpointID   string `json:"endpoint_id"`
	EndpointName string `json:"endpoint_name"`
	Attempt      int    `json:"attempt"`
	Outcome      string `json:"outcome"`
	StatusCode   int    `json:"status_code"`
	ResponseBody string `json:"response_body"`
	LatencyMs    int    `json:"latency_ms"`
	Error        string `json:"error"`
	Reason       string `json:"reason"`
	NextMs       *int64 `json:"next_ms"`
	Signature    string `json:"signature"`
	WorkerID     string `json:"worker_id"`
	TSMs         int64  `json:"ts_ms"`
}

// EventAttempts returns every attempt for an event, oldest first.
func (c *Client) EventAttempts(ctx context.Context, tenantID, eventID string) ([]AttemptView, error) {
	rows, err := Query[attemptRow](ctx, c, `SELECT delivery_id, endpoint_id, endpoint_name, attempt, outcome, status_code, response_body,
		latency_ms, error, reason, toUnixTimestamp64Milli(next_retry_at) AS next_ms, signature, worker_id, toUnixTimestamp64Milli(ts) AS ts_ms
		FROM attempts WHERE tenant_id = {tenant:String} AND event_id = {event:String} ORDER BY ts, attempt`,
		map[string]string{"tenant": tenantID, "event": eventID})
	if err != nil {
		return nil, err
	}
	out := make([]AttemptView, 0, len(rows))
	for _, r := range rows {
		v := AttemptView{DeliveryID: r.DeliveryID, EndpointID: r.EndpointID, EndpointName: r.EndpointName, Attempt: r.Attempt,
			Outcome: r.Outcome, StatusCode: r.StatusCode, ResponseBody: r.ResponseBody, LatencyMs: r.LatencyMs, Error: r.Error,
			Reason: r.Reason, Signature: r.Signature, WorkerID: r.WorkerID, TS: time.UnixMilli(r.TSMs).UTC()}
		if r.NextMs != nil {
			t := time.UnixMilli(*r.NextMs).UTC()
			v.NextRetryAt = &t
		}
		out = append(out, v)
	}
	return out, nil
}

// TimePoint is one timeseries bucket.
type TimePoint struct {
	TS        time.Time `json:"ts"`
	Delivered int64     `json:"delivered"`
	Failed    int64     `json:"failed"`
	P95       float64   `json:"p95_ms"`
}

// Timeseries returns delivered/failed counts in 5s buckets.
func (c *Client) Timeseries(ctx context.Context, tenantID string, minutes int) ([]TimePoint, error) {
	type row struct {
		B         int64   `json:"b"`
		Delivered int64   `json:"delivered"`
		Failed    int64   `json:"failed"`
		P95       float64 `json:"p95"`
	}
	p := map[string]string{"m": strconv.Itoa(minutes)}
	tf := ""
	if tenantID != "" {
		tf = " AND tenant_id = {tenant:String}"
		p["tenant"] = tenantID
	}
	rows, err := Query[row](ctx, c, `SELECT toUnixTimestamp(toStartOfInterval(ts, INTERVAL 5 SECOND)) AS b,
		countIf(outcome = 'success') AS delivered,
		countIf(attempted = 1 AND outcome != 'success') AS failed,
		ifNotFinite(quantileIf(0.95)(latency_ms, attempted = 1), 0) AS p95
		FROM attempts WHERE ts >= now64(3) - toIntervalMinute({m:UInt32})`+tf+`
		GROUP BY b ORDER BY b`, p)
	if err != nil {
		return nil, err
	}
	out := make([]TimePoint, 0, len(rows))
	for _, r := range rows {
		out = append(out, TimePoint{TS: time.Unix(r.B, 0).UTC(), Delivered: r.Delivered, Failed: r.Failed, P95: r.P95})
	}
	return out, nil
}

// EndpointStats are 5-minute delivery stats for an endpoint.
type EndpointStats struct {
	EndpointID string  `json:"endpoint_id"`
	Attempts   int64   `json:"attempts"`
	Failures   int64   `json:"failures"`
	P95        float64 `json:"p95"`
}

// EndpointHealth returns stats per endpoint over the last 5 minutes.
func (c *Client) EndpointHealth(ctx context.Context, tenantID string) (map[string]EndpointStats, error) {
	rows, err := Query[EndpointStats](ctx, c, `SELECT endpoint_id, countIf(attempted = 1) AS attempts,
		countIf(attempted = 1 AND outcome != 'success') AS failures,
		ifNotFinite(quantileIf(0.95)(latency_ms, attempted = 1), 0) AS p95
		FROM attempts WHERE tenant_id = {tenant:String} AND ts >= now64(3) - INTERVAL 5 MINUTE GROUP BY endpoint_id`,
		map[string]string{"tenant": tenantID})
	if err != nil {
		return nil, err
	}
	out := map[string]EndpointStats{}
	for _, r := range rows {
		out[r.EndpointID] = r
	}
	return out, nil
}

// Integrity are delivery accounting totals since a point in time.
type Integrity struct {
	Created   int64 `json:"created"`
	Delivered int64 `json:"delivered"`
	Dead      int64 `json:"dead"`
}

// IntegritySince counts unique deliveries to the given endpoints for events created since t.
func (c *Client) IntegritySince(ctx context.Context, since time.Time, endpointIDs []string) (*Integrity, error) {
	if len(endpointIDs) == 0 {
		return &Integrity{}, nil
	}
	arr, _ := json.Marshal(endpointIDs)
	rows, err := Query[Integrity](ctx, c, `SELECT
		(SELECT uniqExact(delivery_id) FROM deliveries WHERE event_at >= {since:DateTime64(3)} AND endpoint_id IN {eps:Array(String)}) AS created,
		(SELECT uniqExact(delivery_id) FROM attempts WHERE outcome = 'success' AND event_at >= {since:DateTime64(3)} AND endpoint_id IN {eps:Array(String)}) AS delivered,
		(SELECT count() FROM deliveries FINAL WHERE state = 'dead' AND event_at >= {since:DateTime64(3)} AND endpoint_id IN {eps:Array(String)}) AS dead`,
		map[string]string{"since": FormatTime(since), "eps": strings.ReplaceAll(string(arr), `"`, `'`)})
	if err != nil || len(rows) == 0 {
		return &Integrity{}, err
	}
	return &rows[0], nil
}

// DeadCount counts currently dead-lettered deliveries.
func (c *Client) DeadCount(ctx context.Context) (int64, error) {
	type row struct {
		N int64 `json:"n"`
	}
	rows, err := Query[row](ctx, c, `SELECT count() AS n FROM deliveries FINAL WHERE state = 'dead'`, nil)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	return rows[0].N, nil
}

// DeadDeliveryIDs lists every currently dead-lettered delivery (used to seed the DLQ set).
func (c *Client) DeadDeliveryIDs(ctx context.Context) ([]string, error) {
	type row struct {
		ID string `json:"delivery_id"`
	}
	rows, err := Query[row](ctx, c, `SELECT delivery_id FROM deliveries FINAL WHERE state = 'dead'`, nil)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids, nil
}
