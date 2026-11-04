package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/chstore"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/scaler"
	"github.com/boomerang-dev/boomerang/internal/stats"
)

func parseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		t := time.UnixMilli(ms)
		return &t, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (a *API) searchEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := chstore.SearchFilter{
		TenantID: r.PathValue("tid"), Type: q.Get("type"), State: q.Get("state"), EndpointID: q.Get("endpoint_id"),
		Q: q.Get("q"), Limit: 50, To: time.Now().Add(time.Minute), From: time.Now().Add(-24 * time.Hour),
	}
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 && n <= 500 {
		f.Limit = n
	}
	for name, dst := range map[string]**time.Time{"before": &f.Before} {
		t, err := parseTime(q.Get(name))
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid_time", name+" must be RFC 3339")
			return
		}
		*dst = t
	}
	if t, err := parseTime(q.Get("from")); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_time", "from must be RFC 3339")
		return
	} else if t != nil {
		f.From = *t
	}
	if t, err := parseTime(q.Get("to")); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_time", "to must be RFC 3339")
		return
	} else if t != nil {
		f.To = *t
	}
	evs, err := a.d.CH.SearchEvents(r.Context(), f)
	if err != nil {
		internalErr(w, err)
		return
	}
	if evs == nil {
		evs = []chstore.EventSummary{}
	}
	var next *string
	if len(evs) == f.Limit {
		s := evs[len(evs)-1].CreatedAt.Format(time.RFC3339Nano)
		next = &s
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"events": evs, "next_before": next})
}

func (a *API) getEvent(w http.ResponseWriter, r *http.Request) {
	tid, evid := r.PathValue("tid"), r.PathValue("evid")
	ev, err := a.d.CH.GetEvent(r.Context(), tid, evid)
	if err != nil {
		internalErr(w, err)
		return
	}
	if ev == nil {
		httpx.Error(w, http.StatusNotFound, "not_found", "event not found (it may still be in flight; try again in a moment)")
		return
	}
	ds, err := a.d.CH.EventDeliveries(r.Context(), tid, evid)
	if err != nil {
		internalErr(w, err)
		return
	}
	atts, err := a.d.CH.EventAttempts(r.Context(), tid, evid)
	if err != nil {
		internalErr(w, err)
		return
	}
	var payload any = nil
	body := []byte(ev.Body)
	if ev.BodyRef != "" {
		if b, err := a.d.Blob.Get(r.Context(), ev.BodyRef); err == nil {
			body = b
		}
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &envelope) == nil && envelope.Data != nil {
		payload = envelope.Data
	}
	opt := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	summaries := make([]chstore.DeliverySummary, 0, len(ds))
	for _, d := range ds {
		summaries = append(summaries, d.DeliverySummary)
	}
	if atts == nil {
		atts = []chstore.AttemptView{}
	}
	httpx.JSON(w, http.StatusOK, map[string]any{
		"event": map[string]any{
			"event_id": ev.EventID, "tenant_id": ev.TenantID, "event_type": ev.EventType,
			"created_at": time.UnixMilli(ev.CreatedMs).UTC(), "payload": payload, "payload_ref": opt(ev.BodyRef),
			"payload_bytes": ev.BodyBytes, "idempotency_key": opt(ev.IdempotencyKey), "ordering_key": opt(ev.OrderingKey),
		},
		"deliveries": summaries,
		"attempts":   atts,
	})
}

// replay re-enqueues deliveries with a fresh retry budget.
func (a *API) replay(ctx context.Context, tenantID string, ds []chstore.DeliveryInfo) (int, error) {
	if len(ds) == 0 {
		return 0, nil
	}
	tenant, err := a.d.Store.GetTenant(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	ids := map[string]struct{}{}
	for _, d := range ds {
		ids[d.EventID] = struct{}{}
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	events, err := a.d.CH.GetEvents(ctx, tenantID, list)
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	var recs []*kgo.Record
	n := 0
	for _, d := range ds {
		ev, ok := events[d.EventID]
		if !ok {
			continue
		}
		task := model.DeliveryTask{
			DeliveryID: d.DeliveryID, EventID: d.EventID, TenantID: tenantID, EndpointID: d.EndpointID,
			EventType: ev.EventType, OrderingKey: ev.OrderingKey, Attempt: d.Attempts + 1,
			MaxAttempts: d.Attempts + max(1, tenant.RetryMaxAttempts), EventAt: time.UnixMilli(ev.CreatedMs).UTC(),
			BodyRef: ev.BodyRef, Replay: true,
		}
		if ev.BodyRef == "" {
			task.Body = json.RawMessage(ev.Body)
		}
		tr, err := kafkax.JSONRecord(ctx, model.TopicDeliveries, task.PartitionKey(), task)
		if err != nil {
			return 0, err
		}
		st, err := kafkax.JSONRecord(ctx, model.TopicStatus, []byte(task.EventID), model.StatusMsg{Kind: "attempt", Attempt: &model.AttemptRecord{
			DeliveryID: task.DeliveryID, EventID: task.EventID, TenantID: tenantID, EndpointID: d.EndpointID,
			EndpointName: d.EndpointName, EventType: task.EventType, EventAt: task.EventAt, Attempt: task.Attempt,
			Outcome: model.OutcomeDeferred, State: model.StatePending, Reason: "Manual replay requested", TS: now,
		}})
		if err != nil {
			return 0, err
		}
		recs = append(recs, tr, st)
		n++
	}
	return n, kafkax.ProduceSync(ctx, a.d.Producer, recs...)
}

func (a *API) replayEvent(w http.ResponseWriter, r *http.Request) {
	tid := r.PathValue("tid")
	var in struct {
		DeliveryIDs []string `json:"delivery_ids"`
	}
	_ = httpx.Decode(r, maxBody, &in)
	ds, err := a.d.CH.EventDeliveries(r.Context(), tid, r.PathValue("evid"))
	if err != nil {
		internalErr(w, err)
		return
	}
	want := map[string]bool{}
	for _, id := range in.DeliveryIDs {
		want[id] = true
	}
	var pick []chstore.DeliveryInfo
	for _, d := range ds {
		if len(want) > 0 && want[d.DeliveryID] || len(want) == 0 && d.State != model.StateDelivered {
			pick = append(pick, d)
		}
	}
	n, err := a.replay(r.Context(), tid, pick)
	if err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func (a *API) replayDelivery(w http.ResponseWriter, r *http.Request) {
	tid := r.PathValue("tid")
	d, err := a.d.CH.GetDelivery(r.Context(), tid, r.PathValue("did"))
	if err != nil {
		internalErr(w, err)
		return
	}
	if d == nil {
		httpx.Error(w, http.StatusNotFound, "not_found", "delivery not found")
		return
	}
	n, err := a.replay(r.Context(), tid, []chstore.DeliveryInfo{*d})
	if err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func (a *API) replayDLQ(w http.ResponseWriter, r *http.Request) {
	e, ok := a.loadEndpoint(w, r)
	if !ok {
		return
	}
	ds, err := a.d.CH.DeadDeliveries(r.Context(), e.TenantID, e.ID, 10_000)
	if err != nil {
		internalErr(w, err)
		return
	}
	n, err := a.replay(r.Context(), e.TenantID, ds)
	if err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]int{"replayed": n})
}

func (a *API) timeseries(w http.ResponseWriter, r *http.Request) {
	minutes := 15
	if n, err := strconv.Atoi(r.URL.Query().Get("minutes")); err == nil && n > 0 && n <= 24*60 {
		minutes = n
	}
	pts, err := a.d.CH.Timeseries(r.Context(), r.URL.Query().Get("tenant_id"), minutes)
	if err != nil {
		internalErr(w, err)
		return
	}
	if pts == nil {
		pts = []chstore.TimePoint{}
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"bucket_s": 5, "points": pts})
}

// --- infrastructure ---

func (a *API) scaleWorkers(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Replicas int `json:"replicas"`
	}
	if err := httpx.Decode(r, maxBody, &in); err != nil || in.Replicas < 1 || in.Replicas > 10 {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "replicas must be 1..10")
		return
	}
	if err := a.scaler.Scale(r.Context(), in.Replicas); err != nil {
		scalerErr(w, err)
		return
	}
	a.d.Redis.Set(r.Context(), stats.ScalerDesired, in.Replicas, 0)
	httpx.JSON(w, http.StatusOK, map[string]int{"replicas": in.Replicas})
}

func (a *API) killWorker(graceful bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("wid")
		var err error
		if graceful {
			err = a.scaler.Stop(r.Context(), id)
		} else {
			err = a.scaler.Kill(r.Context(), id)
		}
		if err != nil {
			scalerErr(w, err)
			return
		}
		if graceful {
			httpx.JSON(w, http.StatusOK, map[string]string{"stopped": id})
			return
		}
		httpx.JSON(w, http.StatusOK, map[string]string{"killed": id})
	}
}

func scalerErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scaler.ErrUnavailable):
		httpx.Error(w, http.StatusNotImplemented, "scaler_unavailable", "worker scaling needs the Docker socket (compose) or in-cluster access (Kubernetes)")
	case errors.Is(err, scaler.ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "not_found", "worker not found")
	default:
		internalErr(w, err)
	}
}
