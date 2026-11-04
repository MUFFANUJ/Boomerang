package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"

	"github.com/boomerang-dev/boomerang/internal/cache"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/obs"
	"github.com/boomerang-dev/boomerang/internal/store"
)

const (
	maxEventBytes = 1 << 20
	maxBatch      = 500
	idemTTL       = 24 * time.Hour
)

var eventTypeRe = regexp.MustCompile(`^[a-zA-Z0-9_\-]+(\.[a-zA-Z0-9_\-]+)*$`)

type eventIn struct {
	Type           string          `json:"type"`
	Data           json.RawMessage `json:"data"`
	OrderingKey    string          `json:"ordering_key"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (e *eventIn) validate() error {
	if e.Type == "" || len(e.Type) > 100 || !eventTypeRe.MatchString(e.Type) {
		return errors.New("type is required and must look like \"order.created\"")
	}
	if len(bytes.TrimSpace(e.Data)) == 0 {
		return errors.New("data is required")
	}
	if !json.Valid(e.Data) {
		return errors.New("data must be valid JSON")
	}
	if len(e.OrderingKey) > 200 || len(e.IdempotencyKey) > 200 {
		return errors.New("ordering_key and idempotency_key must be at most 200 characters")
	}
	return nil
}

// authenticate resolves the API key and applies the tenant's rate limit.
func (a *API) authenticate(w http.ResponseWriter, r *http.Request, cost int) (*model.Tenant, bool) {
	raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || raw == "" {
		httpx.Error(w, http.StatusUnauthorized, "unauthorized", "missing API key (Authorization: Bearer <key>)")
		return nil, false
	}
	ki, err := a.d.Cache.APIKey(r.Context(), raw)
	if errors.Is(err, store.ErrNotFound) || (ki != nil && ki.Revoked) {
		httpx.Error(w, http.StatusUnauthorized, "unauthorized", "invalid or revoked API key")
		return nil, false
	}
	if err != nil {
		internalErr(w, err)
		return nil, false
	}
	a.touchKey(ki)
	tenant, err := a.d.Cache.Tenant(r.Context(), ki.TenantID)
	if err != nil {
		storeErr(w, err, "tenant")
		return nil, false
	}
	res, err := a.limiter.Take(r.Context(), "rl:tenant:"+tenant.ID, float64(tenant.RateLimitRPS), float64(tenant.RateLimitBurst), cost)
	if err != nil {
		// Fail open: a Redis blip must not stop ingestion.
		return tenant, true
	}
	if !res.Allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())+1))
		obs.EventsIngested.WithLabelValues(tenant.ID, "rate_limited").Add(float64(cost))
		httpx.Error(w, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf("tenant rate limit of %d events/s exceeded", tenant.RateLimitRPS))
		return nil, false
	}
	return tenant, true
}

// touchKey records last-used time at most once a minute per key (checked in memory first, so
// the hot path normally costs nothing).
func (a *API) touchKey(ki *cache.KeyInfo) {
	now := time.Now()
	if last, ok := a.keyTouched.Load(ki.KeyID); ok && now.Sub(last.(time.Time)) < time.Minute {
		return
	}
	a.keyTouched.Store(ki.KeyID, now)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if ok, _ := a.d.Redis.SetNX(ctx, "keyused:"+ki.KeyID, 1, time.Minute).Result(); ok {
			_ = a.d.Store.TouchKey(ctx, ki.KeyID)
		}
	}()
}

type prepared struct {
	env       model.EventEnvelope
	duplicate bool
	idemKey   string // redis key to release if the write fails
}

// prepare validates an event, claims its idempotency key and builds the envelope.
func (a *API) prepare(ctx context.Context, tenant *model.Tenant, in *eventIn) (*prepared, error) {
	now := time.Now().UTC()
	p := &prepared{env: model.EventEnvelope{
		EventID: model.NewID("evt"), TenantID: tenant.ID, EventType: in.Type, CreatedAt: now,
		IdempotencyKey: in.IdempotencyKey, OrderingKey: in.OrderingKey,
	}}
	if in.IdempotencyKey != "" {
		k := "idem:" + tenant.ID + ":" + in.IdempotencyKey
		ok, err := a.d.Redis.SetNX(ctx, k, p.env.EventID, idemTTL).Result()
		if err != nil {
			return nil, err
		}
		if !ok {
			orig, err := a.d.Redis.Get(ctx, k).Result()
			if err != nil {
				return nil, err
			}
			p.env.EventID, p.duplicate = orig, true
			return p, nil
		}
		p.idemKey = k
	}
	body, err := json.Marshal(struct {
		ID        string          `json:"id"`
		Type      string          `json:"type"`
		CreatedAt time.Time       `json:"created_at"`
		Data      json.RawMessage `json:"data"`
	}{p.env.EventID, in.Type, now, in.Data})
	if err != nil {
		return nil, err
	}
	p.env.BodyBytes = len(body)
	if len(body) > a.d.Cfg.PayloadInlineMaxBytes {
		ref, err := a.d.Blob.Put(ctx, fmt.Sprintf("events/%s/%s.json", tenant.ID, p.env.EventID), body)
		if err != nil {
			return nil, fmt.Errorf("offload payload: %w", err)
		}
		p.env.BodyRef = ref
	} else {
		p.env.Body = body
	}
	return p, nil
}

func (a *API) record(ctx context.Context, p *prepared) (*kgo.Record, error) {
	key := p.env.OrderingKey
	if key == "" {
		key = p.env.EventID
	}
	return kafkax.JSONRecord(ctx, model.TopicEvents, []byte(p.env.TenantID+"|"+key), p.env)
}

func (a *API) release(ps []*prepared) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, p := range ps {
		if p.idemKey != "" {
			a.d.Redis.Del(ctx, p.idemKey)
		}
	}
}

func (a *API) ingestOne(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { obs.IngestLatency.Observe(time.Since(start).Seconds()) }()
	tenant, ok := a.authenticate(w, r, 1)
	if !ok {
		return
	}
	var in eventIn
	if err := httpx.Decode(r, maxEventBytes, &in); err != nil {
		if errors.Is(err, httpx.ErrTooLarge) {
			httpx.Error(w, http.StatusRequestEntityTooLarge, "payload_too_large", "event exceeds 1 MiB")
			return
		}
		httpx.Error(w, http.StatusBadRequest, "invalid_json", "body must be a JSON object")
		return
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		in.IdempotencyKey = k
	}
	if err := in.validate(); err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	ctx, span := obs.Tracer("api").Start(r.Context(), "ingest")
	defer span.End()
	p, err := a.prepare(ctx, tenant, &in)
	if err != nil {
		internalErr(w, err)
		return
	}
	span.SetAttributes(attribute.String("event.id", p.env.EventID), attribute.String("event.type", in.Type))
	if p.duplicate {
		obs.EventsIngested.WithLabelValues(tenant.ID, "duplicate").Inc()
		httpx.JSON(w, http.StatusOK, map[string]string{"id": p.env.EventID, "status": "duplicate"})
		return
	}
	rec, err := a.record(ctx, p)
	if err == nil {
		// Acknowledge only after Kafka has the event (acks=all): at-least-once from here on.
		err = kafkax.ProduceSync(ctx, a.d.Producer, rec)
	}
	if err != nil {
		a.release([]*prepared{p})
		obs.EventsIngested.WithLabelValues(tenant.ID, "error").Inc()
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "could not persist event, retry later")
		return
	}
	a.received.Add(1)
	obs.EventsIngested.WithLabelValues(tenant.ID, "accepted").Inc()
	httpx.JSON(w, http.StatusAccepted, map[string]string{"id": p.env.EventID, "status": "accepted"})
}

func (a *API) ingestBatch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { obs.IngestLatency.Observe(time.Since(start).Seconds()) }()
	var in struct {
		Events []eventIn `json:"events"`
	}
	if err := httpx.Decode(r, 8*maxEventBytes, &in); err != nil {
		if errors.Is(err, httpx.ErrTooLarge) {
			httpx.Error(w, http.StatusRequestEntityTooLarge, "payload_too_large", "batch exceeds 8 MiB")
			return
		}
		httpx.Error(w, http.StatusBadRequest, "invalid_json", "body must be {\"events\": [...]}")
		return
	}
	if len(in.Events) == 0 || len(in.Events) > maxBatch {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", fmt.Sprintf("events must contain 1..%d items", maxBatch))
		return
	}
	for i := range in.Events {
		if err := in.Events[i].validate(); err != nil {
			httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", fmt.Sprintf("events[%d]: %v", i, err))
			return
		}
	}
	tenant, ok := a.authenticate(w, r, len(in.Events))
	if !ok {
		return
	}
	ctx := r.Context()
	ids := make([]string, len(in.Events))
	var ps []*prepared
	var recs []*kgo.Record
	for i := range in.Events {
		p, err := a.prepare(ctx, tenant, &in.Events[i])
		if err != nil {
			a.release(ps)
			internalErr(w, err)
			return
		}
		ids[i] = p.env.EventID
		if p.duplicate {
			continue
		}
		rec, err := a.record(ctx, p)
		if err != nil {
			a.release(append(ps, p))
			internalErr(w, err)
			return
		}
		ps = append(ps, p)
		recs = append(recs, rec)
	}
	if err := kafkax.ProduceSync(ctx, a.d.Producer, recs...); err != nil {
		a.release(ps)
		httpx.Error(w, http.StatusServiceUnavailable, "unavailable", "could not persist events, retry later")
		return
	}
	a.received.Add(int64(len(recs)))
	obs.EventsIngested.WithLabelValues(tenant.ID, "accepted").Add(float64(len(recs)))
	httpx.JSON(w, http.StatusAccepted, map[string]any{"ids": ids})
}
