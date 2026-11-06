// Package realtime streams live system state to dashboards over WebSockets: delivery
// metrics (from the status stream), sampled event flow, infrastructure state (workers,
// consumer lag, retry tiers, component health) and delivery integrity.
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/circuit"
	"github.com/boomerang-dev/boomerang/internal/health"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/obs"
	"github.com/boomerang-dev/boomerang/internal/stats"
)

// Service is the realtime fan-out to dashboards.
type Service struct {
	d       *app.Deps
	agg     *Aggregator
	sampler *Sampler
	hub     *Hub
	kcl     *kgo.Client
	adm     *kadm.Client

	mu            sync.RWMutex
	endpoints     []*model.Endpoint
	flowBuf       []any
	lastStats     []byte
	lastInfra     []byte
	lastIntegrity []byte
	statsData     any
	infraData     *InfraSnapshot
	integrityData any
	retrying      int64
	dlqDepth      int64
	lastReceived  int64
}

// New creates the service.
func New(d *app.Deps) (*Service, error) {
	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(d.Cfg.KafkaBrokers...),
		kgo.ClientID("realtime-"+d.Cfg.ServiceID),
		kgo.ConsumeTopics(model.TopicStatus),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, err
	}
	return &Service{
		d: d, agg: NewAggregator(), sampler: NewSampler(80), hub: NewHub(), kcl: kcl, adm: kadm.NewClient(kcl),
		lastReceived: -1,
	}, nil
}

// Mount registers HTTP routes.
func (s *Service) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /ws", s.serveWS)
	mux.HandleFunc("GET /api/overview", func(w http.ResponseWriter, _ *http.Request) { s.serveLast(w, func() any { return s.statsData }) })
	mux.HandleFunc("GET /api/integrity", func(w http.ResponseWriter, _ *http.Request) {
		s.serveLast(w, func() any { return s.integrityData })
	})
	mux.HandleFunc("GET /api/infra", func(w http.ResponseWriter, _ *http.Request) { s.serveLast(w, func() any { return s.infraData }) })
}

func (s *Service) serveLast(w http.ResponseWriter, get func() any) {
	s.mu.RLock()
	v := get()
	s.mu.RUnlock()
	if v == nil {
		httpx.Error(w, http.StatusServiceUnavailable, "warming_up", "no snapshot yet, try again in a second")
		return
	}
	httpx.JSON(w, http.StatusOK, v)
}

// Run consumes the status stream and drives the periodic snapshots until ctx is done.
func (s *Service) Run(ctx context.Context) error {
	defer s.kcl.Close()
	s.refreshEndpoints(ctx)
	go s.listenEndpointChanges(ctx)
	go s.every(ctx, 100*time.Millisecond, s.flushFlow)
	go s.every(ctx, time.Second, s.tickStats)
	go s.every(ctx, 2*time.Second, s.tickInfra)
	go s.every(ctx, 5*time.Second, s.tickIntegrity)
	go s.every(ctx, 10*time.Second, s.refreshEndpoints)
	for {
		fetches := s.kcl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) { slog.Warn("status fetch error", "err", err) })
		fetches.EachRecord(s.handle)
	}
}

func (s *Service) every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

// --- status stream ---

type flowEvent struct {
	Kind       string              `json:"kind"`
	EventID    string              `json:"event_id"`
	EventType  string              `json:"event_type"`
	TenantID   string              `json:"tenant_id"`
	TS         time.Time           `json:"ts"`
	Deliveries []model.DeliveryRef `json:"deliveries"`
}

type flowAttempt struct {
	Kind         string     `json:"kind"`
	EventID      string     `json:"event_id"`
	DeliveryID   string     `json:"delivery_id"`
	EndpointID   string     `json:"endpoint_id"`
	EndpointName string     `json:"endpoint_name"`
	Attempt      int        `json:"attempt"`
	Outcome      string     `json:"outcome"`
	StatusCode   int        `json:"status_code"`
	LatencyMs    int        `json:"latency_ms"`
	NextRetryAt  *time.Time `json:"next_retry_at"`
	Reason       string     `json:"reason"`
	TS           time.Time  `json:"ts"`
}

func (s *Service) handle(r *kgo.Record) {
	var m model.StatusMsg
	if err := json.Unmarshal(r.Value, &m); err != nil {
		return
	}
	switch {
	case m.Kind == "event" && m.Event != nil:
		if s.sampler.Event(m.Event.EventID) {
			s.pushFlow(flowEvent{Kind: "event", EventID: m.Event.EventID, EventType: m.Event.EventType,
				TenantID: m.Event.TenantID, TS: m.Event.CreatedAt, Deliveries: m.Event.Deliveries})
		}
	case m.Kind == "attempt" && m.Attempt != nil:
		a := m.Attempt
		s.agg.Record(a)
		if s.sampler.Attempt(a.EventID) {
			s.pushFlow(flowAttempt{Kind: "attempt", EventID: a.EventID, DeliveryID: a.DeliveryID, EndpointID: a.EndpointID,
				EndpointName: a.EndpointName, Attempt: a.Attempt, Outcome: a.Outcome, StatusCode: a.StatusCode,
				LatencyMs: a.LatencyMs, NextRetryAt: a.NextRetryAt, Reason: a.Reason, TS: a.TS})
		}
	}
}

func (s *Service) pushFlow(item any) {
	s.mu.Lock()
	if len(s.flowBuf) < 2000 {
		s.flowBuf = append(s.flowBuf, item)
	}
	s.mu.Unlock()
}

func (s *Service) flushFlow(context.Context) {
	s.mu.Lock()
	items := s.flowBuf
	s.flowBuf = nil
	s.mu.Unlock()
	if len(items) == 0 {
		return
	}
	s.hub.Broadcast(mustJSON(map[string]any{"type": "flow", "data": map[string]any{"items": items, "sample_rate": s.sampler.Rate()}}), false)
}

// --- endpoints ---

func (s *Service) refreshEndpoints(ctx context.Context) {
	eps, err := s.d.Store.ListAllEndpoints(ctx)
	if err != nil {
		slog.Warn("endpoint refresh failed", "err", err)
		return
	}
	s.mu.Lock()
	s.endpoints = eps
	s.mu.Unlock()
}

func (s *Service) listenEndpointChanges(ctx context.Context) {
	sub := s.d.Redis.Subscribe(ctx, stats.EndpointChanged)
	defer sub.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-sub.Channel():
			if !ok {
				return
			}
			s.refreshEndpoints(ctx)
			s.hub.Broadcast(mustJSON(map[string]any{"type": "endpoint_change", "data": map[string]string{"endpoint_id": m.Payload}}), true)
		}
	}
}

// --- stats ---

type latency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type endpointStat struct {
	ID                    string   `json:"id"`
	Name                  string   `json:"name"`
	TenantID              string   `json:"tenant_id"`
	Circuit               string   `json:"circuit"`
	Status                string   `json:"status"`
	SuccessRate           *float64 `json:"success_rate"`
	DeliveredEPS          float64  `json:"delivered_eps"`
	FailedEPS             float64  `json:"failed_eps"`
	P95                   *float64 `json:"p95_ms"`
	RateLimitEffectiveRPS *float64 `json:"rate_limit_effective_rps"`
}

func (s *Service) tickStats(ctx context.Context) {
	vals, err := s.d.Redis.MGet(ctx, stats.Received, stats.Delivered, stats.FailedAttempts, stats.SimStatus).Result()
	if err != nil {
		slog.Warn("stats read failed", "err", err)
		vals = make([]any, 4)
	}
	num := func(v any) int64 {
		if str, ok := v.(string); ok {
			n, _ := strconv.ParseInt(str, 10, 64)
			return n
		}
		return 0
	}
	received := num(vals[0])
	s.mu.Lock()
	receivedEPS := 0.0
	if s.lastReceived >= 0 && received >= s.lastReceived {
		receivedEPS = float64(received - s.lastReceived)
	}
	s.lastReceived = received
	retrying, dlq := s.retrying, s.dlqDepth
	eps := s.endpoints
	s.mu.Unlock()
	g := s.agg.Global()

	var lat *latency
	if g.p50 != nil {
		lat = &latency{*g.p50, *g.p95, *g.p99}
	}
	epStats := make([]endpointStat, 0, len(eps))
	for _, e := range eps {
		cb := circuit.Read(ctx, s.d.Redis, e.ID, s.d.Cfg.CircuitCooldown)
		es := s.agg.Endpoint(e.ID)
		status, _ := health.Classify(e.Paused, cb.State, es.successRate)
		var rate *float64
		if e.RateLimitRPS > 0 {
			rate = cb.EffectiveRate
			if rate == nil {
				v := float64(e.RateLimitRPS)
				rate = &v
			}
		}
		epStats = append(epStats, endpointStat{ID: e.ID, Name: e.Name, TenantID: e.TenantID, Circuit: cb.State, Status: status,
			SuccessRate: es.successRate, DeliveredEPS: es.deliveredEPS, FailedEPS: es.failedEPS, P95: es.p95, RateLimitEffectiveRPS: rate})
		v := 0.0
		if cb.State != circuit.Closed {
			v = 1
		}
		obs.CircuitState.WithLabelValues(e.Name).Set(v)
	}
	var sim json.RawMessage
	if str, ok := vals[3].(string); ok && str != "" {
		sim = json.RawMessage(str)
	}
	data := map[string]any{
		"ts": time.Now().UTC(),
		"totals": map[string]int64{
			"received": received, "delivered": num(vals[1]), "failed_attempts": num(vals[2]),
			"retrying": retrying, "dlq": dlq,
		},
		"rates": map[string]float64{
			"received_eps": receivedEPS, "delivered_eps": g.deliveredEPS, "failed_eps": g.failedEPS, "deferred_eps": g.deferredEPS,
		},
		"success_rate": g.successRate,
		"latency_ms":   lat,
		"endpoints":    epStats,
		"sim":          sim,
	}
	msg := mustJSON(map[string]any{"type": "stats", "data": data})
	s.mu.Lock()
	s.statsData, s.lastStats = data, msg
	s.mu.Unlock()
	s.hub.Broadcast(msg, true)
}

// --- infrastructure ---

// InfraSnapshot is the infra message payload.
type InfraSnapshot struct {
	TS             time.Time       `json:"ts"`
	Workers        []workerView    `json:"workers"`
	ConsumerGroups []groupView     `json:"consumer_groups"`
	RetryTiers     []tierView      `json:"retry_tiers"`
	DLQ            dlqView         `json:"dlq"`
	Components     []componentView `json:"components"`
	Scaler         scalerView      `json:"scaler"`
}

type workerView struct {
	ID         string    `json:"id"`
	Host       string    `json:"host"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	LastSeen   time.Time `json:"last_seen"`
	Partitions []int32   `json:"partitions"`
	Processed  int64     `json:"processed_total"`
	Inflight   int64     `json:"inflight"`
	EPS        float64   `json:"eps"`
}

type partitionView struct {
	Partition int32   `json:"partition"`
	Lag       int64   `json:"lag"`
	Committed int64   `json:"committed"`
	End       int64   `json:"end"`
	Owner     *string `json:"owner"`
}

type groupView struct {
	Group      string          `json:"group"`
	Topic      string          `json:"topic"`
	TotalLag   int64           `json:"total_lag"`
	Members    int             `json:"members"`
	Partitions []partitionView `json:"partitions"`
}

type tierView struct {
	Tier        int    `json:"tier"`
	Topic       string `json:"topic"`
	DelayHintMs int64  `json:"delay_hint_ms"`
	Depth       int64  `json:"depth"`
}

type dlqView struct {
	Depth      int64 `json:"depth"`
	TopicTotal int64 `json:"topic_total"`
}

type componentView struct {
	Name      string   `json:"name"`
	Status    string   `json:"status"`
	LatencyMs *float64 `json:"latency_ms"`
	Detail    string   `json:"detail"`
}

type scalerView struct {
	Enabled         bool   `json:"enabled"`
	Kind            string `json:"kind"`
	DesiredReplicas *int   `json:"desired_replicas"`
}

func (s *Service) tickInfra(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1800*time.Millisecond)
	defer cancel()
	snap := &InfraSnapshot{TS: time.Now().UTC(), Workers: s.workers(ctx)}

	type gt struct{ group, topic string }
	groups := []gt{{model.GroupDelivery, model.TopicDeliveries}, {model.GroupFanout, model.TopicEvents}, {model.GroupRecorder, model.TopicStatus}}
	for _, t := range model.RetryTiers {
		groups = append(groups, gt{fmt.Sprintf(model.GroupRetryFmt, t.Tier), t.Topic})
	}
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.group)
	}
	lags, err := s.adm.Lag(ctx, names...)
	if err != nil {
		slog.Warn("lag query failed", "err", err)
	}
	// Under heavy load a lag query can time out; keep the last good numbers for any group that
	// came back empty rather than flashing zeros on the dashboard.
	s.mu.RLock()
	prev := map[string]groupView{}
	if s.infraData != nil {
		for _, g := range s.infraData.ConsumerGroups {
			prev[g.Group] = g
		}
	}
	s.mu.RUnlock()
	tierDepth := map[string]int64{}
	for _, g := range groups {
		gv := groupView{Group: g.group, Topic: g.topic, Partitions: []partitionView{}}
		if l, ok := lags[g.group]; ok {
			gv.Members = len(l.Members)
			for _, ml := range l.Lag.Sorted() {
				if ml.Topic != g.topic {
					continue
				}
				pv := partitionView{Partition: ml.Partition, Lag: max(ml.Lag, 0), Committed: ml.Commit.At, End: ml.End.Offset}
				if ml.Member != nil {
					owner := strings.TrimPrefix(ml.Member.ClientID, "worker-")
					pv.Owner = &owner
				}
				gv.TotalLag += pv.Lag
				gv.Partitions = append(gv.Partitions, pv)
				obs.ConsumerLag.WithLabelValues(g.group, g.topic, strconv.Itoa(int(ml.Partition))).Set(float64(pv.Lag))
			}
		}
		if l, ok := lags[g.group]; (!ok || l.Error() != nil) && len(gv.Partitions) == 0 {
			if p, had := prev[g.group]; had {
				gv = p
			}
		}
		tierDepth[g.topic] = gv.TotalLag
		snap.ConsumerGroups = append(snap.ConsumerGroups, gv)
	}
	var retrying int64
	for _, t := range model.RetryTiers {
		hint := t.MaxDelay.Milliseconds()
		if t.Tier == len(model.RetryTiers) {
			hint = 30 * 60 * 1000
		}
		snap.RetryTiers = append(snap.RetryTiers, tierView{Tier: t.Tier, Topic: t.Topic, DelayHintMs: hint, Depth: tierDepth[t.Topic]})
		retrying += tierDepth[t.Topic]
	}

	if n, err := s.d.Redis.SCard(ctx, stats.DLQSet).Result(); err == nil {
		snap.DLQ.Depth = n
		obs.DLQDepth.Set(float64(n))
	}
	if ends, err := s.adm.ListEndOffsets(ctx, model.TopicDLQ); err == nil {
		ends.Each(func(o kadm.ListedOffset) {
			if o.Err == nil && o.Offset > 0 {
				snap.DLQ.TopicTotal += o.Offset
			}
		})
	}
	snap.Components = s.components(ctx)
	snap.Scaler = s.scaler(ctx)

	msg := mustJSON(map[string]any{"type": "infra", "data": snap})
	s.mu.Lock()
	s.infraData, s.lastInfra = snap, msg
	s.retrying, s.dlqDepth = retrying, snap.DLQ.Depth
	s.mu.Unlock()
	s.hub.Broadcast(msg, true)
}

func (s *Service) workers(ctx context.Context) []workerView {
	now := time.Now()
	rdb := s.d.Redis
	rdb.ZRemRangeByScore(ctx, stats.WorkersSeen, "-inf", strconv.FormatInt(now.Add(-60*time.Second).UnixMilli(), 10))
	zs, err := rdb.ZRangeWithScores(ctx, stats.WorkersSeen, 0, -1).Result()
	out := []workerView{}
	if err != nil || len(zs) == 0 {
		return out
	}
	keys := make([]string, len(zs))
	for i, z := range zs {
		keys[i] = stats.WorkerInfoPrefix + z.Member.(string)
	}
	vals, _ := rdb.MGet(ctx, keys...).Result()
	for i, z := range zs {
		id := z.Member.(string)
		seen := time.UnixMilli(int64(z.Score)).UTC()
		wv := workerView{ID: id, Host: id, Status: "stale", LastSeen: seen, Partitions: []int32{}}
		if i < len(vals) {
			if str, ok := vals[i].(string); ok {
				var hb stats.WorkerHeartbeat
				if json.Unmarshal([]byte(str), &hb) == nil {
					wv = workerView{ID: hb.ID, Host: hb.Host, Status: hb.Status, StartedAt: hb.StartedAt, LastSeen: hb.LastSeen,
						Partitions: hb.Partitions, Processed: hb.Processed, Inflight: hb.Inflight, EPS: hb.EPS}
				}
			}
		}
		if now.Sub(wv.LastSeen) > 5*time.Second {
			wv.Status, wv.EPS, wv.Inflight, wv.Partitions = "stale", 0, 0, []int32{}
		}
		sort.Slice(wv.Partitions, func(a, b int) bool { return wv.Partitions[a] < wv.Partitions[b] })
		out = append(out, wv)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].StartedAt.Before(out[b].StartedAt) })
	return out
}

func (s *Service) components(ctx context.Context) []componentView {
	check := func(name string, fn func(context.Context) error) componentView {
		start := time.Now()
		cctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		err := fn(cctx)
		ms := float64(time.Since(start).Microseconds()) / 1000
		cv := componentView{Name: name, Status: "up", LatencyMs: &ms, Detail: "ok"}
		if err != nil {
			cv.Status, cv.LatencyMs, cv.Detail = "down", nil, err.Error()
		} else if ms > 250 {
			cv.Status, cv.Detail = "degraded", "slow response"
		}
		return cv
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	out := make([]componentView, 5)
	checks := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"kafka", func(c context.Context) error { return s.kcl.Ping(c) }},
		{"redis", func(c context.Context) error { return s.d.Redis.Ping(c).Err() }},
		{"postgres", s.d.Store.Ping},
		{"clickhouse", s.d.CH.Ping},
		{"s3", s.d.Blob.Ping},
	}
	for i, c := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cv := check(c.name, c.fn)
			mu.Lock()
			out[i] = cv
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

func (s *Service) scaler(ctx context.Context) scalerView {
	sv := scalerView{Kind: "none"}
	if b, err := s.d.Redis.Get(ctx, "scaler:info").Bytes(); err == nil {
		_ = json.Unmarshal(b, &sv)
	}
	if n, err := s.d.Redis.Get(ctx, stats.ScalerDesired).Int(); err == nil {
		sv.DesiredReplicas = &n
	}
	return sv
}

// --- integrity ---

func (s *Service) tickIntegrity(ctx context.Context) {
	since := time.Unix(0, 0)
	if ms, err := s.d.Redis.Get(ctx, stats.IntegritySince).Int64(); err == nil {
		since = time.UnixMilli(ms)
	}
	s.mu.RLock()
	var receiverEPs []string
	for _, e := range s.endpoints {
		if strings.HasPrefix(e.URL, s.d.Cfg.ReceiversURL+"/") {
			receiverEPs = append(receiverEPs, e.ID)
		}
	}
	s.mu.RUnlock()
	in, err := s.d.CH.IntegritySince(ctx, since, receiverEPs)
	if err != nil {
		slog.Warn("integrity query failed", "err", err)
		return
	}
	var unique, dups int64
	for _, name := range []string{"inventory", "email", "analytics"} {
		n, _ := s.d.Redis.SCard(ctx, stats.ReceiverPrefix+name+":ids").Result()
		d, err := s.d.Redis.HGet(ctx, stats.ReceiverPrefix+name+":stats", "duplicates").Int64()
		if err != nil && err != redis.Nil {
			slog.Warn("receiver stats read failed", "err", err)
		}
		unique += n
		dups += d
	}
	inProgress := max(0, in.Created-in.Delivered-in.Dead)
	data := map[string]any{
		"ts": time.Now().UTC(), "since": since.UTC(),
		"deliveries_created": in.Created, "delivered": in.Delivered, "dead": in.Dead, "in_progress": inProgress,
		"receivers_unique": unique, "receivers_duplicates": dups, "lost": max(0, in.Delivered-unique),
	}
	msg := mustJSON(map[string]any{"type": "integrity", "data": data})
	s.mu.Lock()
	s.integrityData, s.lastIntegrity = data, msg
	s.mu.Unlock()
	s.hub.Broadcast(msg, true)
}

// --- websocket ---

func (s *Service) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, CompressionMode: websocket.CompressionContextTakeover})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	c := s.hub.Add()
	defer s.hub.Remove(c)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	// Read loop: discard client messages (pings), detect disconnects.
	go func() {
		defer cancel()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()
	s.mu.RLock()
	initial := [][]byte{s.lastStats, s.lastInfra, s.lastIntegrity}
	s.mu.RUnlock()
	for _, m := range initial {
		if m != nil {
			if err := write(ctx, conn, m); err != nil {
				return
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			conn.Close(websocket.StatusNormalClosure, "")
			return
		case m := <-c.ch:
			if err := write(ctx, conn, m); err != nil {
				return
			}
		}
	}
}

func write(ctx context.Context, c *websocket.Conn, b []byte) error {
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.Write(wctx, websocket.MessageText, b)
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Hub tracks connected dashboards.
type Hub struct {
	mu      sync.Mutex
	clients map[*client]struct{}
}

type client struct{ ch chan []byte }

// NewHub creates a hub.
func NewHub() *Hub { return &Hub{clients: map[*client]struct{}{}} }

// Add registers a client.
func (h *Hub) Add() *client {
	c := &client{ch: make(chan []byte, 256)}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	return c
}

// Remove unregisters a client.
func (h *Hub) Remove(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

// Broadcast sends to every client. Slow clients drop flow messages rather than stall the
// server; snapshot messages (important=true) are always queued if there is room.
func (h *Hub) Broadcast(b []byte, important bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		if !important && len(c.ch) > cap(c.ch)/2 {
			continue
		}
		select {
		case c.ch <- b:
		default:
		}
	}
}
