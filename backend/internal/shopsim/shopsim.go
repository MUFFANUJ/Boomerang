// Package shopsim is a simulated e-commerce store that sends realistic events to Boomerang's
// public API, exactly as a customer integration would: over HTTP, through the gateway,
// with an API key and idempotency keys.
package shopsim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/stats"
)

// Sim is the traffic generator.
type Sim struct {
	d   *app.Deps
	hc  *http.Client
	gen *generator

	mu       sync.Mutex
	mode     string
	rateEPS  float64
	types    []string
	cancel   context.CancelFunc
	burst    *burstState
	loadtest *loadtestState
	last     *lastEvent

	sent, accepted, duplicates, errors, limited atomic.Int64
	lat                                         latWindow
	epsWindow                                   epsCounter
}

type burstState struct {
	Total int   `json:"total"`
	Done  int64 `json:"done"`
}

type ltStep struct {
	TS          float64 `json:"t_s"`
	TargetEPS   float64 `json:"target_eps"`
	AchievedEPS float64 `json:"achieved_eps"`
	P95         float64 `json:"p95_ms"`
	Errors      int64   `json:"errors"`
}

type loadtestState struct {
	StartEPS  float64  `json:"start_eps"`
	EndEPS    float64  `json:"end_eps"`
	Duration  int      `json:"duration_s"`
	ElapsedS  float64  `json:"elapsed_s"`
	Steps     []ltStep `json:"steps"`
	startedAt time.Time
}

type lastEvent struct {
	ID   string    `json:"id"`
	Type string    `json:"type"`
	TS   time.Time `json:"ts"`
}

// New creates the simulator.
func New(d *app.Deps) *Sim {
	return &Sim{
		d: d, gen: newGenerator(), mode: "idle", types: slices.Clone(model.ShopSimEventTypes),
		hc: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024, IdleConnTimeout: 60 * time.Second}},
	}
}

// Mount registers the control API.
func (s *Sim) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/sim/status", func(w http.ResponseWriter, _ *http.Request) { httpx.JSON(w, http.StatusOK, s.Status()) })
	mux.HandleFunc("POST /api/sim/single", s.single)
	mux.HandleFunc("POST /api/sim/stream", s.stream)
	mux.HandleFunc("POST /api/sim/burst", s.burstH)
	mux.HandleFunc("POST /api/sim/loadtest", s.loadtestH)
	mux.HandleFunc("POST /api/sim/stop", func(w http.ResponseWriter, _ *http.Request) {
		s.stop()
		httpx.JSON(w, http.StatusOK, s.Status())
	})
}

// Run publishes status to Redis for the realtime service until ctx is done.
func (s *Sim) Run(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.stop()
			return
		case <-t.C:
			b, _ := json.Marshal(s.Status())
			s.d.Redis.Set(ctx, stats.SimStatus, b, 10*time.Second)
		}
	}
}

// Status reports the simulator state.
func (s *Sim) Status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p50, p95, p99 := s.lat.percentiles()
	var burst, lt any
	if s.burst != nil {
		burst = &burstState{Total: s.burst.Total, Done: atomic.LoadInt64(&s.burst.Done)}
	}
	if s.loadtest != nil {
		c := *s.loadtest
		c.Steps = slices.Clone(s.loadtest.Steps)
		lt = &c
	}
	return map[string]any{
		"mode": s.mode, "running": s.mode != "idle", "rate_eps": s.rateEPS, "event_types": s.types,
		"sent": s.sent.Load(), "accepted": s.accepted.Load(), "duplicates": s.duplicates.Load(),
		"errors": s.errors.Load(), "rate_limited": s.limited.Load(), "current_eps": s.epsWindow.rate(),
		"latency_ms": map[string]float64{"p50": p50, "p95": p95, "p99": p99},
		"burst":      burst, "loadtest": lt, "last_event": s.last,
	}
}

func (s *Sim) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	s.mode, s.rateEPS = "idle", 0
}

func (s *Sim) start(mode string, rate float64, types []string) context.Context {
	s.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.mode, s.rateEPS = cancel, mode, rate
	if len(types) > 0 {
		s.types = types
	}
	s.burst, s.loadtest = nil, nil
	return ctx
}

func (s *Sim) finish(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() == nil && s.cancel != nil {
		s.cancel()
		s.cancel = nil
		s.mode, s.rateEPS = "idle", 0
	}
}

func validTypes(in []string) ([]string, error) {
	if len(in) == 0 {
		return slices.Clone(model.ShopSimEventTypes), nil
	}
	for _, t := range in {
		if !slices.Contains(model.ShopSimEventTypes, t) {
			return nil, fmt.Errorf("unknown event type %q", t)
		}
	}
	return in, nil
}

// send posts one event through the gateway.
func (s *Sim) send(ctx context.Context, ev simEvent) (string, error) {
	b, _ := json.Marshal(map[string]any{"type": ev.Type, "data": ev.Data, "ordering_key": ev.OrderingKey})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.d.Cfg.GatewayURL+"/v1/events", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.d.Cfg.ShopSimAPIKey)
	req.Header.Set("Idempotency-Key", ev.IdempotencyKey)
	start := time.Now()
	s.sent.Add(1)
	resp, err := s.hc.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			s.errors.Add(1)
		}
		return "", err
	}
	defer resp.Body.Close()
	s.lat.add(float64(time.Since(start).Microseconds()) / 1000)
	var out struct {
		ID    string `json:"id"`
		Error string `json:"error"`
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = json.Unmarshal(body, &out)
	switch resp.StatusCode {
	case http.StatusAccepted:
		s.accepted.Add(1)
		s.epsWindow.add()
	case http.StatusOK:
		s.duplicates.Add(1)
	case http.StatusTooManyRequests:
		s.limited.Add(1)
		return "", errors.New("rate limited")
	default:
		s.errors.Add(1)
		return "", fmt.Errorf("ingest returned %s: %s", resp.Status, out.Error)
	}
	s.mu.Lock()
	s.last = &lastEvent{ID: out.ID, Type: ev.Type, TS: time.Now().UTC()}
	s.mu.Unlock()
	return out.ID, nil
}

// sendWithDuplicates occasionally re-sends the same event with the same idempotency key, the
// way a client retrying after a network blip would. Boomerang must not deliver it twice.
func (s *Sim) sendWithDuplicates(ctx context.Context, ev simEvent) {
	_, _ = s.send(ctx, ev)
	if rand.Float64() < 0.01 {
		_, _ = s.send(ctx, ev)
	}
}

func (s *Sim) single(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Type string `json:"type"`
	}
	_ = httpx.Decode(r, 4096, &in)
	if in.Type == "" {
		in.Type = "order.created"
	}
	if _, err := validTypes([]string{in.Type}); err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	ev := s.gen.next([]string{in.Type})
	id, err := s.send(r.Context(), ev)
	if err != nil {
		httpx.Error(w, http.StatusBadGateway, "ingest_failed", err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]string{"id": id, "type": ev.Type})
}

func (s *Sim) stream(w http.ResponseWriter, r *http.Request) {
	var in struct {
		RateEPS    float64  `json:"rate_eps"`
		EventTypes []string `json:"event_types"`
	}
	if err := httpx.Decode(r, 4096, &in); err != nil || in.RateEPS <= 0 || in.RateEPS > 20000 {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "rate_eps must be 0 < rate <= 20000")
		return
	}
	types, err := validTypes(in.EventTypes)
	if err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	ctx := s.start("stream", in.RateEPS, types)
	rate := in.RateEPS
	go s.pace(ctx, func() float64 { return rate }, types)
	httpx.JSON(w, http.StatusOK, s.Status())
}

func (s *Sim) burstH(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Count       int      `json:"count"`
		EventTypes  []string `json:"event_types"`
		Concurrency int      `json:"concurrency"`
	}
	if err := httpx.Decode(r, 4096, &in); err != nil || in.Count <= 0 || in.Count > 1_000_000 {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "count must be 1..1000000")
		return
	}
	types, err := validTypes(in.EventTypes)
	if err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	if in.Concurrency <= 0 || in.Concurrency > 512 {
		in.Concurrency = 64
	}
	ctx := s.start("burst", 0, types)
	bs := &burstState{Total: in.Count}
	s.mu.Lock()
	s.burst = bs
	s.mu.Unlock()
	go func() {
		defer s.finish(ctx)
		var next atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < in.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ctx.Err() == nil && next.Add(1) <= int64(in.Count) {
					s.sendWithDuplicates(ctx, s.gen.next(types))
					atomic.AddInt64(&bs.Done, 1)
				}
			}()
		}
		wg.Wait()
	}()
	httpx.JSON(w, http.StatusOK, s.Status())
}

func (s *Sim) loadtestH(w http.ResponseWriter, r *http.Request) {
	var in struct {
		StartEPS   float64  `json:"start_eps"`
		EndEPS     float64  `json:"end_eps"`
		DurationS  int      `json:"duration_s"`
		EventTypes []string `json:"event_types"`
	}
	if err := httpx.Decode(r, 4096, &in); err != nil || in.StartEPS <= 0 || in.EndEPS < in.StartEPS || in.EndEPS > 20000 || in.DurationS < 10 || in.DurationS > 3600 {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "need 0 < start_eps <= end_eps <= 20000 and 10 <= duration_s <= 3600")
		return
	}
	types, err := validTypes(in.EventTypes)
	if err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", err.Error())
		return
	}
	ctx := s.start("loadtest", in.StartEPS, types)
	lt := &loadtestState{StartEPS: in.StartEPS, EndEPS: in.EndEPS, Duration: in.DurationS, Steps: []ltStep{}, startedAt: time.Now()}
	s.mu.Lock()
	s.loadtest = lt
	s.mu.Unlock()
	target := func() float64 {
		f := min(1, time.Since(lt.startedAt).Seconds()/float64(in.DurationS))
		return in.StartEPS + (in.EndEPS-in.StartEPS)*f
	}
	go s.pace(ctx, target, types)
	go func() {
		defer s.finish(ctx)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		lastAcc, lastErr := s.accepted.Load(), s.errors.Load()+s.limited.Load()
		lastT := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				acc, errs := s.accepted.Load(), s.errors.Load()+s.limited.Load()
				_, p95, _ := s.lat.percentiles()
				tgt := target()
				el := now.Sub(lt.startedAt).Seconds()
				s.mu.Lock()
				lt.ElapsedS = el
				s.rateEPS = tgt
				lt.Steps = append(lt.Steps, ltStep{TS: el, TargetEPS: tgt, AchievedEPS: float64(acc-lastAcc) / now.Sub(lastT).Seconds(), P95: p95, Errors: errs - lastErr})
				s.mu.Unlock()
				lastAcc, lastErr, lastT = acc, errs, now
				if el >= float64(in.DurationS) {
					return
				}
			}
		}
	}()
	httpx.JSON(w, http.StatusOK, s.Status())
}

// pace sends events at rate() events/s using a pool of senders. If the system can't keep up,
// tokens are dropped and the achieved rate falls below target, which is what a load test
// needs to observe.
func (s *Sim) pace(ctx context.Context, rate func() float64, types []string) {
	tokens := make(chan struct{}, 4096)
	senders := 256
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					s.sendWithDuplicates(ctx, s.gen.next(types))
				}
			}
		}()
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	var acc float64
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case now := <-t.C:
			acc += rate() * now.Sub(last).Seconds()
			last = now
			for ; acc >= 1; acc-- {
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}
	}
}

// --- metrics helpers ---

type latWindow struct {
	mu      sync.Mutex
	samples []timedSample
}

type timedSample struct {
	at time.Time
	ms float64
}

func (l *latWindow) add(ms float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.samples = append(l.samples, timedSample{time.Now(), ms})
	if len(l.samples) > 20000 {
		l.samples = l.samples[len(l.samples)-10000:]
	}
}

func (l *latWindow) percentiles() (float64, float64, float64) {
	l.mu.Lock()
	cut := time.Now().Add(-5 * time.Second)
	i := sort.Search(len(l.samples), func(i int) bool { return l.samples[i].at.After(cut) })
	l.samples = l.samples[i:]
	vals := make([]float64, len(l.samples))
	for j, s := range l.samples {
		vals[j] = s.ms
	}
	l.mu.Unlock()
	if len(vals) == 0 {
		return 0, 0, 0
	}
	sort.Float64s(vals)
	q := func(p float64) float64 { return vals[int(p*float64(len(vals)-1))] }
	return q(0.5), q(0.95), q(0.99)
}

type epsCounter struct {
	mu   sync.Mutex
	secs [3]struct {
		sec int64
		n   int64
	}
}

func (e *epsCounter) add() {
	sec := time.Now().Unix()
	e.mu.Lock()
	b := &e.secs[sec%3]
	if b.sec != sec {
		b.sec, b.n = sec, 0
	}
	b.n++
	e.mu.Unlock()
}

func (e *epsCounter) rate() float64 {
	prev := time.Now().Unix() - 1
	e.mu.Lock()
	defer e.mu.Unlock()
	if b := e.secs[prev%3]; b.sec == prev {
		return float64(b.n)
	}
	return 0
}

// --- event generation ---

type simEvent struct {
	Type           string
	Data           map[string]any
	OrderingKey    string
	IdempotencyKey string
}

type order struct {
	ID       string
	Customer map[string]any
	Items    []map[string]any
	Total    int
}

type generator struct {
	mu     sync.Mutex
	orders []*order
	seq    int
}

func newGenerator() *generator { return &generator{} }

var (
	firstNames = []string{"Ava", "Liam", "Maya", "Noah", "Zara", "Ethan", "Priya", "Lucas", "Amara", "Kenji", "Sofia", "Omar"}
	lastNames  = []string{"Chen", "Patel", "Garcia", "Okafor", "Kim", "Silva", "Nguyen", "Müller", "Haddad", "Rossi"}
	products   = []struct {
		SKU, Name string
		Price     int
	}{
		{"SKU-TEE-BLK", "Organic Cotton Tee (Black)", 2800}, {"SKU-HOOD-GRY", "Heavyweight Hoodie (Grey)", 6400},
		{"SKU-CAP-NVY", "Canvas Cap (Navy)", 2200}, {"SKU-MUG-WHT", "Stoneware Mug", 1800},
		{"SKU-BAG-OLV", "Waxed Tote Bag (Olive)", 4600}, {"SKU-SOCK-3PK", "Merino Socks 3-Pack", 2400},
		{"SKU-BTL-STL", "Insulated Bottle", 3200}, {"SKU-NB-A5", "Dot Grid Notebook A5", 1400},
	}
	carriers = []string{"UPS", "FedEx", "DHL", "USPS"}
	weights  = map[string]float64{"order.created": 0.34, "payment.succeeded": 0.30, "payment.failed": 0.05, "order.shipped": 0.25, "order.refunded": 0.06}
)

func pick[T any](xs []T) T { return xs[rand.IntN(len(xs))] }

func (g *generator) newOrder() *order {
	g.seq++
	first, last := pick(firstNames), pick(lastNames)
	o := &order{ID: fmt.Sprintf("ord_%06d%s", g.seq, uuid.NewString()[:4]),
		Customer: map[string]any{"id": "cus_" + uuid.NewString()[:8], "name": first + " " + last,
			"email": fmt.Sprintf("%s.%s@example.com", lowerASCII(first), lowerASCII(last))}}
	for i := 0; i < 1+rand.IntN(3); i++ {
		p := pick(products)
		qty := 1 + rand.IntN(2)
		o.Items = append(o.Items, map[string]any{"sku": p.SKU, "name": p.Name, "quantity": qty, "unit_price": p.Price})
		o.Total += qty * p.Price
	}
	g.orders = append(g.orders, o)
	if len(g.orders) > 5000 {
		g.orders = g.orders[1000:]
	}
	return o
}

func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return string(b)
}

func (g *generator) next(types []string) simEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	var total float64
	for _, t := range types {
		total += weights[t]
	}
	r := rand.Float64() * total
	typ := types[len(types)-1]
	for _, t := range types {
		if r -= weights[t]; r < 0 {
			typ = t
			break
		}
	}
	var o *order
	if typ == "order.created" || len(g.orders) == 0 {
		o = g.newOrder()
	} else {
		o = g.orders[len(g.orders)-1-rand.IntN(min(len(g.orders), 200))]
	}
	now := time.Now().UTC()
	data := map[string]any{"order_id": o.ID, "customer": o.Customer, "currency": "USD", "amount": o.Total}
	switch typ {
	case "order.created":
		data["items"] = o.Items
		data["channel"] = pick([]string{"web", "ios", "android"})
	case "payment.succeeded":
		data["payment_id"] = "pay_" + uuid.NewString()[:12]
		data["method"] = pick([]string{"card", "apple_pay", "paypal"})
	case "payment.failed":
		data["payment_id"] = "pay_" + uuid.NewString()[:12]
		data["failure_code"] = pick([]string{"card_declined", "insufficient_funds", "expired_card"})
	case "order.shipped":
		data["carrier"] = pick(carriers)
		data["tracking_number"] = fmt.Sprintf("1Z%010d", rand.IntN(1_000_000_000))
		data["shipped_at"] = now
	case "order.refunded":
		data["refund_id"] = "re_" + uuid.NewString()[:12]
		data["reason"] = pick([]string{"requested_by_customer", "damaged", "wrong_size"})
		data["amount"] = o.Total / (1 + rand.IntN(2))
	}
	return simEvent{Type: typ, Data: data, OrderingKey: o.ID, IdempotencyKey: uuid.NewString()}
}
