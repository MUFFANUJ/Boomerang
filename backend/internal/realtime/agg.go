package realtime

import (
	"hash/fnv"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/boomerang-dev/boomerang/internal/model"
)

const (
	window     = 60 // seconds of history
	rateWindow = 3  // seconds averaged for the live rates
)

type bucket struct {
	sec                               int64
	delivered, failed, deferred, dead int
	lat                               []int
	latSeen                           int
}

func (b *bucket) reset(sec int64) {
	*b = bucket{sec: sec, lat: b.lat[:0]}
}

const maxLatSamples = 2000

func (b *bucket) addLatency(ms int) {
	b.latSeen++
	if len(b.lat) < maxLatSamples {
		b.lat = append(b.lat, ms)
		return
	}
	// Reservoir sampling keeps percentiles unbiased under heavy load.
	if j := rand.IntN(b.latSeen); j < maxLatSamples {
		b.lat[j] = ms
	}
}

// series is a ring of per-second buckets.
type series struct{ b [window]bucket }

func (s *series) at(sec int64) *bucket {
	bk := &s.b[sec%window]
	if bk.sec != sec {
		bk.reset(sec)
	}
	return bk
}

func (s *series) record(sec int64, a *model.AttemptRecord) {
	bk := s.at(sec)
	switch a.Outcome {
	case model.OutcomeSuccess:
		bk.delivered++
	case model.OutcomeFailure:
		bk.failed++
	case model.OutcomeDeferred:
		bk.deferred++
	case model.OutcomeDead:
		bk.dead++
		if a.Attempted {
			bk.failed++
		}
	}
	if a.Attempted {
		bk.addLatency(a.LatencyMs)
	}
}

type summary struct {
	deliveredEPS, failedEPS, deferredEPS float64
	successRate                          *float64
	p50, p95, p99                        *float64
}

func (s *series) summarize(now int64) summary {
	var out summary
	// Rates are a 3-second moving average over event time, which smooths out the batching of
	// the status stream without lagging noticeably.
	for sec := now - rateWindow; sec < now; sec++ {
		if bk := s.b[sec%window]; bk.sec == sec {
			out.deliveredEPS += float64(bk.delivered) / rateWindow
			out.failedEPS += float64(bk.failed) / rateWindow
			out.deferredEPS += float64(bk.deferred) / rateWindow
		}
	}
	var ok, bad int
	var lats []int
	for i := range s.b {
		bk := &s.b[i]
		if bk.sec <= now-window || bk.sec > now {
			continue
		}
		ok += bk.delivered
		bad += bk.failed
		lats = append(lats, bk.lat...)
	}
	if ok+bad > 0 {
		r := float64(ok) / float64(ok+bad)
		out.successRate = &r
	}
	if len(lats) > 0 {
		sort.Ints(lats)
		q := func(p float64) *float64 {
			v := float64(lats[int(p*float64(len(lats)-1))])
			return &v
		}
		out.p50, out.p95, out.p99 = q(0.50), q(0.95), q(0.99)
	}
	return out
}

// Aggregator keeps rolling delivery metrics, globally and per endpoint.
type Aggregator struct {
	mu        sync.Mutex
	global    series
	endpoints map[string]*series
}

// NewAggregator creates an aggregator.
func NewAggregator() *Aggregator { return &Aggregator{endpoints: map[string]*series{}} }

// Record adds an attempt.
func (a *Aggregator) Record(ar *model.AttemptRecord) {
	// Bucket by when the attempt happened, so batching in the status stream doesn't make the
	// rate jumpy. If we're reading the stream late (under heavy load), count it now instead,
	// otherwise the live rate would read zero while work is clearly happening.
	now := time.Now().Unix()
	sec := ar.TS.Unix()
	if sec > now || sec < now-rateWindow {
		sec = now
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.global.record(sec, ar)
	s, ok := a.endpoints[ar.EndpointID]
	if !ok {
		s = &series{}
		a.endpoints[ar.EndpointID] = s
	}
	s.record(sec, ar)
}

// Global summarizes everything.
func (a *Aggregator) Global() summary {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.global.summarize(time.Now().Unix())
}

// Endpoint summarizes one endpoint.
func (a *Aggregator) Endpoint(id string) summary {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.endpoints[id]
	if !ok {
		return summary{}
	}
	return s.summarize(time.Now().Unix())
}

// Sampler decides which events appear in the live flow. Sampling is per event: once an event
// is chosen, all of its later attempts (retries, dead-lettering) are shown too.
type Sampler struct {
	mu       sync.Mutex
	target   float64 // events per second to show
	rate     float64
	count    int
	windowAt time.Time
	chosen   map[string]time.Time
}

// NewSampler creates a sampler showing about target events/s.
func NewSampler(target float64) *Sampler {
	return &Sampler{target: target, rate: 1, windowAt: time.Now(), chosen: map[string]time.Time{}}
}

func hash01(s string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return float64(h.Sum32()%10000) / 10000
}

// Event decides whether a new event is sampled.
func (s *Sampler) Event(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.count++
	if el := now.Sub(s.windowAt); el >= time.Second {
		eps := float64(s.count) / el.Seconds()
		s.rate = 1
		if eps > s.target {
			s.rate = s.target / eps
		}
		s.count, s.windowAt = 0, now
	}
	if hash01(id) >= s.rate {
		return false
	}
	s.chosen[id] = now
	if len(s.chosen) > 50_000 {
		for k, t := range s.chosen {
			if now.Sub(t) > 10*time.Minute || len(s.chosen) > 40_000 {
				delete(s.chosen, k)
			}
		}
	}
	return true
}

// Attempt reports whether an attempt belongs to a sampled event. Attempts for events never
// seen (e.g. sent before the page loaded) are sampled at the current rate.
func (s *Sampler) Attempt(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.chosen[eventID]; ok {
		return true
	}
	return hash01(eventID) < s.rate
}

// Rate is the current sample rate.
func (s *Sampler) Rate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rate
}
