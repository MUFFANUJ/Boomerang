// Package receivers runs the demo's mock webhook receivers (Inventory, Email, Analytics).
// Each one verifies every signature exactly as a real integration would, dedupes on the
// delivery id, and misbehaves on demand according to its chaos settings in Redis.
package receivers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/signing"
	"github.com/boomerang-dev/boomerang/internal/stats"
)

var names = map[string]bool{"inventory": true, "email": true, "analytics": true}

type chaos struct {
	failureRate float64
	delay       time.Duration
	down        bool
	at          time.Time
}

type secrets struct {
	list []string
	at   time.Time
}

// Service hosts the receivers.
type Service struct {
	d  *app.Deps
	hc *http.Client

	mu      sync.Mutex
	chaos   map[string]chaos
	secrets map[string]secrets
}

// New creates the receivers service.
func New(d *app.Deps) *Service {
	return &Service{d: d, hc: &http.Client{Timeout: 5 * time.Second}, chaos: map[string]chaos{}, secrets: map[string]secrets{}}
}

// Mount registers routes.
func (s *Service) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /{name}", s.receive)
}

func (s *Service) chaosFor(ctx context.Context, name string) chaos {
	s.mu.Lock()
	c, ok := s.chaos[name]
	s.mu.Unlock()
	if ok && time.Since(c.at) < 300*time.Millisecond {
		return c
	}
	m, err := s.d.Redis.HGetAll(ctx, stats.ChaosPrefix+name).Result()
	if err != nil {
		return c
	}
	c = chaos{at: time.Now()}
	c.failureRate, _ = strconv.ParseFloat(m["failure_rate"], 64)
	ms, _ := strconv.Atoi(m["delay_ms"])
	c.delay = time.Duration(ms) * time.Millisecond
	c.down = m["down"] == "1"
	s.mu.Lock()
	s.chaos[name] = c
	s.mu.Unlock()
	return c
}

func (s *Service) secretsFor(ctx context.Context, name string, refresh bool) []string {
	s.mu.Lock()
	sec, ok := s.secrets[name]
	s.mu.Unlock()
	if ok && !refresh && time.Since(sec.at) < 30*time.Second {
		return sec.list
	}
	if ok && refresh && time.Since(sec.at) < time.Second {
		return sec.list // don't hammer the API on a flood of bad signatures
	}
	u := s.d.Cfg.APIInternalURL + "/internal/secrets?url=" + url.QueryEscape(s.d.Cfg.ReceiversURL+"/"+name)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	req.Header.Set("X-Internal-Token", s.d.Cfg.InternalToken)
	resp, err := s.hc.Do(req)
	if err != nil {
		slog.Warn("secret fetch failed", "receiver", name, "err", err)
		return sec.list
	}
	defer resp.Body.Close()
	var out struct {
		Secrets []string `json:"secrets"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil {
		slog.Warn("secret fetch failed", "receiver", name, "status", resp.Status)
		return sec.list
	}
	s.mu.Lock()
	s.secrets[name] = secrets{list: out.Secrets, at: time.Now()}
	s.mu.Unlock()
	return out.Secrets
}

func (s *Service) bump(ctx context.Context, name, field string) {
	s.d.Redis.HIncrBy(ctx, stats.ReceiverPrefix+name+":stats", field, 1)
}

func (s *Service) receive(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !names[name] {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()
	c := s.chaosFor(ctx, name)
	if c.down {
		s.bump(ctx, name, "injected_failures")
		httpx.Error(w, http.StatusServiceUnavailable, "down", name+" service is down (chaos)")
		return
	}
	// Simulated processing time, plus any injected delay.
	wait := time.Duration(3+rand.IntN(15))*time.Millisecond + c.delay
	select {
	case <-ctx.Done():
		return
	case <-time.After(wait):
	}
	if c.failureRate > 0 && rand.Float64() < c.failureRate {
		s.bump(ctx, name, "injected_failures")
		httpx.Error(w, http.StatusInternalServerError, "injected_failure", "random failure injected by chaos settings")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "bad_body", err.Error())
		return
	}
	id := r.Header.Get(signing.HeaderID)
	ts := r.Header.Get(signing.HeaderTimestamp)
	sig := r.Header.Get(signing.HeaderSignature)
	now := time.Now()
	err = signing.Verify(s.secretsFor(ctx, name, false), id, ts, sig, body, now, signing.DefaultTolerance)
	if err == signing.ErrNoMatch {
		// The secret may have just been rotated: refresh once and re-check.
		err = signing.Verify(s.secretsFor(ctx, name, true), id, ts, sig, body, now, signing.DefaultTolerance)
	}
	if err != nil {
		s.bump(ctx, name, "invalid_signatures")
		httpx.Error(w, http.StatusUnauthorized, "invalid_signature", err.Error())
		return
	}
	pipe := s.d.Redis.Pipeline()
	added := pipe.SAdd(ctx, stats.ReceiverPrefix+name+":ids", id)
	pipe.HIncrBy(ctx, stats.ReceiverPrefix+name+":stats", "received", 1)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		httpx.Error(w, http.StatusServiceUnavailable, "storage", "could not record delivery")
		return
	}
	dup := added.Val() == 0
	if dup {
		s.bump(ctx, name, "duplicates")
	}
	httpx.JSON(w, http.StatusOK, map[string]any{
		"received": true, "receiver": name, "signature": "valid", "duplicate": dup,
		"message": fmt.Sprintf("%s processed %s", name, r.Header.Get(signing.HeaderEventType)),
	})
}
