package circuit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setup(t *testing.T, cfg Config) (*Guard, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb, cfg), mr, rdb
}

func TestCircuitOpensAfterThresholdAndRecovers(t *testing.T) {
	ctx := context.Background()
	g, _, rdb := setup(t, Config{FailureThreshold: 3, Cooldown: 200 * time.Millisecond, ProbeLease: time.Second})
	ep := "ep_1"

	for i := 0; i < 2; i++ {
		if d, _ := g.Before(ctx, ep, 0); d.Decision != Allow {
			t.Fatalf("attempt %d should be allowed, got %s", i, d.Decision)
		}
		if o, _ := g.After(ctx, ep, false, false, 0); o.State != Closed {
			t.Fatalf("circuit should stay closed below threshold, got %s", o.State)
		}
	}
	g.Before(ctx, ep, 0)
	o, err := g.After(ctx, ep, false, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o.State != Open || !o.Changed {
		t.Fatalf("third failure should open the circuit, got %+v", o)
	}
	if d, _ := g.Before(ctx, ep, 0); d.Decision != DenyOpen {
		t.Fatalf("open circuit must deny, got %s", d.Decision)
	}

	time.Sleep(250 * time.Millisecond)
	g.mu.Lock()
	g.local = map[string]localState{} // drop the local short-circuit cache
	g.mu.Unlock()

	// After the cooldown exactly one probe is let through.
	if d, _ := g.Before(ctx, ep, 0); d.Decision != Allow || d.State != HalfOpen {
		t.Fatalf("first call after cooldown should be the half-open probe, got %+v", d)
	}
	if d, _ := g.Before(ctx, ep, 0); d.Decision != DenyProbe {
		t.Fatalf("second concurrent call must wait for the probe, got %s", d.Decision)
	}
	o, _ = g.After(ctx, ep, true, false, 0)
	if o.State != Closed || !o.Changed {
		t.Fatalf("successful probe should close the circuit, got %+v", o)
	}
	if s := Read(ctx, rdb, ep, time.Second); s.State != Closed || s.ConsecutiveFailures != 0 {
		t.Fatalf("unexpected snapshot %+v", s)
	}
}

func TestFailedProbeReopens(t *testing.T) {
	ctx := context.Background()
	g, _, _ := setup(t, Config{FailureThreshold: 1, Cooldown: 50 * time.Millisecond})
	g.Before(ctx, "ep", 0)
	g.After(ctx, "ep", false, false, 0)
	time.Sleep(70 * time.Millisecond)
	g.mu.Lock()
	g.local = map[string]localState{}
	g.mu.Unlock()
	if d, _ := g.Before(ctx, "ep", 0); d.State != HalfOpen {
		t.Fatalf("expected half-open probe, got %+v", d)
	}
	if o, _ := g.After(ctx, "ep", false, false, 0); o.State != Open {
		t.Fatalf("failed probe should reopen, got %+v", o)
	}
}

func TestAdaptiveRateLimit(t *testing.T) {
	ctx := context.Background()
	g, _, rdb := setup(t, Config{FailureThreshold: 100, Cooldown: time.Second, MinRate: 2, MaxRateWait: 2 * time.Second})
	ep := "ep_rl"
	// Burst equals the max rate: 10 immediate deliveries, then reservations up to MaxRateWait
	// (2s at 10/s = 20 more, each told how long to wait), then denial.
	for i := 0; i < 10; i++ {
		if d, _ := g.Before(ctx, ep, 10); d.Decision != Allow || d.Wait != 0 {
			t.Fatalf("delivery %d should pass immediately, got %+v", i, d)
		}
	}
	var last Decision
	for i := 0; i < 20; i++ {
		d, _ := g.Before(ctx, ep, 10)
		if d.Decision != Allow || d.Wait <= last.Wait {
			t.Fatalf("reservation %d should be allowed with a growing wait, got %+v (prev %v)", i, d, last.Wait)
		}
		last = d
	}
	if last.Wait > 2100*time.Millisecond {
		t.Fatalf("reservations must not exceed MaxRateWait, got %v", last.Wait)
	}
	if d, _ := g.Before(ctx, ep, 10); d.Decision != DenyLimited || d.Wait <= 0 {
		t.Fatalf("beyond MaxRateWait the delivery is deferred, got %+v", d)
	}
	// An overload signal (timeout / 429 / 503) halves the endpoint's rate.
	g.After(ctx, ep, false, true, 10)
	if r, _ := rdb.HGet(ctx, AIMDKey(ep), "rate").Float64(); r != 5 {
		t.Fatalf("rate should halve to 5, got %v", r)
	}
	// A second overload within the same second does not halve again.
	g.After(ctx, ep, false, true, 10)
	if r, _ := rdb.HGet(ctx, AIMDKey(ep), "rate").Float64(); r != 5 {
		t.Fatalf("rate should stay 5 within a second, got %v", r)
	}
	// Successes creep it back up.
	for i := 0; i < 10; i++ {
		g.After(ctx, ep, true, false, 10)
	}
	if r, _ := rdb.HGet(ctx, AIMDKey(ep), "rate").Float64(); r != 10 {
		t.Fatalf("rate should recover to the max, got %v", r)
	}
}

func TestFlakyEndpointDoesNotTrip(t *testing.T) {
	ctx := context.Background()
	g, _, _ := setup(t, Config{FailureThreshold: 5, Cooldown: time.Second})
	// 30% failures, including a run of 6 consecutive ones, at volume: the ratio stays
	// below 50% so the breaker must stay closed.
	pattern := []bool{true, true, true, false, true, true, true, true, false, true}
	for i := 0; i < 5; i++ {
		for _, ok := range pattern {
			g.After(ctx, "ep_flaky", ok, false, 0)
		}
	}
	for i := 0; i < 6; i++ {
		if o, _ := g.After(ctx, "ep_flaky", false, false, 0); o.State != Closed {
			t.Fatalf("flaky endpoint tripped after %d consecutive failures: %+v", i+1, o)
		}
	}
}

func TestDeadEndpointTripsQuicklyAtVolume(t *testing.T) {
	ctx := context.Background()
	g, _, _ := setup(t, Config{FailureThreshold: 100, Cooldown: time.Second})
	var o Outcome
	for i := 0; i < 10; i++ {
		o, _ = g.After(ctx, "ep_dead", false, false, 0)
	}
	if o.State != Open {
		t.Fatalf("10 straight failures should trip on volume even with a high consecutive threshold, got %+v", o)
	}
}
