package model

import (
	"testing"
	"time"
)

func TestMatchEventType(t *testing.T) {
	cases := []struct {
		pattern, typ string
		want         bool
	}{
		{"*", "order.created", true},
		{"order.*", "order.created", true},
		{"order.*", "order.shipped", true},
		{"order.*", "payment.failed", false},
		{"order.*", "orderly.thing", false},
		{"payment.failed", "payment.failed", true},
		{"payment.failed", "payment.succeeded", false},
	}
	for _, c := range cases {
		if got := MatchEventType(c.pattern, c.typ); got != c.want {
			t.Errorf("MatchEventType(%q, %q) = %v, want %v", c.pattern, c.typ, got, c.want)
		}
	}
}

func TestRetrySchedule(t *testing.T) {
	tn := Tenant{RetryScheduleMs: []int{10_000, 60_000, 300_000, 1_800_000}}
	want := map[int]time.Duration{2: 10 * time.Second, 3: time.Minute, 4: 5 * time.Minute, 5: 30 * time.Minute, 9: 30 * time.Minute}
	for next, d := range want {
		if got := tn.RetryDelay(next); got != d {
			t.Errorf("RetryDelay(%d) = %v, want %v", next, got, d)
		}
	}
}

func TestTierFor(t *testing.T) {
	cases := map[time.Duration]int{
		2 * time.Second: 1, 15 * time.Second: 1, 16 * time.Second: 2, time.Minute: 2,
		5 * time.Minute: 3, 30 * time.Minute: 4, 48 * time.Hour: 4,
	}
	for d, tier := range cases {
		if got := TierFor(d).Tier; got != tier {
			t.Errorf("TierFor(%v) = %d, want %d", d, got, tier)
		}
	}
}

func TestDeliveryIDIsDeterministic(t *testing.T) {
	a, b := DeliveryID("evt_1", "ep_1"), DeliveryID("evt_1", "ep_1")
	if a != b {
		t.Fatal("delivery id must be stable so re-running fan-out dedupes")
	}
	if a == DeliveryID("evt_1", "ep_2") {
		t.Fatal("different endpoints must get different delivery ids")
	}
}

func TestSecretsDuringRotation(t *testing.T) {
	now := time.Now()
	future, past := now.Add(time.Minute), now.Add(-time.Minute)
	e := Endpoint{Secret: "new", PreviousSecret: "old", PreviousExpiresAt: &future}
	if s := e.Secrets(now); len(s) != 2 || s[0] != "new" {
		t.Fatalf("expected [new old], got %v", s)
	}
	e.PreviousExpiresAt = &past
	if s := e.Secrets(now); len(s) != 1 {
		t.Fatalf("expired previous secret must not be used, got %v", s)
	}
}
