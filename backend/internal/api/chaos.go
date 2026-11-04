package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/stats"
)

// Receivers are the demo's mock receiver services.
var Receivers = []struct{ Name, Display string }{
	{"inventory", "Inventory Service"},
	{"email", "Email Service"},
	{"analytics", "Analytics Service"},
}

// ReceiverChaos is the failure injection config for one receiver.
type ReceiverChaos struct {
	FailureRate float64 `json:"failure_rate"`
	DelayMs     int     `json:"delay_ms"`
	Down        bool    `json:"down"`
}

type receiverStats struct {
	Received          int64 `json:"received"`
	Unique            int64 `json:"unique"`
	Duplicates        int64 `json:"duplicates"`
	InvalidSignatures int64 `json:"invalid_signatures"`
	InjectedFailures  int64 `json:"injected_failures"`
}

type receiverView struct {
	Name        string        `json:"name"`
	DisplayName string        `json:"display_name"`
	URL         string        `json:"url"`
	Chaos       ReceiverChaos `json:"chaos"`
	Mode        string        `json:"mode"`
	Stats       receiverStats `json:"stats"`
}

type preset struct {
	ID          string                   `json:"id"`
	Name        string                   `json:"name"`
	Description string                   `json:"description"`
	Config      map[string]ReceiverChaos `json:"-"`
}

var presets = []preset{
	{"all-healthy", "All healthy", "Every receiver responds quickly and successfully.", map[string]ReceiverChaos{
		"inventory": {}, "email": {}, "analytics": {}}},
	{"email-outage", "Email service outage", "The Email Service is completely down. Watch its circuit breaker open while the others keep working.", map[string]ReceiverChaos{
		"inventory": {}, "email": {Down: true}, "analytics": {}}},
	{"slow-inventory", "Slow inventory service", "Inventory takes 2.5 s to respond. Backpressure keeps it from slowing everyone else.", map[string]ReceiverChaos{
		"inventory": {DelayMs: 2500}, "email": {}, "analytics": {}}},
	{"flaky-email", "Flaky email service", "Email fails 40% of requests: below the circuit breaker's 50% trip point, so retries carry it, and the unlucky few that fail 5 times in a row land in the DLQ.", map[string]ReceiverChaos{
		"inventory": {}, "email": {FailureRate: 0.4}, "analytics": {}}},
	{"random-network-failures", "Random network failures", "Every receiver fails 20% of requests at random. Retries absorb it.", map[string]ReceiverChaos{
		"inventory": {FailureRate: 0.2}, "email": {FailureRate: 0.2}, "analytics": {FailureRate: 0.2}}},
	{"everything-on-fire", "Everything on fire", "Email down, inventory slow and flaky, analytics dropping a third of requests.", map[string]ReceiverChaos{
		"inventory": {FailureRate: 0.5, DelayMs: 1500}, "email": {Down: true}, "analytics": {FailureRate: 0.35}}},
}

func modeOf(c ReceiverChaos) string {
	switch {
	case c.Down:
		return "down"
	case c.FailureRate > 0:
		return "flaky"
	case c.DelayMs >= 1000:
		return "slow"
	default:
		return "healthy"
	}
}

func (a *API) receiver(ctx context.Context, name, display string) receiverView {
	rdb := a.d.Redis
	m, _ := rdb.HGetAll(ctx, stats.ChaosPrefix+name).Result()
	c := ReceiverChaos{}
	c.FailureRate, _ = strconv.ParseFloat(m["failure_rate"], 64)
	c.DelayMs, _ = strconv.Atoi(m["delay_ms"])
	c.Down = m["down"] == "1"
	sm, _ := rdb.HGetAll(ctx, stats.ReceiverPrefix+name+":stats").Result()
	n := func(k string) int64 { v, _ := strconv.ParseInt(sm[k], 10, 64); return v }
	uniq, _ := rdb.SCard(ctx, stats.ReceiverPrefix+name+":ids").Result()
	return receiverView{
		Name: name, DisplayName: display, URL: a.d.Cfg.ReceiversURL + "/" + name, Chaos: c, Mode: modeOf(c),
		Stats: receiverStats{Received: n("received"), Unique: uniq, Duplicates: n("duplicates"),
			InvalidSignatures: n("invalid_signatures"), InjectedFailures: n("injected_failures")},
	}
}

func (a *API) receivers(ctx context.Context) []receiverView {
	out := make([]receiverView, 0, len(Receivers))
	for _, r := range Receivers {
		out = append(out, a.receiver(ctx, r.Name, r.Display))
	}
	return out
}

func (a *API) getChaos(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{"receivers": a.receivers(r.Context()), "presets": presets})
}

func (a *API) setChaos(ctx context.Context, name string, c ReceiverChaos) error {
	down := "0"
	if c.Down {
		down = "1"
	}
	return a.d.Redis.HSet(ctx, stats.ChaosPrefix+name,
		"failure_rate", strconv.FormatFloat(c.FailureRate, 'f', -1, 64), "delay_ms", strconv.Itoa(c.DelayMs), "down", down).Err()
}

func (a *API) putChaos(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	display := ""
	for _, rc := range Receivers {
		if rc.Name == name {
			display = rc.Display
		}
	}
	if display == "" {
		httpx.Error(w, http.StatusNotFound, "not_found", "unknown receiver")
		return
	}
	cur := a.receiver(r.Context(), name, display).Chaos
	var in struct {
		FailureRate *float64 `json:"failure_rate"`
		DelayMs     *int     `json:"delay_ms"`
		Down        *bool    `json:"down"`
	}
	if err := httpx.Decode(r, maxBody, &in); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if in.FailureRate != nil {
		cur.FailureRate = min(1, max(0, *in.FailureRate))
	}
	if in.DelayMs != nil {
		cur.DelayMs = min(30_000, max(0, *in.DelayMs))
	}
	if in.Down != nil {
		cur.Down = *in.Down
	}
	if err := a.setChaos(r.Context(), name, cur); err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, a.receiver(r.Context(), name, display))
}

func (a *API) applyPreset(w http.ResponseWriter, r *http.Request) {
	for _, p := range presets {
		if p.ID != r.PathValue("id") {
			continue
		}
		for name, c := range p.Config {
			if err := a.setChaos(r.Context(), name, c); err != nil {
				internalErr(w, err)
				return
			}
		}
		httpx.JSON(w, http.StatusOK, map[string]any{"receivers": a.receivers(r.Context())})
		return
	}
	httpx.Error(w, http.StatusNotFound, "not_found", "unknown preset")
}

// resetStats clears receiver counters and starts a fresh integrity window.
func (a *API) resetStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	for _, rc := range Receivers {
		a.d.Redis.Del(ctx, stats.ReceiverPrefix+rc.Name+":ids", stats.ReceiverPrefix+rc.Name+":stats")
	}
	a.d.Redis.Set(ctx, stats.IntegritySince, time.Now().UnixMilli(), 0)
	httpx.JSON(w, http.StatusOK, map[string]bool{"ok": true})
}
