package api

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/boomerang-dev/boomerang/internal/circuit"
	"github.com/boomerang-dev/boomerang/internal/config"
	"github.com/boomerang-dev/boomerang/internal/health"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/ssrf"
	"github.com/boomerang-dev/boomerang/internal/store"
)

const maxBody = 64 << 10

// --- tenants ---

func (a *API) listTenants(w http.ResponseWriter, r *http.Request) {
	ts, err := a.d.Store.ListTenants(r.Context())
	if err != nil {
		internalErr(w, err)
		return
	}
	if ts == nil {
		ts = []*model.Tenant{}
	}
	httpx.JSON(w, http.StatusOK, ts)
}

func (a *API) createTenant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if err := httpx.Decode(r, maxBody, &in); err != nil || strings.TrimSpace(in.Name) == "" {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "name is required")
		return
	}
	t, err := a.d.Store.CreateTenant(r.Context(), &model.Tenant{
		Name: strings.TrimSpace(in.Name), RateLimitRPS: 1000, RateLimitBurst: 2000, RetryMaxAttempts: 5,
		RetryScheduleMs: config.DefaultRetrySchedule(a.d.Cfg.DemoMode),
	})
	if err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusCreated, t)
}

func (a *API) getTenant(w http.ResponseWriter, r *http.Request) {
	t, err := a.d.Store.GetTenant(r.Context(), r.PathValue("tid"))
	if err != nil {
		storeErr(w, err, "tenant")
		return
	}
	httpx.JSON(w, http.StatusOK, t)
}

func (a *API) updateTenant(w http.ResponseWriter, r *http.Request) {
	var p store.TenantPatch
	if err := httpx.Decode(r, maxBody, &p); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	switch {
	case p.RateLimitRPS != nil && (*p.RateLimitRPS < 1 || *p.RateLimitRPS > 1_000_000):
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "rate_limit_rps must be 1..1000000")
		return
	case p.RateLimitBurst != nil && (*p.RateLimitBurst < 1 || *p.RateLimitBurst > 10_000_000):
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "rate_limit_burst must be 1..10000000")
		return
	case p.RetryMaxAttempts != nil && (*p.RetryMaxAttempts < 1 || *p.RetryMaxAttempts > 50):
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "retry_max_attempts must be 1..50")
		return
	case p.RetryScheduleMs != nil && (len(p.RetryScheduleMs) == 0 || len(p.RetryScheduleMs) > 50 ||
		slices.ContainsFunc(p.RetryScheduleMs, func(v int) bool { return v < 100 || v > 7*24*3600*1000 })):
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "retry_schedule_ms needs 1..50 delays between 100ms and 7 days")
		return
	}
	t, err := a.d.Store.UpdateTenant(r.Context(), r.PathValue("tid"), p)
	if err != nil {
		storeErr(w, err, "tenant")
		return
	}
	a.d.Cache.InvalidateTenant(r.Context(), t.ID)
	httpx.JSON(w, http.StatusOK, t)
}

// --- API keys ---

func (a *API) listKeys(w http.ResponseWriter, r *http.Request) {
	ks, err := a.d.Store.ListKeys(r.Context(), r.PathValue("tid"))
	if err != nil {
		internalErr(w, err)
		return
	}
	if ks == nil {
		ks = []*model.APIKey{}
	}
	httpx.JSON(w, http.StatusOK, ks)
}

func (a *API) createKey(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if err := httpx.Decode(r, maxBody, &in); err != nil || strings.TrimSpace(in.Name) == "" {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "name is required")
		return
	}
	if _, err := a.d.Store.GetTenant(r.Context(), r.PathValue("tid")); err != nil {
		storeErr(w, err, "tenant")
		return
	}
	k, raw, err := a.d.Store.CreateKey(r.Context(), r.PathValue("tid"), strings.TrimSpace(in.Name), "")
	if err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusCreated, struct {
		*model.APIKey
		Key string `json:"key"`
	}{k, raw})
}

func (a *API) revokeKey(w http.ResponseWriter, r *http.Request) {
	k, err := a.d.Store.RevokeKey(r.Context(), r.PathValue("tid"), r.PathValue("kid"))
	if err != nil {
		storeErr(w, err, "API key")
		return
	}
	a.d.Cache.InvalidateKeys(r.Context())
	httpx.JSON(w, http.StatusOK, k)
}

// --- endpoints ---

type endpointHealth struct {
	Circuit               string     `json:"circuit"`
	ConsecutiveFailures   int        `json:"consecutive_failures"`
	CircuitOpenedAt       *time.Time `json:"circuit_opened_at"`
	SuccessRate           *float64   `json:"success_rate"`
	Attempts5m            int64      `json:"attempts_5m"`
	Failures5m            int64      `json:"failures_5m"`
	P95LatencyMs          *float64   `json:"p95_latency_ms"`
	Score                 int        `json:"score"`
	Status                string     `json:"status"`
	RateLimitEffectiveRPS *float64   `json:"rate_limit_effective_rps"`
}

type endpointView struct {
	*model.Endpoint
	SecretPrefix string         `json:"secret_prefix"`
	Health       endpointHealth `json:"health"`
}

func (a *API) views(ctx context.Context, tenantID string, eps []*model.Endpoint) []endpointView {
	hs, _ := a.d.CH.EndpointHealth(ctx, tenantID) // health is best-effort; config still renders
	out := make([]endpointView, 0, len(eps))
	for _, e := range eps {
		cb := circuit.Read(ctx, a.d.Redis, e.ID, a.d.Cfg.CircuitCooldown)
		h := endpointHealth{Circuit: cb.State, ConsecutiveFailures: cb.ConsecutiveFailures, CircuitOpenedAt: cb.OpenedAt}
		if e.RateLimitRPS > 0 {
			h.RateLimitEffectiveRPS = cb.EffectiveRate
			if h.RateLimitEffectiveRPS == nil {
				v := float64(e.RateLimitRPS)
				h.RateLimitEffectiveRPS = &v
			}
		}
		if s, ok := hs[e.ID]; ok && s.Attempts > 0 {
			sr := float64(s.Attempts-s.Failures) / float64(s.Attempts)
			p95 := s.P95
			h.SuccessRate, h.Attempts5m, h.Failures5m, h.P95LatencyMs = &sr, s.Attempts, s.Failures, &p95
		}
		h.Status, h.Score = health.Classify(e.Paused, cb.State, h.SuccessRate)
		prefix := e.Secret
		if len(prefix) > 10 {
			prefix = prefix[:10] + "…"
		}
		out = append(out, endpointView{Endpoint: e, SecretPrefix: prefix, Health: h})
	}
	return out
}

func (a *API) loadEndpoint(w http.ResponseWriter, r *http.Request) (*model.Endpoint, bool) {
	e, err := a.d.Store.GetEndpoint(r.Context(), r.PathValue("eid"))
	if err == nil && e.TenantID != r.PathValue("tid") {
		err = store.ErrNotFound
	}
	if err != nil {
		storeErr(w, err, "endpoint")
		return nil, false
	}
	return e, true
}

func (a *API) listEndpoints(w http.ResponseWriter, r *http.Request) {
	eps, err := a.d.Store.ListEndpoints(r.Context(), r.PathValue("tid"))
	if err != nil {
		internalErr(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, a.views(r.Context(), r.PathValue("tid"), eps))
}

func (a *API) getEndpoint(w http.ResponseWriter, r *http.Request) {
	e, ok := a.loadEndpoint(w, r)
	if !ok {
		return
	}
	httpx.JSON(w, http.StatusOK, a.views(r.Context(), e.TenantID, []*model.Endpoint{e})[0])
}

func (a *API) validateEndpoint(w http.ResponseWriter, r *http.Request, name, url *string, types []string, rate *int) bool {
	if name != nil && strings.TrimSpace(*name) == "" {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "name is required")
		return false
	}
	if url != nil {
		if err := a.ssrf.ValidateURL(r.Context(), *url); err != nil {
			code := "invalid_url"
			if errors.Is(err, ssrf.ErrBlocked) {
				code = "ssrf_blocked"
			}
			httpx.Error(w, http.StatusUnprocessableEntity, code, err.Error())
			return false
		}
	}
	if types != nil {
		if len(types) == 0 {
			httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "subscribe to at least one event type")
			return false
		}
		for _, t := range types {
			if t != "*" && !eventTypeRe.MatchString(strings.TrimSuffix(t, ".*")) {
				httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "invalid event type pattern: "+t)
				return false
			}
		}
	}
	if rate != nil && (*rate < 0 || *rate > 100_000) {
		httpx.Error(w, http.StatusUnprocessableEntity, "validation_failed", "rate_limit_rps must be 0..100000")
		return false
	}
	return true
}

func (a *API) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name         string   `json:"name"`
		URL          string   `json:"url"`
		Description  string   `json:"description"`
		EventTypes   []string `json:"event_types"`
		RateLimitRPS int      `json:"rate_limit_rps"`
	}
	if err := httpx.Decode(r, maxBody, &in); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if in.EventTypes == nil {
		in.EventTypes = []string{}
	}
	if !a.validateEndpoint(w, r, &in.Name, &in.URL, in.EventTypes, &in.RateLimitRPS) {
		return
	}
	tid := r.PathValue("tid")
	if _, err := a.d.Store.GetTenant(r.Context(), tid); err != nil {
		storeErr(w, err, "tenant")
		return
	}
	e, err := a.d.Store.CreateEndpoint(r.Context(), &model.Endpoint{
		TenantID: tid, Name: strings.TrimSpace(in.Name), URL: in.URL, Description: in.Description,
		EventTypes: in.EventTypes, RateLimitRPS: in.RateLimitRPS,
	})
	if err != nil {
		internalErr(w, err)
		return
	}
	a.d.Cache.InvalidateEndpoint(r.Context(), tid, e.ID)
	httpx.JSON(w, http.StatusCreated, a.views(r.Context(), tid, []*model.Endpoint{e})[0])
}

func (a *API) updateEndpoint(w http.ResponseWriter, r *http.Request) {
	var p store.EndpointPatch
	if err := httpx.Decode(r, maxBody, &p); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if !a.validateEndpoint(w, r, p.Name, p.URL, p.EventTypes, p.RateLimitRPS) {
		return
	}
	a.patchEndpoint(w, r, p)
}

func (a *API) patchEndpoint(w http.ResponseWriter, r *http.Request, p store.EndpointPatch) {
	tid := r.PathValue("tid")
	e, err := a.d.Store.UpdateEndpoint(r.Context(), tid, r.PathValue("eid"), p)
	if err != nil {
		storeErr(w, err, "endpoint")
		return
	}
	a.d.Cache.InvalidateEndpoint(r.Context(), tid, e.ID)
	httpx.JSON(w, http.StatusOK, a.views(r.Context(), tid, []*model.Endpoint{e})[0])
}

func (a *API) pauseEndpoint(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.patchEndpoint(w, r, store.EndpointPatch{Paused: &paused})
	}
}

func (a *API) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	tid, eid := r.PathValue("tid"), r.PathValue("eid")
	if err := a.d.Store.DeleteEndpoint(r.Context(), tid, eid); err != nil {
		storeErr(w, err, "endpoint")
		return
	}
	a.d.Cache.InvalidateEndpoint(r.Context(), tid, eid)
	w.WriteHeader(http.StatusNoContent)
}

type secretView struct {
	Secret            string     `json:"secret"`
	PreviousSecret    *string    `json:"previous_secret"`
	PreviousExpiresAt *time.Time `json:"previous_expires_at"`
}

func secretsOf(e *model.Endpoint) secretView {
	v := secretView{Secret: e.Secret}
	if e.PreviousSecret != "" && e.PreviousExpiresAt != nil && time.Now().Before(*e.PreviousExpiresAt) {
		v.PreviousSecret, v.PreviousExpiresAt = &e.PreviousSecret, e.PreviousExpiresAt
	}
	return v
}

func (a *API) getSecret(w http.ResponseWriter, r *http.Request) {
	e, ok := a.loadEndpoint(w, r)
	if !ok {
		return
	}
	httpx.JSON(w, http.StatusOK, secretsOf(e))
}

func (a *API) rotateSecret(w http.ResponseWriter, r *http.Request) {
	e, err := a.d.Store.RotateSecret(r.Context(), r.PathValue("tid"), r.PathValue("eid"), a.d.Cfg.SecretRotationGrace)
	if err != nil {
		storeErr(w, err, "endpoint")
		return
	}
	a.d.Cache.InvalidateEndpoint(r.Context(), e.TenantID, e.ID)
	httpx.JSON(w, http.StatusOK, secretsOf(e))
}

func (a *API) eventTypes(w http.ResponseWriter, _ *http.Request) {
	types := slices.Clone(model.ShopSimEventTypes)
	sort.Strings(types)
	httpx.JSON(w, http.StatusOK, types)
}

// internalSecrets lets the mock receivers fetch the secrets they must verify against, the way a
// real receiver would have its secret configured. Not exposed through the gateway.
func (a *API) internalSecrets(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Internal-Token") != a.d.Cfg.InternalToken {
		httpx.Error(w, http.StatusUnauthorized, "unauthorized", "internal token required")
		return
	}
	url := r.URL.Query().Get("url")
	eps, err := a.d.Store.ListAllEndpoints(r.Context())
	if err != nil {
		internalErr(w, err)
		return
	}
	secrets := []string{}
	for _, e := range eps {
		if e.URL == url {
			secrets = append(secrets, e.Secrets(time.Now())...)
		}
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"secrets": secrets})
}
