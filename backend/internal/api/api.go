// Package api serves the public ingestion API (/v1) and the management API used by the
// dashboard (/api).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/ratelimit"
	"github.com/boomerang-dev/boomerang/internal/scaler"
	"github.com/boomerang-dev/boomerang/internal/ssrf"
	"github.com/boomerang-dev/boomerang/internal/stats"
	"github.com/boomerang-dev/boomerang/internal/store"
)

// API is the HTTP API service.
type API struct {
	d        *app.Deps
	limiter  *ratelimit.Limiter
	ssrf     *ssrf.Policy
	scaler   scaler.Scaler
	received *stats.Accumulator

	keyTouched sync.Map // key id -> last time we recorded its use
}

// New creates the API.
func New(d *app.Deps) *API {
	a := &API{
		d: d, limiter: ratelimit.New(d.Redis), ssrf: ssrf.New(d.Cfg.SSRFAllowHosts),
		received: stats.NewAccumulator(d.Redis, stats.Received), scaler: scaler.None{},
	}
	switch d.Cfg.ScalerKind {
	case "docker":
		if s, err := scaler.NewDocker(d.Cfg.DockerSocket, d.Cfg.ComposeProject, d.Cfg.WorkerService); err == nil {
			a.scaler = s
		} else {
			slog.Warn("docker scaler unavailable", "err", err)
		}
	case "kubernetes":
		if s, err := scaler.NewKubernetes(d.Cfg.K8sNamespace, d.Cfg.K8sWorkerDeploy); err == nil {
			a.scaler = s
		} else {
			slog.Warn("kubernetes scaler unavailable", "err", err)
		}
	}
	return a
}

// Run starts background loops.
func (a *API) Run(ctx context.Context) {
	info, _ := json.Marshal(map[string]any{"enabled": a.scaler.Kind() != "none", "kind": a.scaler.Kind()})
	a.d.Redis.Set(ctx, "scaler:info", info, 0)
	a.d.Redis.Del(ctx, stats.ScalerDesired) // a fresh deployment starts from its configured replicas
	a.received.Run(ctx, 100*time.Millisecond)
}

// Mount registers routes.
func (a *API) Mount(mux *http.ServeMux) {
	// Public ingestion
	mux.HandleFunc("POST /v1/events", a.ingestOne)
	mux.HandleFunc("POST /v1/events/batch", a.ingestBatch)

	// Tenants & settings
	mux.HandleFunc("GET /api/tenants", a.listTenants)
	mux.HandleFunc("POST /api/tenants", a.createTenant)
	mux.HandleFunc("GET /api/tenants/{tid}", a.getTenant)
	mux.HandleFunc("PATCH /api/tenants/{tid}", a.updateTenant)
	mux.HandleFunc("GET /api/tenants/{tid}/keys", a.listKeys)
	mux.HandleFunc("POST /api/tenants/{tid}/keys", a.createKey)
	mux.HandleFunc("DELETE /api/tenants/{tid}/keys/{kid}", a.revokeKey)

	// Endpoints
	mux.HandleFunc("GET /api/tenants/{tid}/endpoints", a.listEndpoints)
	mux.HandleFunc("POST /api/tenants/{tid}/endpoints", a.createEndpoint)
	mux.HandleFunc("GET /api/tenants/{tid}/endpoints/{eid}", a.getEndpoint)
	mux.HandleFunc("PATCH /api/tenants/{tid}/endpoints/{eid}", a.updateEndpoint)
	mux.HandleFunc("DELETE /api/tenants/{tid}/endpoints/{eid}", a.deleteEndpoint)
	mux.HandleFunc("POST /api/tenants/{tid}/endpoints/{eid}/pause", a.pauseEndpoint(true))
	mux.HandleFunc("POST /api/tenants/{tid}/endpoints/{eid}/resume", a.pauseEndpoint(false))
	mux.HandleFunc("GET /api/tenants/{tid}/endpoints/{eid}/secret", a.getSecret)
	mux.HandleFunc("POST /api/tenants/{tid}/endpoints/{eid}/rotate-secret", a.rotateSecret)
	mux.HandleFunc("POST /api/tenants/{tid}/endpoints/{eid}/replay-dlq", a.replayDLQ)
	mux.HandleFunc("GET /api/event-types", a.eventTypes)

	// Event explorer
	mux.HandleFunc("GET /api/tenants/{tid}/events", a.searchEvents)
	mux.HandleFunc("GET /api/tenants/{tid}/events/{evid}", a.getEvent)
	mux.HandleFunc("POST /api/tenants/{tid}/events/{evid}/replay", a.replayEvent)
	mux.HandleFunc("POST /api/tenants/{tid}/deliveries/{did}/replay", a.replayDelivery)

	// Chaos
	mux.HandleFunc("GET /api/chaos", a.getChaos)
	mux.HandleFunc("PUT /api/chaos/{name}", a.putChaos)
	mux.HandleFunc("POST /api/chaos/presets/{id}", a.applyPreset)
	mux.HandleFunc("POST /api/chaos/reset-stats", a.resetStats)

	// Infrastructure
	mux.HandleFunc("POST /api/infra/workers/scale", a.scaleWorkers)
	mux.HandleFunc("POST /api/infra/workers/{wid}/kill", a.killWorker(false))
	mux.HandleFunc("POST /api/infra/workers/{wid}/stop", a.killWorker(true))
	mux.HandleFunc("GET /api/metrics/timeseries", a.timeseries)

	// Internal (not routed by the gateway)
	mux.HandleFunc("GET /internal/secrets", a.internalSecrets)
}

// storeErr maps repository errors to HTTP responses.
func storeErr(w http.ResponseWriter, err error, what string) {
	if errors.Is(err, store.ErrNotFound) {
		httpx.Error(w, http.StatusNotFound, "not_found", what+" not found")
		return
	}
	slog.Error("store error", "what", what, "err", err)
	httpx.Error(w, http.StatusInternalServerError, "internal", "internal error")
}

func internalErr(w http.ResponseWriter, err error) {
	slog.Error("internal error", "err", err)
	httpx.Error(w, http.StatusInternalServerError, "internal", "internal error")
}
