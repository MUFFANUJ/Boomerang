// Package obs wires logging, Prometheus metrics, OpenTelemetry tracing and the standard
// health/metrics HTTP surface shared by every service.
package obs

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Logger builds the process logger (JSON to stdout, collected by Loki).
func Logger(service, level string) *slog.Logger {
	var lv slog.Level
	_ = lv.UnmarshalText([]byte(level))
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv})
	l := slog.New(h).With("service", service)
	slog.SetDefault(l)
	return l
}

// Tracer returns the service tracer. When endpoint is empty, tracing is a no-op.
func InitTracing(ctx context.Context, service, endpoint string) (shutdown func(context.Context) error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		return func(context.Context) error { return nil }
	}
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"))
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
		return func(context.Context) error { return nil }
	}
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("boomerang-"+service)))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown
}

// Tracer returns a named tracer.
func Tracer(name string) trace.Tracer { return otel.Tracer("boomerang/" + name) }

// Health tracks readiness checks.
type Health struct {
	mu       sync.RWMutex
	checks   map[string]func(context.Context) error
	stopping bool
}

// NewHealth creates an empty health registry.
func NewHealth() *Health { return &Health{checks: map[string]func(context.Context) error{}} }

// Add registers a readiness check.
func (h *Health) Add(name string, fn func(context.Context) error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = fn
}

// SetStopping makes readiness fail so load balancers drain this instance.
func (h *Health) SetStopping() {
	h.mu.Lock()
	h.stopping = true
	h.mu.Unlock()
}

func (h *Health) ready(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.stopping {
		http.Error(w, "stopping", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for name, fn := range h.checks {
		if err := fn(ctx); err != nil {
			http.Error(w, name+": "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	_, _ = w.Write([]byte("ok"))
}

// Mount adds /healthz, /readyz and /metrics to a mux.
func (h *Health) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", h.ready)
	mux.Handle("GET /metrics", promhttp.Handler())
}

// Serve runs an HTTP server until ctx is cancelled, then shuts it down gracefully.
func Serve(ctx context.Context, addr string, handler http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(shCtx)
}

// Metrics shared across services.
var (
	EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "boomerang_events_ingested_total", Help: "Events accepted by the ingestion API.",
	}, []string{"tenant", "result"})
	IngestLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "boomerang_ingest_duration_seconds", Help: "Ingestion request latency.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	})
	DeliveriesCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "boomerang_deliveries_created_total", Help: "Delivery tasks created by fan-out.",
	})
	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "boomerang_delivery_attempts_total", Help: "Delivery attempts by outcome.",
	}, []string{"endpoint", "outcome"})
	DeliveryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "boomerang_delivery_duration_seconds", Help: "Webhook HTTP latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"endpoint"})
	DLQTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "boomerang_dlq_total", Help: "Deliveries moved to the dead-letter queue.",
	})
	CircuitState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "boomerang_circuit_open", Help: "1 when the endpoint circuit is open.",
	}, []string{"endpoint"})
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "boomerang_consumer_lag", Help: "Kafka consumer lag per partition.",
	}, []string{"group", "topic", "partition"})
	DLQDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "boomerang_dlq_depth", Help: "Deliveries currently dead-lettered.",
	})
	WorkerInflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "boomerang_worker_inflight", Help: "Deliveries in flight in this worker.",
	})
	RetriesScheduled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "boomerang_retries_scheduled_total", Help: "Tasks sent to retry tiers.",
	}, []string{"tier", "reason"})
)
