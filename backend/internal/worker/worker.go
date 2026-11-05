// Package worker delivers webhooks. Each task is checked against the endpoint's circuit
// breaker and rate limiter, signed, POSTed with a strict timeout, and then either recorded as
// delivered, scheduled on a retry tier, or dead-lettered. Every outcome is published to the
// status stream before the task's offset can be committed.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/circuit"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/obs"
	"github.com/boomerang-dev/boomerang/internal/signing"
	"github.com/boomerang-dev/boomerang/internal/ssrf"
	"github.com/boomerang-dev/boomerang/internal/stats"
	"github.com/boomerang-dev/boomerang/internal/store"
)

// Worker is a delivery worker process.
type Worker struct {
	d      *app.Deps
	id     string
	guard  *circuit.Guard
	hc     *http.Client
	tracer trace.Tracer

	epMu    sync.Mutex
	epSlots map[string]chan struct{}
	epLatMu sync.Mutex
	epLat   map[string]float64 // EWMA of webhook latency (ms) per endpoint

	processed atomic.Int64
	inflight  atomic.Int64
	started   time.Time
	stopping  atomic.Bool
	gc        *kafkax.GroupConsumer
}

// New creates a worker.
func New(d *app.Deps) *Worker {
	policy := ssrf.New(d.Cfg.SSRFAllowHosts)
	dialer := &net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		DialContext:           policy.DialContext(dialer),
		MaxIdleConns:          2048,
		MaxIdleConnsPerHost:   512,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: d.Cfg.DeliveryTimeout,
		ForceAttemptHTTP2:     true,
	}
	return &Worker{
		d: d, id: d.Cfg.ServiceID, tracer: obs.Tracer("worker"),
		guard: circuit.New(d.Redis, circuit.Config{
			FailureThreshold: d.Cfg.CircuitFailureThreshold,
			Cooldown:         d.Cfg.CircuitCooldown,
			ProbeLease:       d.Cfg.DeliveryTimeout + 2*time.Second,
		}),
		hc: &http.Client{
			Transport: tr,
			// Never follow redirects: a redirect could point at an internal address.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		epSlots: map[string]chan struct{}{},
		epLat:   map[string]float64{},
		started: time.Now().UTC(),
	}
}

// Run consumes delivery tasks until ctx is cancelled, then drains in-flight work.
func (w *Worker) Run(ctx context.Context) error {
	gc, err := kafkax.NewGroupConsumer(kafkax.GroupOptions{
		Brokers: w.d.Cfg.KafkaBrokers, Group: model.GroupDelivery, Topics: []string{model.TopicDeliveries},
		ClientID: "worker-" + w.id,
		Factory: func(ctl *kafkax.Control) kafkax.Partition {
			return newTrackedPartition(ctl, w.d.Cfg.WorkerPartitionMaxPending, w.process)
		},
	})
	if err != nil {
		return err
	}
	w.gc = gc
	hbCtx, hbCancel := context.WithCancel(context.Background())
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); w.heartbeat(hbCtx) }()
	go func() {
		<-ctx.Done()
		w.stopping.Store(true)
		w.writeHeartbeat(context.Background(), 0)
	}()
	err = gc.Run(ctx)
	hbCancel()
	<-hbDone
	c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.d.Redis.ZRem(c, stats.WorkersSeen, w.id)
	w.d.Redis.Del(c, stats.WorkerInfoPrefix+w.id)
	return err
}

// slowEndpointMs is the latency above which an endpoint counts as slow for backpressure.
const slowEndpointMs = 750

func (w *Worker) observeLatency(ep string, d time.Duration) {
	ms := float64(d.Milliseconds())
	w.epLatMu.Lock()
	defer w.epLatMu.Unlock()
	if cur, ok := w.epLat[ep]; ok {
		w.epLat[ep] = cur*0.9 + ms*0.1
	} else {
		w.epLat[ep] = ms
	}
}

func (w *Worker) latency(ep string) float64 {
	w.epLatMu.Lock()
	defer w.epLatMu.Unlock()
	return w.epLat[ep]
}

// slots returns the endpoint's concurrency semaphore for this worker.
func (w *Worker) slots(ep string) chan struct{} {
	w.epMu.Lock()
	defer w.epMu.Unlock()
	c, ok := w.epSlots[ep]
	if !ok {
		c = make(chan struct{}, w.d.Cfg.WorkerEndpointInflight)
		w.epSlots[ep] = c
	}
	return c
}

// process handles one record. It returns only once the outcome is durably recorded in Kafka
// (status stream plus retry/DLQ topic), so committing its offset afterwards is safe.
func (w *Worker) process(rec *kgo.Record) {
	var task model.DeliveryTask
	if err := json.Unmarshal(rec.Value, &task); err != nil {
		slog.Error("dropping malformed task", "offset", rec.Offset, "err", err)
		return
	}
	w.inflight.Add(1)
	obs.WorkerInflight.Inc()
	defer func() {
		w.inflight.Add(-1)
		obs.WorkerInflight.Dec()
		w.processed.Add(1)
	}()
	ctx, span := w.tracer.Start(kafkax.ExtractContext(context.Background(), rec), "deliver",
		trace.WithAttributes(attribute.String("delivery.id", task.DeliveryID), attribute.String("event.id", task.EventID),
			attribute.String("endpoint.id", task.EndpointID), attribute.Int("attempt", task.Attempt)))
	defer span.End()

	// Infrastructure errors (Kafka/Redis/Postgres blips) are retried here until they succeed;
	// the task is not lost and its offset is not committed in the meantime.
	backoff := 250 * time.Millisecond
	for {
		err := w.deliver(ctx, &task)
		if err == nil {
			return
		}
		span.RecordError(err)
		slog.Warn("delivery bookkeeping failed, retrying", "delivery", task.DeliveryID, "err", err)
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (w *Worker) deliver(ctx context.Context, task *model.DeliveryTask) error {
	ep, err := w.d.Cache.Endpoint(ctx, task.EndpointID)
	if errors.Is(err, store.ErrNotFound) {
		return w.dead(ctx, task, "", "Endpoint was deleted", false, nil)
	}
	if err != nil {
		return w.deferTask(ctx, task, "", "Endpoint configuration unavailable: "+err.Error(), 2*time.Second, model.StateRetrying, throttle)
	}
	tenant, err := w.d.Cache.Tenant(ctx, task.TenantID)
	if err != nil {
		return w.deferTask(ctx, task, ep.Name, "Tenant configuration unavailable", 2*time.Second, model.StateRetrying, throttle)
	}
	if ep.Paused {
		return w.deferTask(ctx, task, ep.Name, "Endpoint is paused; delivery will resume when it is unpaused", w.pausedDelay(), model.StatePaused, hold)
	}

	// Backpressure: cap concurrent deliveries per endpoint so one slow receiver can't hog the
	// worker. A task waits briefly for a slot; if the endpoint stays saturated, the task is
	// shunted to the retry tier without spending an attempt, freeing the partition for others.
	sem := w.slots(ep.ID)
	wait := time.NewTimer(w.d.Cfg.WorkerBackpressureWait)
	select {
	case sem <- struct{}{}:
		wait.Stop()
	case <-wait.C:
		// Shunting a task aside only helps when the endpoint itself is slow. If the endpoint
		// is fast and we're just busy, deferring would only add work, so keep waiting and let
		// Kafka hold the backlog.
		if lat := w.latency(ep.ID); lat >= slowEndpointMs {
			return w.deferTask(ctx, task, ep.Name, fmt.Sprintf("Backpressure: endpoint is slow (~%.0f ms) and %d deliveries are already in flight",
				lat, cap(sem)), jitter(5*time.Second), model.StateRetrying, throttle)
		}
		sem <- struct{}{}
	}
	defer func() { <-sem }()

	maxRate := float64(ep.RateLimitRPS)
	dec, err := w.guard.Before(ctx, ep.ID, maxRate)
	if err != nil {
		slog.Warn("guard unavailable, failing open", "err", err)
		dec = circuit.Decision{Decision: circuit.Allow, State: circuit.Closed}
	}
	if dec.Decision == circuit.Allow && dec.Wait > 0 {
		// We hold a reserved rate-limit slot; wait for it exactly once.
		time.Sleep(dec.Wait)
	}
	switch dec.Decision {
	case circuit.DenyOpen:
		return w.deferTask(ctx, task, ep.Name, "Circuit open: endpoint is failing, holding deliveries until it recovers", jitter(max(dec.Wait, time.Second)), model.StateRetrying, hold)
	case circuit.DenyProbe:
		return w.deferTask(ctx, task, ep.Name, "Circuit half-open: waiting for the probe delivery to finish", jitter(max(dec.Wait, time.Second)), model.StateRetrying, hold)
	case circuit.DenyLimited:
		return w.deferTask(ctx, task, ep.Name, fmt.Sprintf("Rate limited at %.0f deliveries/s for this endpoint", dec.EffectiveRate), jitter(max(dec.Wait, 3*time.Second)), model.StateRetrying, throttle)
	}

	body := []byte(task.Body)
	if task.BodyRef != "" {
		if body, err = w.d.Blob.Get(ctx, task.BodyRef); err != nil {
			return w.deferTask(ctx, task, ep.Name, "Payload storage unavailable: "+err.Error(), 5*time.Second, model.StateRetrying, throttle)
		}
	}

	res := w.send(ctx, ep, task, body)
	w.observeLatency(ep.ID, res.latency)
	success := res.status >= 200 && res.status < 300
	overload := res.timeout || res.status == 429 || res.status == 503
	out, gerr := w.guard.After(ctx, ep.ID, success, overload, maxRate)
	if gerr != nil {
		slog.Warn("guard record failed", "err", gerr)
	} else if out.Changed {
		slog.Info("circuit state changed", "endpoint", ep.Name, "state", out.State)
		v := 0.0
		if out.State == circuit.Open {
			v = 1
		}
		obs.CircuitState.WithLabelValues(ep.Name).Set(v)
	}
	obs.DeliveryLatency.WithLabelValues(ep.Name).Observe(res.latency.Seconds())

	now := time.Now().UTC()
	ar := &model.AttemptRecord{
		DeliveryID: task.DeliveryID, EventID: task.EventID, TenantID: task.TenantID, EndpointID: ep.ID,
		EndpointName: ep.Name, EventType: task.EventType, EventAt: task.EventAt, Attempt: task.Attempt,
		Attempted: true, StatusCode: res.status, ResponseBody: res.body, LatencyMs: int(res.latency.Milliseconds()),
		Error: res.errText, Signature: res.signature, WorkerID: w.id, TS: now,
	}
	if success {
		ar.Outcome, ar.State = model.OutcomeSuccess, model.StateDelivered
		obs.DeliveryAttempts.WithLabelValues(ep.Name, model.OutcomeSuccess).Inc()
		return w.publish(ctx, ar)
	}
	reason := failureReason(res)
	if task.Attempt >= task.MaxAttempts {
		return w.dead(ctx, task, ep.Name, fmt.Sprintf("Retries exhausted after %d attempts (last: %s)", task.Attempt, reason), true, ar)
	}
	delay := jitter(tenant.RetryDelay(task.Attempt + 1))
	next := now.Add(delay)
	ar.Outcome, ar.State, ar.NextRetryAt = model.OutcomeFailure, model.StateRetrying, &next
	ar.Reason = fmt.Sprintf("%s; retry %d of %d in %s", reason, task.Attempt, task.MaxAttempts-1, delay.Round(100*time.Millisecond))
	if out.Changed && out.State == circuit.Open {
		ar.Reason += "; circuit opened"
	}
	obs.DeliveryAttempts.WithLabelValues(ep.Name, model.OutcomeFailure).Inc()
	retryTask := *task
	retryTask.Attempt++
	retryTask.NotBefore = next
	retryTask.DeferredSince, retryTask.DeferReason, retryTask.DeferReported = nil, "", nil
	return w.schedule(ctx, &retryTask, delay, "failure", ar)
}

type sendResult struct {
	status    int
	body      string
	latency   time.Duration
	errText   string
	timeout   bool
	signature string
}

func (w *Worker) send(ctx context.Context, ep *model.Endpoint, task *model.DeliveryTask, body []byte) sendResult {
	ts := time.Now().Unix()
	sig := signing.SignatureHeader(ep.Secrets(time.Now()), task.DeliveryID, ts, body)
	rctx, cancel := context.WithTimeout(ctx, w.d.Cfg.DeliveryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil {
		return sendResult{errText: err.Error(), signature: sig}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Boomerang-Webhooks/1.0")
	req.Header.Set(signing.HeaderID, task.DeliveryID)
	req.Header.Set(signing.HeaderEventID, task.EventID)
	req.Header.Set(signing.HeaderEventType, task.EventType)
	req.Header.Set(signing.HeaderAttempt, strconv.Itoa(task.Attempt))
	req.Header.Set(signing.HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(signing.HeaderSignature, sig)
	start := time.Now()
	resp, err := w.hc.Do(req)
	if err != nil {
		r := sendResult{latency: time.Since(start), errText: err.Error(), signature: sig}
		var ne net.Error
		r.timeout = errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
		return r
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024)) // let the connection be reused
	return sendResult{status: resp.StatusCode, body: string(b), latency: time.Since(start), signature: sig}
}

func failureReason(r sendResult) string {
	switch {
	case r.timeout:
		return "Timed out waiting for the receiver"
	case r.status == 0 && strings.Contains(r.errText, "connection refused"):
		return "Connection refused"
	case r.status == 0:
		return "Network error: " + r.errText
	default:
		return fmt.Sprintf("Receiver returned HTTP %d %s", r.status, http.StatusText(r.status))
	}
}

func (w *Worker) pausedDelay() time.Duration {
	if w.d.Cfg.DemoMode {
		return jitter(5 * time.Second)
	}
	return jitter(30 * time.Second)
}

// Deferral kinds. A hold is caused by the endpoint (circuit open, paused) and ages out into the
// DLQ after DeferMaxAge. A throttle is Boomerang protecting a receiver or itself (backpressure,
// rate limiting, a dependency blip); it never kills a delivery, it only delays it.
type deferKind bool

const (
	hold     deferKind = true
	throttle deferKind = false
)

// deferTask reschedules a task without spending an attempt.
// The longer a delivery has been held, the less often it is re-checked, and an unchanged
// deferral is only reported to the status stream every 30s, so a long outage doesn't flood the
// pipeline (or the dashboard) with identical "still waiting" records.
func (w *Worker) deferTask(ctx context.Context, task *model.DeliveryTask, epName, reason string, delay time.Duration, state string, kind deferKind) error {
	now := time.Now().UTC()
	t := *task
	if kind == hold {
		if t.DeferredSince == nil {
			t.DeferredSince = &now
		} else if held := now.Sub(*t.DeferredSince); held > w.d.Cfg.DeferMaxAge {
			return w.dead(ctx, task, epName, "Held for too long without a delivery attempt: "+reason, false, nil)
		} else if backoff := min(held/3, 30*time.Second); backoff > delay {
			delay = jitter(backoff)
		}
	}
	next := now.Add(delay)
	t.NotBefore = next
	report := t.DeferReported == nil || t.DeferReason != reason || now.Sub(*t.DeferReported) >= 30*time.Second
	obs.DeliveryAttempts.WithLabelValues(epName, model.OutcomeDeferred).Inc()
	if !report {
		tier := model.TierFor(delay)
		obs.RetriesScheduled.WithLabelValues(strconv.Itoa(tier.Tier), "deferred").Inc()
		rr, err := kafkax.JSONRecord(ctx, tier.Topic, t.PartitionKey(), t)
		if err != nil {
			return err
		}
		return kafkax.ProduceSync(ctx, w.d.Producer, rr)
	}
	t.DeferReason, t.DeferReported = reason, &now
	ar := &model.AttemptRecord{
		DeliveryID: t.DeliveryID, EventID: t.EventID, TenantID: t.TenantID, EndpointID: t.EndpointID,
		EndpointName: epName, EventType: t.EventType, EventAt: t.EventAt, Attempt: t.Attempt, Attempted: false,
		Outcome: model.OutcomeDeferred, State: state, Reason: reason, NextRetryAt: &next, WorkerID: w.id, TS: now,
	}
	return w.schedule(ctx, &t, delay, "deferred", ar)
}

func (w *Worker) schedule(ctx context.Context, t *model.DeliveryTask, delay time.Duration, why string, ar *model.AttemptRecord) error {
	tier := model.TierFor(delay)
	obs.RetriesScheduled.WithLabelValues(strconv.Itoa(tier.Tier), why).Inc()
	rr, err := kafkax.JSONRecord(ctx, tier.Topic, t.PartitionKey(), t)
	if err != nil {
		return err
	}
	st, err := kafkax.JSONRecord(ctx, model.TopicStatus, []byte(t.EventID), model.StatusMsg{Kind: "attempt", Attempt: ar})
	if err != nil {
		return err
	}
	return kafkax.ProduceSync(ctx, w.d.Producer, rr, st)
}

func (w *Worker) dead(ctx context.Context, task *model.DeliveryTask, epName, reason string, attempted bool, ar *model.AttemptRecord) error {
	now := time.Now().UTC()
	if ar == nil {
		ar = &model.AttemptRecord{
			DeliveryID: task.DeliveryID, EventID: task.EventID, TenantID: task.TenantID, EndpointID: task.EndpointID,
			EndpointName: epName, EventType: task.EventType, EventAt: task.EventAt, Attempt: task.Attempt,
			WorkerID: w.id, TS: now,
		}
	}
	ar.Attempted = attempted
	ar.Outcome, ar.State, ar.Reason, ar.NextRetryAt = model.OutcomeDead, model.StateDead, reason, nil
	obs.DLQTotal.Inc()
	obs.DeliveryAttempts.WithLabelValues(epName, model.OutcomeDead).Inc()
	dl, err := kafkax.JSONRecord(ctx, model.TopicDLQ, task.PartitionKey(), map[string]any{"task": task, "reason": reason, "dead_at": now})
	if err != nil {
		return err
	}
	st, err := kafkax.JSONRecord(ctx, model.TopicStatus, []byte(task.EventID), model.StatusMsg{Kind: "attempt", Attempt: ar})
	if err != nil {
		return err
	}
	return kafkax.ProduceSync(ctx, w.d.Producer, dl, st)
}

func (w *Worker) publish(ctx context.Context, ar *model.AttemptRecord) error {
	st, err := kafkax.JSONRecord(ctx, model.TopicStatus, []byte(ar.EventID), model.StatusMsg{Kind: "attempt", Attempt: ar})
	if err != nil {
		return err
	}
	if err := kafkax.ProduceSync(ctx, w.d.Producer, st); err != nil {
		trace.SpanFromContext(ctx).SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// jitter spreads retries ±10% so a recovering receiver isn't hit by a synchronized wave.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	f := 0.9 + rand.Float64()*0.2
	return time.Duration(float64(d) * f)
}

func (w *Worker) heartbeat(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p := w.processed.Load()
			w.writeHeartbeat(ctx, float64(p-last))
			last = p
		}
	}
}

func (w *Worker) writeHeartbeat(ctx context.Context, eps float64) {
	host, _ := os.Hostname()
	hb := stats.WorkerHeartbeat{
		ID: w.id, Host: host, Status: "running", StartedAt: w.started, LastSeen: time.Now().UTC(),
		Processed: w.processed.Load(), Inflight: w.inflight.Load(), EPS: eps, Partitions: []int32{},
	}
	if w.stopping.Load() {
		hb.Status = "stopping"
	}
	if w.gc != nil {
		for _, tp := range w.gc.Owned() {
			if tp.Topic == model.TopicDeliveries {
				hb.Partitions = append(hb.Partitions, tp.Partition)
			}
		}
	}
	b, _ := json.Marshal(hb)
	pipe := w.d.Redis.Pipeline()
	pipe.Set(ctx, stats.WorkerInfoPrefix+w.id, b, 60*time.Second)
	pipe.ZAdd(ctx, stats.WorkersSeen, redis.Z{Score: float64(time.Now().UnixMilli()), Member: w.id})
	_, _ = pipe.Exec(ctx)
}
