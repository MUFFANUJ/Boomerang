//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/config"
	"github.com/boomerang-dev/boomerang/internal/fanout"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/retry"
	"github.com/boomerang-dev/boomerang/internal/signing"
	"github.com/boomerang-dev/boomerang/internal/worker"
)

// TestPipelineRetriesUntilDelivered pushes one event through fan-out, a delivery worker and the
// retry tiers against real Kafka, Redis and Postgres. The receiver fails twice before
// accepting, verifying the HMAC signature every time.
func TestPipelineRetriesUntilDelivered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	cfg := config.Load("integration")
	cfg.KafkaBrokers = []string{startKafka(t)}
	cfg.RedisAddr = startRedis(t)
	cfg.PostgresURL = startPostgres(t)
	cfg.SSRFAllowHosts = []string{"127.0.0.1"}
	cfg.DeliveryTimeout = 2 * time.Second
	cfg.ServiceID = "it-worker"

	if err := retryUntil(ctx, func() error { return kafkax.EnsureTopics(ctx, cfg.KafkaBrokers) }); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	d, err := app.Open(ctx, cfg, app.Need{Cache: true, Producer: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.Store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	// Receiver: fail twice, then accept. Every request must carry a valid signature.
	var mu sync.Mutex
	var calls []int
	var secret string
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		err := signing.Verify([]string{secret}, r.Header.Get(signing.HeaderID), r.Header.Get(signing.HeaderTimestamp),
			r.Header.Get(signing.HeaderSignature), body, time.Now(), signing.DefaultTolerance)
		if err != nil {
			t.Errorf("receiver got an invalid signature: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		calls = append(calls, len(calls)+1)
		if len(calls) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer recv.Close()

	ten, err := d.Store.CreateTenant(ctx, &model.Tenant{Name: "IT", RateLimitRPS: 100, RateLimitBurst: 100, RetryMaxAttempts: 3, RetryScheduleMs: []int{300, 300}})
	if err != nil {
		t.Fatal(err)
	}
	ep, err := d.Store.CreateEndpoint(ctx, &model.Endpoint{TenantID: ten.ID, Name: "Orders", URL: recv.URL + "/hook", EventTypes: []string{"order.*"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Store.CreateEndpoint(ctx, &model.Endpoint{TenantID: ten.ID, Name: "Payments", URL: recv.URL + "/payments", EventTypes: []string{"payment.*"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	secret = ep.Secret
	mu.Unlock()

	// Watch the status stream from the start.
	status, err := kgo.NewClient(kgo.SeedBrokers(cfg.KafkaBrokers...), kgo.ConsumeTopics(model.TopicStatus),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer status.Close()

	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, run := range []func(context.Context) error{
		func(c context.Context) error { return fanout.Run(c, d) },
		func(c context.Context) error { return worker.New(d).Run(c) },
		func(c context.Context) error { return retry.Run(c, d) },
	} {
		wg.Add(1)
		go func() { defer wg.Done(); _ = run(runCtx) }()
	}
	defer func() { stop(); wg.Wait() }()

	ev := model.EventEnvelope{
		EventID: model.NewID("evt"), TenantID: ten.ID, EventType: "order.created", CreatedAt: time.Now().UTC(),
		Body: json.RawMessage(`{"id":"x","type":"order.created","data":{"order_id":"ord_1"}}`), BodyBytes: 60,
	}
	rec, _ := kafkax.JSONRecord(ctx, model.TopicEvents, []byte(ev.EventID), ev)
	if err := kafkax.ProduceSync(ctx, d.Producer, rec); err != nil {
		t.Fatal(err)
	}

	var event *model.EventRecord
	var attempts []*model.AttemptRecord
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := status.PollFetches(pctx)
		pcancel()
		fetches.EachRecord(func(r *kgo.Record) {
			var m model.StatusMsg
			if json.Unmarshal(r.Value, &m) != nil {
				return
			}
			if m.Event != nil && m.Event.EventID == ev.EventID {
				event = m.Event
			}
			if m.Attempt != nil && m.Attempt.EventID == ev.EventID {
				attempts = append(attempts, m.Attempt)
			}
		})
		if n := len(attempts); n > 0 && attempts[n-1].Outcome == model.OutcomeSuccess {
			break
		}
	}

	if event == nil || len(event.Deliveries) != 1 || event.Deliveries[0].EndpointID != ep.ID {
		t.Fatalf("fan-out should create exactly one delivery (to the order.* endpoint), got %+v", event)
	}
	var real []*model.AttemptRecord
	for _, a := range attempts {
		if a.Attempted {
			real = append(real, a)
		}
	}
	if len(real) != 3 {
		t.Fatalf("expected 3 attempts (fail, fail, success), got %d: %+v", len(real), attempts)
	}
	for i, want := range []string{model.OutcomeFailure, model.OutcomeFailure, model.OutcomeSuccess} {
		if real[i].Outcome != want || real[i].Attempt != i+1 {
			t.Errorf("attempt %d: got outcome %s attempt %d, want %s", i+1, real[i].Outcome, real[i].Attempt, want)
		}
	}
	if real[0].NextRetryAt == nil || real[0].StatusCode != http.StatusServiceUnavailable {
		t.Errorf("failed attempt should record status and next retry time: %+v", real[0])
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 3 {
		t.Fatalf("receiver should see exactly 3 requests, saw %d", len(calls))
	}
}

func retryUntil(ctx context.Context, fn func() error) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return err
}
