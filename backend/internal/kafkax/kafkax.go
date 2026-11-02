// Package kafkax wraps franz-go with Boomerang's conventions: topic layout, a JSON producer,
// trace propagation through record headers, and a group consumer that runs one goroutine per
// assigned partition and commits only processed offsets (at-least-once).
package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/boomerang-dev/boomerang/internal/model"
)

// TopicSpec describes a topic Boomerang needs.
type TopicSpec struct {
	Name       string
	Partitions int32
	Configs    map[string]*string
}

func str(s string) *string { return &s }

// Topics returns every topic with its partition count.
func Topics() []TopicSpec {
	t := []TopicSpec{
		{model.TopicEvents, 12, map[string]*string{"retention.ms": str("604800000")}},
		{model.TopicDeliveries, 12, map[string]*string{"retention.ms": str("604800000")}},
		{model.TopicStatus, 12, map[string]*string{"retention.ms": str("86400000")}},
		{model.TopicDLQ, 3, map[string]*string{"retention.ms": str("2592000000")}},
	}
	for _, r := range model.RetryTiers {
		t = append(t, TopicSpec{r.Topic, 6, map[string]*string{"retention.ms": str("604800000")}})
	}
	return t
}

// EnsureTopics creates missing topics. It is safe to call repeatedly.
func EnsureTopics(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, t := range Topics() {
		resp, err := adm.CreateTopics(ctx, t.Partitions, 1, t.Configs, t.Name)
		if err != nil {
			return fmt.Errorf("create topic %s: %w", t.Name, err)
		}
		for _, r := range resp {
			if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
				return fmt.Errorf("create topic %s: %w (%s)", r.Topic, r.Err, r.ErrMessage)
			}
		}
	}
	return nil
}

// NewProducer returns a client tuned for batched, idempotent produces.
func NewProducer(brokers []string, clientID string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.MaxBufferedRecords(20_000),
		kgo.RecordRetries(20),
	)
}

// JSONRecord builds a record with a JSON value and propagates the trace context from ctx.
func JSONRecord(ctx context.Context, topic string, key []byte, v any) (*kgo.Record, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	r := &kgo.Record{Topic: topic, Key: key, Value: b}
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier{r})
	return r, nil
}

// ExtractContext returns a context carrying the trace stored in a record's headers.
func ExtractContext(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier{r})
}

// HeaderCarrier adapts record headers to an OpenTelemetry TextMapCarrier.
type HeaderCarrier struct{ R *kgo.Record }

// Get returns a header value.
func (c HeaderCarrier) Get(key string) string {
	for _, h := range c.R.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set sets a header value.
func (c HeaderCarrier) Set(key, val string) {
	for i, h := range c.R.Headers {
		if h.Key == key {
			c.R.Headers[i].Value = []byte(val)
			return
		}
	}
	c.R.Headers = append(c.R.Headers, kgo.RecordHeader{Key: key, Value: []byte(val)})
}

// Keys lists header keys.
func (c HeaderCarrier) Keys() []string {
	out := make([]string, 0, len(c.R.Headers))
	for _, h := range c.R.Headers {
		out = append(out, h.Key)
	}
	return out
}

// ProduceJSON synchronously produces a batch of values. All records must succeed.
func ProduceSync(ctx context.Context, cl *kgo.Client, recs ...*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	return cl.ProduceSync(ctx, recs...).FirstErr()
}

// Logger helper for consumer code.
func logErr(msg string, err error, args ...any) {
	slog.Error(msg, append(args, "err", err)...)
}
