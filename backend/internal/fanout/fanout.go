// Package fanout turns each ingested event into one delivery task per subscribed endpoint.
// Tasks are keyed by endpoint (plus ordering key), so an endpoint's deliveries for one
// ordering key always land on the same partition, in order.
package fanout

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/obs"
	"github.com/boomerang-dev/boomerang/internal/store"
)

// Run consumes events until ctx is cancelled.
func Run(ctx context.Context, d *app.Deps) error {
	tracer := obs.Tracer("fanout")
	handler := func(ctx context.Context, tp kafkax.TP, recs []*kgo.Record) error {
		var out []*kgo.Record
		for _, r := range recs {
			var ev model.EventEnvelope
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				slog.Error("dropping malformed event", "offset", r.Offset, "err", err)
				continue
			}
			rctx, span := tracer.Start(kafkax.ExtractContext(ctx, r), "fanout",
				trace.WithAttributes(attribute.String("event.id", ev.EventID), attribute.String("event.type", ev.EventType)))
			recsForEvent, err := fanOut(rctx, d, &ev)
			span.End()
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					slog.Warn("dropping event for unknown tenant", "tenant", ev.TenantID, "event", ev.EventID)
					continue
				}
				return err // retried by the batch partition; nothing has been produced yet
			}
			out = append(out, recsForEvent...)
		}
		// One synchronous produce for the whole batch; offsets are committed only after
		// every delivery task is durably in Kafka.
		return kafkax.ProduceSync(ctx, d.Producer, out...)
	}
	gc, err := kafkax.NewGroupConsumer(kafkax.GroupOptions{
		Brokers: d.Cfg.KafkaBrokers, Group: model.GroupFanout, Topics: []string{model.TopicEvents},
		ClientID: "fanout-" + d.Cfg.ServiceID,
		Factory:  kafkax.BatchFactory(handler, kafkax.BatchOptions{MaxBatch: 500}),
	})
	if err != nil {
		return err
	}
	return gc.Run(ctx)
}

func fanOut(ctx context.Context, d *app.Deps, ev *model.EventEnvelope) ([]*kgo.Record, error) {
	tenant, err := d.Cache.Tenant(ctx, ev.TenantID)
	if err != nil {
		return nil, err
	}
	subs, err := d.Cache.Subscribers(ctx, ev.TenantID)
	if err != nil {
		return nil, err
	}
	rec := &model.EventRecord{
		EventID: ev.EventID, TenantID: ev.TenantID, EventType: ev.EventType, CreatedAt: ev.CreatedAt,
		Body: ev.Body, BodyRef: ev.BodyRef, BodyBytes: ev.BodyBytes,
		IdempotencyKey: ev.IdempotencyKey, OrderingKey: ev.OrderingKey, Deliveries: []model.DeliveryRef{},
	}
	var out []*kgo.Record
	for _, s := range subs {
		ep := model.Endpoint{EventTypes: s.EventTypes}
		if !ep.Subscribes(ev.EventType) {
			continue
		}
		task := model.DeliveryTask{
			DeliveryID: model.DeliveryID(ev.EventID, s.ID), EventID: ev.EventID, TenantID: ev.TenantID,
			EndpointID: s.ID, EventType: ev.EventType, OrderingKey: ev.OrderingKey,
			Attempt: 1, MaxAttempts: max(1, tenant.RetryMaxAttempts), EventAt: ev.CreatedAt,
			Body: ev.Body, BodyRef: ev.BodyRef,
		}
		r, err := kafkax.JSONRecord(ctx, model.TopicDeliveries, task.PartitionKey(), task)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
		rec.Deliveries = append(rec.Deliveries, model.DeliveryRef{DeliveryID: task.DeliveryID, EndpointID: s.ID, EndpointName: s.Name})
	}
	obs.DeliveriesCreated.Add(float64(len(rec.Deliveries)))
	st, err := kafkax.JSONRecord(ctx, model.TopicStatus, []byte(ev.EventID), model.StatusMsg{Kind: "event", Event: rec})
	if err != nil {
		return nil, err
	}
	return append(out, st), nil
}
