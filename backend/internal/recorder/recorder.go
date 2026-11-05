// Package recorder streams the status topic into ClickHouse with batched inserts and keeps
// the live Redis counters up to date.
package recorder

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/chstore"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/stats"
)

// Run consumes the status stream until ctx is cancelled.
func Run(ctx context.Context, d *app.Deps) error {
	handler := func(ctx context.Context, tp kafkax.TP, recs []*kgo.Record) error {
		var events, deliveries, attempts []any
		var delivered, failed, deferred, dead int64
		// Latest state per delivery in this batch, for the DLQ membership set.
		dlqState := map[string]bool{}
		for _, r := range recs {
			var m model.StatusMsg
			if err := json.Unmarshal(r.Value, &m); err != nil {
				slog.Error("dropping malformed status message", "offset", r.Offset, "err", err)
				continue
			}
			switch {
			case m.Kind == "event" && m.Event != nil:
				er, drs := chstore.RowsFromEvent(m.Event)
				events = append(events, er)
				for _, dr := range drs {
					deliveries = append(deliveries, dr)
				}
			case m.Kind == "attempt" && m.Attempt != nil:
				ar, dr := chstore.RowsFromAttempt(m.Attempt)
				attempts = append(attempts, ar)
				deliveries = append(deliveries, dr)
				dlqState[m.Attempt.DeliveryID] = m.Attempt.State == model.StateDead
				switch m.Attempt.Outcome {
				case model.OutcomeSuccess:
					delivered++
				case model.OutcomeFailure:
					failed++
				case model.OutcomeDeferred:
					deferred++
				case model.OutcomeDead:
					dead++
					if m.Attempt.Attempted {
						failed++
					}
				}
			}
		}
		// The three tables are independent; insert them concurrently (each insert waits for
		// ClickHouse to flush its async-insert buffer, so this cuts batch latency ~3x).
		errs := make(chan error, 3)
		for table, rows := range map[string][]any{"events": events, "deliveries": deliveries, "attempts": attempts} {
			go func() { errs <- d.CH.Insert(ctx, table, rows) }()
		}
		for i := 0; i < 3; i++ {
			if err := <-errs; err != nil {
				return err
			}
		}
		pipe := d.Redis.Pipeline()
		var add, rem []any
		for id, isDead := range dlqState {
			if isDead {
				add = append(add, id)
			} else {
				rem = append(rem, id)
			}
		}
		if len(add) > 0 {
			pipe.SAdd(ctx, stats.DLQSet, add...)
		}
		if len(rem) > 0 {
			pipe.SRem(ctx, stats.DLQSet, rem...)
		}
		for k, v := range map[string]int64{stats.Delivered: delivered, stats.FailedAttempts: failed, stats.Deferred: deferred, stats.DeadTotal: dead} {
			if v > 0 {
				pipe.IncrBy(ctx, k, v)
			}
		}
		_, err := pipe.Exec(ctx)
		if err != nil && err.Error() != "redis: nil" {
			slog.Warn("counter update failed", "err", err)
		}
		return nil
	}
	gc, err := kafkax.NewGroupConsumer(kafkax.GroupOptions{
		Brokers: d.Cfg.KafkaBrokers, Group: model.GroupRecorder, Topics: []string{model.TopicStatus},
		ClientID: "recorder-" + d.Cfg.ServiceID,
		Factory:  kafkax.BatchFactory(handler, kafkax.BatchOptions{MaxBatch: 5000, MaxPending: 50000}),
	})
	if err != nil {
		return err
	}
	return gc.Run(ctx)
}
