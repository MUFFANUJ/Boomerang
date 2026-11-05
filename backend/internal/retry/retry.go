// Package retry runs the tiered retry topics. Each tier holds tasks whose delays fall in its
// band, so a tier's backlog is roughly ordered by due time. The service waits until each task
// is due and then puts it back on the main delivery topic. Offsets are committed only after
// the task has been re-published, so a crash re-sends tasks rather than losing them.
package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
)

// Run consumes every tier (one consumer group per tier, so each tier's depth is visible as
// its own consumer lag) until ctx is cancelled.
func Run(ctx context.Context, d *app.Deps) error {
	var wg sync.WaitGroup
	errc := make(chan error, len(model.RetryTiers))
	for _, tier := range model.RetryTiers {
		gc, err := kafkax.NewGroupConsumer(kafkax.GroupOptions{
			Brokers: d.Cfg.KafkaBrokers, Group: fmt.Sprintf(model.GroupRetryFmt, tier.Tier), Topics: []string{tier.Topic},
			ClientID: fmt.Sprintf("retry-%d-%s", tier.Tier, d.Cfg.ServiceID),
			Factory:  kafkax.BatchFactory(handler(d), kafkax.BatchOptions{MaxBatch: 1000, MaxPending: 20000}),
		})
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := gc.Run(ctx); err != nil {
				errc <- err
			}
		}()
	}
	wg.Wait()
	close(errc)
	return <-errc
}

type pending struct {
	rec  *kgo.Record
	task model.DeliveryTask
}

func handler(d *app.Deps) kafkax.BatchHandler {
	return func(ctx context.Context, tp kafkax.TP, recs []*kgo.Record) error {
		items := make([]pending, 0, len(recs))
		for _, r := range recs {
			var t model.DeliveryTask
			if err := json.Unmarshal(r.Value, &t); err != nil {
				slog.Error("dropping malformed retry task", "topic", tp.Topic, "offset", r.Offset, "err", err)
				continue
			}
			items = append(items, pending{r, t})
		}
		// Release in due-time order within the batch.
		sort.SliceStable(items, func(i, j int) bool { return items[i].task.NotBefore.Before(items[j].task.NotBefore) })
		var due []*kgo.Record
		flush := func() error {
			err := kafkax.ProduceSync(ctx, d.Producer, due...)
			due = due[:0]
			return err
		}
		for _, it := range items {
			if wait := time.Until(it.task.NotBefore); wait > 0 {
				// Forward everything already due before sleeping.
				if err := flush(); err != nil {
					return err
				}
				t := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					t.Stop()
					return ctx.Err()
				case <-t.C:
				}
			}
			t := it.task
			t.NotBefore = time.Time{}
			r := &kgo.Record{Topic: model.TopicDeliveries, Key: t.PartitionKey(), Headers: it.rec.Headers}
			b, err := json.Marshal(t)
			if err != nil {
				return err
			}
			r.Value = b
			due = append(due, r)
			if len(due) >= 200 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return flush()
	}
}
