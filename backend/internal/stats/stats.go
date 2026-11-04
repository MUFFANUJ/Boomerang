// Package stats names the shared Redis keys used for live counters, heartbeats and demo
// controls, so every service agrees on them.
package stats

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Counter keys (monotonic totals).
const (
	Received       = "stats:received"
	Delivered      = "stats:delivered"
	FailedAttempts = "stats:failed_attempts"
	Deferred       = "stats:deferred"
	DeadTotal      = "stats:dead_total"
)

// Other shared keys.
const (
	WorkersSeen      = "workers:seen" // ZSET worker id -> last seen ms
	WorkerInfoPrefix = "worker:"      // JSON heartbeat per worker
	SimStatus        = "shopsim:status"
	IntegritySince   = "integrity:since"
	InvalidateChan   = "boomerang:invalidate"
	EndpointChanged  = "boomerang:endpoint_changed"
	ChaosPrefix      = "chaos:"         // hash per receiver
	ReceiverPrefix   = "recv:"          // recv:<name>:ids (set), recv:<name>:stats (hash)
	ScalerDesired    = "scaler:desired" // last requested replica count
	DLQSet           = "dlq:deliveries" // delivery ids currently dead-lettered (maintained by the recorder)
)

// WorkerHeartbeat is written by each delivery worker.
type WorkerHeartbeat struct {
	ID         string    `json:"id"`
	Host       string    `json:"host"`
	Status     string    `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	LastSeen   time.Time `json:"last_seen"`
	Partitions []int32   `json:"partitions"`
	Processed  int64     `json:"processed_total"`
	Inflight   int64     `json:"inflight"`
	EPS        float64   `json:"eps"`
}

// Accumulator batches counter increments and flushes them periodically, keeping Redis off the
// per-request path at high throughput.
type Accumulator struct {
	rdb redis.Cmdable
	key string
	n   atomic.Int64
}

// NewAccumulator creates an accumulator for key.
func NewAccumulator(rdb redis.Cmdable, key string) *Accumulator {
	return &Accumulator{rdb: rdb, key: key}
}

// Add adds to the pending count.
func (a *Accumulator) Add(n int64) { a.n.Add(n) }

// Run flushes every interval until ctx is done (with a final flush).
func (a *Accumulator) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	flush := func(c context.Context) {
		if n := a.n.Swap(0); n != 0 {
			if err := a.rdb.IncrBy(c, a.key, n).Err(); err != nil {
				a.n.Add(n)
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			flush(c)
			cancel()
			return
		case <-t.C:
			flush(ctx)
		}
	}
}
