package kafkax

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TP identifies a topic partition.
type TP struct {
	Topic     string
	Partition int32
}

// Partition processes the records of one assigned partition. Add must never block.
// Stop must stop processing and, when commit is true, commit whatever has been processed.
type Partition interface {
	Add(recs []*kgo.Record)
	Stop(commit bool)
}

// Watermarked partitions expose their committable progress; the group consumer commits every
// such partition in one request on a timer instead of one request per partition.
type Watermarked interface {
	// Committable returns the last record whose offset (and all before it) is done, or nil.
	Committable() *kgo.Record
}

// Factory creates the processor for a newly assigned partition.
type Factory func(ctl *Control) Partition

// Control gives a partition processor access to its client: committing and flow control.
type Control struct {
	Client    *kgo.Client
	Topic     string
	Partition int32
	mu        sync.Mutex
	paused    bool
}

// Pause stops fetching this partition (backpressure).
func (c *Control) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.paused {
		c.paused = true
		c.Client.PauseFetchPartitions(map[string][]int32{c.Topic: {c.Partition}})
	}
}

// Resume resumes fetching this partition.
func (c *Control) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused {
		c.paused = false
		c.Client.ResumeFetchPartitions(map[string][]int32{c.Topic: {c.Partition}})
	}
}

// Commit commits the offset after rec (and implicitly everything before it on the partition).
func (c *Control) Commit(ctx context.Context, rec *kgo.Record) {
	if rec == nil {
		return
	}
	if err := c.Client.CommitRecords(ctx, rec); err != nil {
		slog.Warn("commit failed", "topic", c.Topic, "partition", c.Partition, "offset", rec.Offset, "err", err)
	}
}

// GroupConsumer consumes topics as part of a consumer group.
type GroupConsumer struct {
	Client  *kgo.Client
	factory Factory
	mu      sync.Mutex
	parts   map[TP]Partition
	ctls    map[TP]*Control
	// commitMu orders batched commits with revocation so a partition is never committed
	// after it has been handed to another member.
	commitMu sync.Mutex
	unacked  map[TP]*kgo.Record // watermarks whose commit failed; retried on the next tick
	// OnAssignmentChange is called (outside locks) whenever the owned partitions change.
	OnAssignmentChange func(owned []TP)
}

// GroupOptions configures a group consumer.
type GroupOptions struct {
	Brokers  []string
	Group    string
	Topics   []string
	ClientID string
	Factory  Factory
	Extra    []kgo.Opt
}

// NewGroupConsumer builds a consumer. Call Run to start it.
func NewGroupConsumer(o GroupOptions) (*GroupConsumer, error) {
	g := &GroupConsumer{factory: o.Factory, parts: map[TP]Partition{}, ctls: map[TP]*Control{}}
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.Brokers...),
		kgo.ClientID(o.ClientID),
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(o.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(1 * time.Second),
		kgo.FetchMaxWait(250 * time.Millisecond),
		kgo.FetchMaxBytes(4 << 20),
		kgo.FetchMaxPartitionBytes(1 << 20),
		kgo.OnPartitionsAssigned(g.assigned),
		kgo.OnPartitionsRevoked(g.revoked),
		kgo.OnPartitionsLost(g.lost),
	}
	opts = append(opts, o.Extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	g.Client = cl
	return g, nil
}

func (g *GroupConsumer) assigned(_ context.Context, cl *kgo.Client, m map[string][]int32) {
	g.mu.Lock()
	for topic, ps := range m {
		for _, p := range ps {
			tp := TP{topic, p}
			ctl := &Control{Client: cl, Topic: topic, Partition: p}
			g.ctls[tp] = ctl
			g.parts[tp] = g.factory(ctl)
		}
	}
	g.mu.Unlock()
	slog.Info("partitions assigned", "assigned", m)
	g.notify()
}

func (g *GroupConsumer) stopPartitions(m map[string][]int32, commit bool) {
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	var wg sync.WaitGroup
	g.mu.Lock()
	for topic, ps := range m {
		for _, p := range ps {
			tp := TP{topic, p}
			part, ok := g.parts[tp]
			if !ok {
				continue
			}
			ctl := g.ctls[tp]
			delete(g.parts, tp)
			delete(g.ctls, tp)
			if r, ok := g.unacked[tp]; ok && commit {
				_ = g.Client.CommitRecords(context.Background(), r)
			}
			delete(g.unacked, tp)
			wg.Add(1)
			go func() {
				defer wg.Done()
				part.Stop(commit)
				ctl.Resume() // never leave a partition paused for its next owner in this client
			}()
		}
	}
	g.mu.Unlock()
	wg.Wait()
	g.notify()
}

func (g *GroupConsumer) revoked(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	slog.Info("partitions revoked", "revoked", m)
	g.stopPartitions(m, true)
}

func (g *GroupConsumer) lost(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	slog.Warn("partitions lost", "lost", m)
	g.stopPartitions(m, false)
}

func (g *GroupConsumer) notify() {
	if g.OnAssignmentChange == nil {
		return
	}
	g.OnAssignmentChange(g.Owned())
}

// Owned returns the currently owned partitions.
func (g *GroupConsumer) Owned() []TP {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]TP, 0, len(g.parts))
	for tp := range g.parts {
		out = append(out, tp)
	}
	return out
}

// commitWatermarks commits every watermarked partition's progress in one request.
func (g *GroupConsumer) commitWatermarks() {
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	g.mu.Lock()
	if g.unacked == nil {
		g.unacked = map[TP]*kgo.Record{}
	}
	for tp, p := range g.parts {
		if wm, ok := p.(Watermarked); ok {
			if r := wm.Committable(); r != nil {
				g.unacked[tp] = r // newer watermark supersedes an older failed one
			}
		}
	}
	recs := make([]*kgo.Record, 0, len(g.unacked))
	for tp, r := range g.unacked {
		if _, owned := g.parts[tp]; owned {
			recs = append(recs, r)
		}
	}
	g.mu.Unlock()
	if len(recs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := g.Client.CommitRecords(ctx, recs...); err != nil {
		slog.Warn("batched commit failed, retrying next tick", "partitions", len(recs), "err", err)
		return
	}
	g.mu.Lock()
	g.unacked = map[TP]*kgo.Record{}
	g.mu.Unlock()
}

// Run polls until ctx is cancelled, then drains every partition, commits and leaves the group.
func (g *GroupConsumer) Run(ctx context.Context) error {
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				g.commitWatermarks()
			}
		}
	}()
	defer func() { <-commitDone }()
	for {
		fetches := g.Client.PollRecords(ctx, 2000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			g.Client.AllowRebalance()
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Warn("fetch error", "topic", t, "partition", p, "err", err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			g.mu.Lock()
			part := g.parts[TP{p.Topic, p.Partition}]
			g.mu.Unlock()
			if part != nil {
				part.Add(p.Records)
			}
		})
		g.Client.AllowRebalance()
	}
	// Graceful shutdown: finish in-flight work and commit before leaving the group so the
	// next owner starts exactly where we stopped.
	g.mu.Lock()
	all := map[string][]int32{}
	for tp := range g.parts {
		all[tp.Topic] = append(all[tp.Topic], tp.Partition)
	}
	g.mu.Unlock()
	g.stopPartitions(all, true)
	g.Client.Close()
	slog.Info("consumer closed")
	return nil
}

// BatchHandler processes a batch of records from one partition. Returning an error retries the
// same batch (with backoff) until it succeeds or the partition is stopped.
type BatchHandler func(ctx context.Context, tp TP, recs []*kgo.Record) error

// BatchOptions configures a sequential batch partition.
type BatchOptions struct {
	MaxBatch   int // records handed to the handler at once
	MaxPending int // queue size at which fetching is paused
}

// BatchFactory processes each partition sequentially, in batches, committing after each batch.
func BatchFactory(h BatchHandler, o BatchOptions) Factory {
	if o.MaxBatch <= 0 {
		o.MaxBatch = 500
	}
	if o.MaxPending <= 0 {
		o.MaxPending = 5000
	}
	return func(ctl *Control) Partition {
		ctx, cancel := context.WithCancel(context.Background())
		bp := &batchPartition{ctl: ctl, h: h, o: o, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{})}
		go bp.loop()
		return bp
	}
}

type batchPartition struct {
	ctl    *Control
	h      BatchHandler
	o      BatchOptions
	mu     sync.Mutex
	queue  []*kgo.Record
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	commit bool
}

func (b *batchPartition) Add(recs []*kgo.Record) {
	b.mu.Lock()
	b.queue = append(b.queue, recs...)
	n := len(b.queue)
	b.mu.Unlock()
	if n >= b.o.MaxPending {
		b.ctl.Pause()
	}
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *batchPartition) Stop(commit bool) {
	b.mu.Lock()
	b.commit = commit
	b.mu.Unlock()
	b.cancel()
	<-b.done
}

func (b *batchPartition) loop() {
	defer close(b.done)
	tp := TP{b.ctl.Topic, b.ctl.Partition}
	for {
		b.mu.Lock()
		n := len(b.queue)
		if n > b.o.MaxBatch {
			n = b.o.MaxBatch
		}
		batch := b.queue[:n:n]
		b.queue = b.queue[n:]
		remaining := len(b.queue)
		b.mu.Unlock()
		if remaining < b.o.MaxPending/2 {
			b.ctl.Resume()
		}
		if len(batch) == 0 {
			select {
			case <-b.ctx.Done():
				return
			case <-b.wake:
				continue
			}
		}
		backoff := 200 * time.Millisecond
		for {
			err := b.h(b.ctx, tp, batch)
			if err == nil {
				// Commit with a fresh context: the partition context may already be
				// cancelled during a graceful revoke, and the batch is fully processed.
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				b.ctl.Commit(cctx, batch[len(batch)-1])
				cancel()
				break
			}
			if b.ctx.Err() != nil {
				return // stopping: the unprocessed batch is redelivered to the next owner
			}
			logErr("batch handler failed, retrying", err, "topic", tp.Topic, "partition", tp.Partition)
			select {
			case <-b.ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
		if b.ctx.Err() != nil {
			return
		}
	}
}
