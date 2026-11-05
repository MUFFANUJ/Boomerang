package worker

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/kafkax"
)

// trackedPartition delivers a partition's tasks concurrently while keeping tasks that share a
// key (endpoint + ordering key) strictly sequential. Offsets are committed up to the highest
// offset below which every task has finished, so a crash only ever re-delivers work
// (at-least-once), never skips it.
type trackedPartition struct {
	ctl        *kafkax.Control
	process    func(*kgo.Record)
	maxPending int

	mu       sync.Mutex
	pending  []*item // uncommitted, offset order
	keys     map[string]*keyQueue
	stopping bool
	wg       sync.WaitGroup
}

type item struct {
	rec  *kgo.Record
	done bool
}

type keyQueue struct{ items []*item }

func newTrackedPartition(ctl *kafkax.Control, maxPending int, process func(*kgo.Record)) *trackedPartition {
	return &trackedPartition{ctl: ctl, process: process, maxPending: maxPending, keys: map[string]*keyQueue{}}
}

func (t *trackedPartition) Add(recs []*kgo.Record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopping {
		return
	}
	for _, r := range recs {
		it := &item{rec: r}
		t.pending = append(t.pending, it)
		k := string(r.Key)
		if q, ok := t.keys[k]; ok {
			q.items = append(q.items, it)
			continue
		}
		t.keys[k] = &keyQueue{}
		t.wg.Add(1)
		go t.runKey(k, it)
	}
	if len(t.pending) >= t.maxPending {
		t.ctl.Pause()
	}
}

func (t *trackedPartition) runKey(k string, it *item) {
	defer t.wg.Done()
	for {
		t.process(it.rec)
		t.mu.Lock()
		it.done = true
		q := t.keys[k]
		if t.stopping || len(q.items) == 0 {
			delete(t.keys, k)
			t.mu.Unlock()
			return
		}
		it = q.items[0]
		q.items = q.items[1:]
		t.mu.Unlock()
	}
}

// committable pops the finished prefix and returns its last record.
func (t *trackedPartition) committable() *kgo.Record {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for n < len(t.pending) && t.pending[n].done {
		n++
	}
	if n == 0 {
		return nil
	}
	last := t.pending[n-1].rec
	t.pending = t.pending[n:]
	if !t.stopping && len(t.pending) < t.maxPending/2 {
		t.ctl.Resume()
	}
	return last
}

// Committable implements kafkax.Watermarked; the group consumer batches the commits.
func (t *trackedPartition) Committable() *kgo.Record { return t.committable() }

// Stop stops dispatching new tasks, waits for in-flight deliveries to finish and, if commit is
// set, commits everything that completed.
func (t *trackedPartition) Stop(commit bool) {
	t.mu.Lock()
	t.stopping = true
	t.mu.Unlock()
	t.wg.Wait()
	if commit {
		if rec := t.committable(); rec != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t.ctl.Commit(ctx, rec)
			cancel()
		}
	}
}
