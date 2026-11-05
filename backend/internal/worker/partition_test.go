package worker

import (
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The tracked partition must only commit offsets whose predecessors are all finished, and
// must keep tasks with the same key in order.
func TestCommitWatermarkAndKeyOrdering(t *testing.T) {
	release := map[int64]chan struct{}{}
	for i := int64(0); i < 4; i++ {
		release[i] = make(chan struct{})
	}
	var mu sync.Mutex
	var order []int64
	tp := &trackedPartition{keys: map[string]*keyQueue{}, maxPending: 100}
	tp.process = func(r *kgo.Record) {
		<-release[r.Offset]
		mu.Lock()
		order = append(order, r.Offset)
		mu.Unlock()
	}
	// Offsets 0 and 2 share a key; 1 and 3 have their own keys.
	recs := []*kgo.Record{
		{Offset: 0, Key: []byte("a")}, {Offset: 1, Key: []byte("b")},
		{Offset: 2, Key: []byte("a")}, {Offset: 3, Key: []byte("c")},
	}
	tp.ctl = nil
	tp.mu.Lock()
	for _, r := range recs {
		it := &item{rec: r}
		tp.pending = append(tp.pending, it)
		k := string(r.Key)
		if q, ok := tp.keys[k]; ok {
			q.items = append(q.items, it)
			continue
		}
		tp.keys[k] = &keyQueue{}
		tp.wg.Add(1)
		go tp.runKey(k, it)
	}
	tp.mu.Unlock()

	waitDone := func(off int64) {
		for {
			tp.mu.Lock()
			done := false
			for _, it := range tp.pending {
				if it.rec.Offset == off {
					done = it.done
				}
			}
			tp.mu.Unlock()
			if done {
				return
			}
		}
	}

	close(release[3])
	waitDone(3)
	close(release[1])
	waitDone(1)
	if rec := peekCommittable(tp); rec != nil {
		t.Fatalf("nothing is committable while offset 0 is in flight, got %d", rec.Offset)
	}
	close(release[0])
	close(release[2])
	tp.wg.Wait()
	rec := peekCommittable(tp)
	if rec == nil || rec.Offset != 3 {
		t.Fatalf("expected watermark at offset 3, got %v", rec)
	}
	// Same-key tasks ran in offset order.
	pos := map[int64]int{}
	for i, o := range order {
		pos[o] = i
	}
	if pos[0] > pos[2] {
		t.Fatalf("key a must process offset 0 before 2, order=%v", order)
	}
}

// peekCommittable is committable() without touching the (nil) Kafka control.
func peekCommittable(t *trackedPartition) *kgo.Record {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for n < len(t.pending) && t.pending[n].done {
		n++
	}
	if n == 0 {
		return nil
	}
	return t.pending[n-1].rec
}
