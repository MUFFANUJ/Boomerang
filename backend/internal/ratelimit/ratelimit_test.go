package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestTokenBucket(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	l := New(rdb)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		r, err := l.Take(ctx, "k", 100, 5, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take %d: allowed=%v err=%v", i, r.Allowed, err)
		}
	}
	r, _ := l.Take(ctx, "k", 100, 5, 1)
	if r.Allowed {
		t.Fatal("bucket should be empty after the burst")
	}
	if r.RetryAfter <= 0 || r.RetryAfter > 20*time.Millisecond {
		t.Fatalf("retry-after should be about 10ms at 100/s, got %v", r.RetryAfter)
	}
	time.Sleep(30 * time.Millisecond)
	if r, _ := l.Take(ctx, "k", 100, 5, 1); !r.Allowed {
		t.Fatal("tokens should refill over time")
	}
	if r, _ := l.Take(ctx, "k", 100, 5, 50); r.Allowed {
		t.Fatal("a request larger than the burst can never be allowed")
	}
	if r, _ := l.Take(ctx, "other", 0, 0, 1000); !r.Allowed {
		t.Fatal("rate 0 means unlimited")
	}
}
