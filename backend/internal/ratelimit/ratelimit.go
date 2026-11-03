// Package ratelimit implements a distributed token bucket in Redis. All replicas of a service
// share one bucket per key, so limits hold no matter how far the service is scaled out.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// BucketScript takes tokens from a bucket.
// KEYS[1] bucket key; ARGV: rate (tokens/s), burst, now (ms), requested tokens.
// Returns {allowed (0/1), wait_ms until enough tokens, remaining tokens (floor)}.
const BucketScript = `
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local req = tonumber(ARGV[4])
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
if now > ts then tokens = math.min(burst, tokens + (now - ts) * rate / 1000) end
local allowed = 0
local wait = 0
if tokens >= req then
  tokens = tokens - req
  allowed = 1
else
  wait = math.ceil((req - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(math.max(now, ts)))
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000 / rate) + 1000)
return {allowed, wait, math.floor(tokens)}
`

var bucket = redis.NewScript(BucketScript)

// Limiter is a token bucket limiter.
type Limiter struct{ rdb redis.Scripter }

// New creates a limiter.
func New(rdb redis.Scripter) *Limiter { return &Limiter{rdb: rdb} }

// Result of a Take.
type Result struct {
	Allowed    bool
	RetryAfter time.Duration
	Remaining  int64
}

// Take removes n tokens from key's bucket.
func (l *Limiter) Take(ctx context.Context, key string, rate, burst float64, n int) (Result, error) {
	if rate <= 0 {
		return Result{Allowed: true}, nil
	}
	if burst < 1 {
		burst = 1
	}
	res, err := bucket.Run(ctx, l.rdb, []string{key}, rate, burst, time.Now().UnixMilli(), n).Int64Slice()
	if err != nil {
		return Result{}, err
	}
	return Result{Allowed: res[0] == 1, RetryAfter: time.Duration(res[1]) * time.Millisecond, Remaining: res[2]}, nil
}
