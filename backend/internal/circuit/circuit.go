// Package circuit protects receivers. Before each delivery the worker asks Guard.Before, which
// checks the endpoint's circuit breaker and its adaptive rate limiter in one atomic Redis call.
// After the attempt, Guard.After records the outcome, opening or closing the circuit and
// adjusting the endpoint's delivery rate (AIMD: halve on overload signals, creep back up on
// success). State lives in Redis so every worker sees the same breaker.
package circuit

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// States.
const (
	Closed   = "closed"
	Open     = "open"
	HalfOpen = "half_open"
)

// Decisions returned by Before.
const (
	Allow       = "ok"
	DenyOpen    = "circuit_open"
	DenyProbe   = "probe_in_flight"
	DenyLimited = "rate_limited"
)

// beforeScript: KEYS cb, bucket, aimd. ARGV now_ms, cooldown_ms, probe_lease_ms, max_rate, burst,
// max_wait_ms. Returns {decision, wait_ms, cb_state, effective_rate*1000}. For "ok", wait_ms is
// how long the caller must wait for its reserved rate-limit slot (0 if none).
const beforeScript = `
local now = tonumber(ARGV[1])
local cooldown = tonumber(ARGV[2])
local lease = tonumber(ARGV[3])
local maxRate = tonumber(ARGV[4])
local burst = tonumber(ARGV[5])
local maxWait = tonumber(ARGV[6])
local st = redis.call('HGET', KEYS[1], 'state') or 'closed'
if st == 'open' then
  local opened = tonumber(redis.call('HGET', KEYS[1], 'opened_at') or '0')
  if now - opened < cooldown then return {'circuit_open', opened + cooldown - now, 'open', 0} end
  st = 'half_open'
  redis.call('HSET', KEYS[1], 'state', 'half_open', 'probe_until', tostring(now + lease))
elseif st == 'half_open' then
  local pu = tonumber(redis.call('HGET', KEYS[1], 'probe_until') or '0')
  if now <= pu then return {'probe_in_flight', pu - now, 'half_open', 0} end
  redis.call('HSET', KEYS[1], 'probe_until', tostring(now + lease))
end
local rate = 0
if maxRate > 0 then
  rate = tonumber(redis.call('HGET', KEYS[3], 'rate') or tostring(maxRate))
  if rate > maxRate then rate = maxRate end
  local b = redis.call('HMGET', KEYS[2], 'tokens', 'ts')
  local tokens = tonumber(b[1]) or burst
  local ts = tonumber(b[2]) or now
  if now > ts then tokens = math.min(burst, tokens + (now - ts) * rate / 1000) end
  -- Reservation (GCRA-style): the bucket may go negative down to maxWait worth of tokens.
  -- Each caller takes its own future slot and is told exactly how long to wait for it, so
  -- many waiters never spin on the limiter.
  local floor = -(maxWait / 1000) * rate
  if tokens - 1 < floor then
    redis.call('HSET', KEYS[2], 'tokens', tostring(tokens), 'ts', tostring(math.max(now, ts)))
    redis.call('PEXPIRE', KEYS[2], 60000)
    return {'rate_limited', math.ceil((floor - (tokens - 1)) * 1000 / rate), st, math.floor(rate * 1000)}
  end
  tokens = tokens - 1
  redis.call('HSET', KEYS[2], 'tokens', tostring(tokens), 'ts', tostring(math.max(now, ts)))
  redis.call('PEXPIRE', KEYS[2], 60000)
  if tokens < 0 then
    return {'ok', math.ceil(-tokens * 1000 / rate), st, math.floor(rate * 1000)}
  end
end
return {'ok', 0, st, math.floor(rate * 1000)}
`

// afterScript: KEYS cb, aimd. ARGV now_ms, success(0/1), overload(0/1), consecutive_threshold,
// max_rate, min_rate, min_volume, failure_rate_threshold, half_life_ms.
// The breaker trips when the time-decayed failure rate reaches the threshold and there is
// either enough volume to trust it or a run of consecutive failures (for quiet endpoints).
// This keeps a merely flaky receiver (say 20% errors) from tripping on an unlucky streak.
// Returns {new_state, changed(0/1), consecutive_failures}.
const afterScript = `
local now = tonumber(ARGV[1])
local success = ARGV[2] == '1'
local overload = ARGV[3] == '1'
local threshold = tonumber(ARGV[4])
local maxRate = tonumber(ARGV[5])
local minRate = tonumber(ARGV[6])
local minVolume = tonumber(ARGV[7])
local failRate = tonumber(ARGV[8])
local halfLife = tonumber(ARGV[9])
local st = redis.call('HGET', KEYS[1], 'state') or 'closed'
local changed = 0
if maxRate > 0 then
  local rate = tonumber(redis.call('HGET', KEYS[2], 'rate') or tostring(maxRate))
  if success then
    rate = math.min(maxRate, rate + math.max(1, maxRate / 100))
  elseif overload then
    local last = tonumber(redis.call('HGET', KEYS[2], 'last_dec') or '0')
    if now - last >= 1000 then
      rate = math.max(minRate, rate * 0.5)
      redis.call('HSET', KEYS[2], 'last_dec', tostring(now))
    end
  end
  redis.call('HSET', KEYS[2], 'rate', tostring(rate))
  redis.call('PEXPIRE', KEYS[2], 3600000)
end
local ok = tonumber(redis.call('HGET', KEYS[1], 'ok') or '0')
local bad = tonumber(redis.call('HGET', KEYS[1], 'bad') or '0')
local wts = tonumber(redis.call('HGET', KEYS[1], 'wts') or tostring(now))
local decay = 0.5 ^ (math.max(0, now - wts) / halfLife)
ok = ok * decay
bad = bad * decay
if success then
  if st ~= 'closed' then
    changed = 1
    -- Start the recovered endpoint with a clean slate.
    redis.call('HSET', KEYS[1], 'state', 'closed', 'failures', '0', 'closed_at', tostring(now), 'ok', '1', 'bad', '0', 'wts', tostring(now))
  else
    redis.call('HSET', KEYS[1], 'failures', '0', 'ok', tostring(ok + 1), 'bad', tostring(bad), 'wts', tostring(now))
  end
  return {'closed', changed, 0}
end
bad = bad + 1
local fails = redis.call('HINCRBY', KEYS[1], 'failures', 1)
redis.call('HSET', KEYS[1], 'ok', tostring(ok), 'bad', tostring(bad), 'wts', tostring(now))
local vol = ok + bad
local trip = (bad / vol) >= failRate and (vol + 0.5 >= minVolume or fails >= threshold)
if st == 'half_open' or (st == 'closed' and trip) then
  redis.call('HSET', KEYS[1], 'state', 'open', 'opened_at', tostring(now))
  if st ~= 'open' then changed = 1 end
  st = 'open'
end
return {st, changed, fails}
`

var (
	before = redis.NewScript(beforeScript)
	after  = redis.NewScript(afterScript)
)

// Config tunes the guard.
type Config struct {
	FailureThreshold int           // consecutive failures that trip a low-volume endpoint
	FailureRate      float64       // decayed failure ratio that trips the breaker
	MinVolume        float64       // decayed attempts needed before the ratio alone can trip it
	HalfLife         time.Duration // decay half-life for the failure ratio
	Cooldown         time.Duration
	ProbeLease       time.Duration // how long one half-open probe may take
	MinRate          float64
	MaxRateWait      time.Duration // longest a delivery may wait in place for a rate-limit slot
}

// Guard is the per-endpoint protection layer.
type Guard struct {
	rdb redis.Scripter
	cfg Config

	mu    sync.Mutex
	local map[string]localState // short-lived cache so a known-open circuit skips Redis
}

type localState struct {
	validUntil time.Time // how long we trust the cached "open" without asking Redis
	reopenAt   time.Time // when the cooldown actually ends
}

// New creates a guard.
func New(rdb redis.Scripter, cfg Config) *Guard {
	if cfg.ProbeLease == 0 {
		cfg.ProbeLease = 10 * time.Second
	}
	if cfg.MinRate == 0 {
		cfg.MinRate = 2
	}
	if cfg.FailureRate == 0 {
		cfg.FailureRate = 0.5
	}
	if cfg.MinVolume == 0 {
		cfg.MinVolume = 10
	}
	if cfg.MaxRateWait == 0 {
		cfg.MaxRateWait = 5 * time.Second
	}
	if cfg.HalfLife == 0 {
		cfg.HalfLife = 10 * time.Second
	}
	return &Guard{rdb: rdb, cfg: cfg, local: map[string]localState{}}
}

// Key helpers (also read by the realtime/api services).
func CBKey(ep string) string     { return "cb:" + ep }
func BucketKey(ep string) string { return "rl:ep:" + ep }
func AIMDKey(ep string) string   { return "aimd:" + ep }

// Decision is the result of Before.
type Decision struct {
	Decision      string
	Wait          time.Duration
	State         string
	EffectiveRate float64
}

// Before decides whether a delivery to the endpoint may proceed now. maxRate <= 0 disables
// rate limiting for the endpoint.
func (g *Guard) Before(ctx context.Context, ep string, maxRate float64) (Decision, error) {
	now := time.Now()
	g.mu.Lock()
	ls, ok := g.local[ep]
	g.mu.Unlock()
	if ok && now.Before(ls.validUntil) && now.Before(ls.reopenAt) {
		return Decision{Decision: DenyOpen, Wait: ls.reopenAt.Sub(now), State: Open}, nil
	}
	burst := maxRate
	if burst < 1 {
		burst = 1
	}
	res, err := before.Run(ctx, g.rdb, []string{CBKey(ep), BucketKey(ep), AIMDKey(ep)},
		now.UnixMilli(), g.cfg.Cooldown.Milliseconds(), g.cfg.ProbeLease.Milliseconds(), maxRate, burst, g.cfg.MaxRateWait.Milliseconds()).Slice()
	if err != nil {
		return Decision{}, err
	}
	d := Decision{
		Decision:      res[0].(string),
		Wait:          time.Duration(res[1].(int64)) * time.Millisecond,
		State:         res[2].(string),
		EffectiveRate: float64(res[3].(int64)) / 1000,
	}
	if d.Decision == DenyOpen {
		g.mu.Lock()
		g.local[ep] = localState{validUntil: now.Add(min(d.Wait, time.Second)), reopenAt: now.Add(d.Wait)}
		g.mu.Unlock()
	}
	return d, nil
}

// Outcome is what After reports.
type Outcome struct {
	State               string
	Changed             bool
	ConsecutiveFailures int64
}

// After records an attempt. overload marks responses that suggest the receiver is struggling
// (timeouts, 429, 503) and should be sent less traffic.
func (g *Guard) After(ctx context.Context, ep string, success, overload bool, maxRate float64) (Outcome, error) {
	b := func(v bool) string {
		if v {
			return "1"
		}
		return "0"
	}
	res, err := after.Run(ctx, g.rdb, []string{CBKey(ep), AIMDKey(ep)},
		time.Now().UnixMilli(), b(success), b(overload), g.cfg.FailureThreshold, maxRate, g.cfg.MinRate,
		g.cfg.MinVolume, g.cfg.FailureRate, g.cfg.HalfLife.Milliseconds()).Slice()
	if err != nil {
		return Outcome{}, err
	}
	o := Outcome{State: res[0].(string), Changed: res[1].(int64) == 1, ConsecutiveFailures: res[2].(int64)}
	if o.State == Closed {
		g.mu.Lock()
		delete(g.local, ep)
		g.mu.Unlock()
	}
	return o, nil
}

// Snapshot is the externally visible breaker state.
type Snapshot struct {
	State               string
	ConsecutiveFailures int
	OpenedAt            *time.Time
	EffectiveRate       *float64
}

// Read returns the current state of an endpoint's breaker and limiter without changing them.
func Read(ctx context.Context, rdb redis.Cmdable, ep string, cooldown time.Duration) Snapshot {
	s := Snapshot{State: Closed}
	m, err := rdb.HGetAll(ctx, CBKey(ep)).Result()
	if err == nil {
		if v := m["state"]; v != "" {
			s.State = v
		}
		var n int
		for _, c := range m["failures"] {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		s.ConsecutiveFailures = n
		if s.State != Closed {
			if ms, ok := parseInt(m["opened_at"]); ok {
				t := time.UnixMilli(ms).UTC()
				s.OpenedAt = &t
				// An open circuit past its cooldown is effectively half-open (the next
				// delivery becomes the probe).
				if s.State == Open && time.Since(t) >= cooldown {
					s.State = HalfOpen
				}
			}
		}
	}
	if r, err := rdb.HGet(ctx, AIMDKey(ep), "rate").Float64(); err == nil {
		s.EffectiveRate = &r
	}
	return s
}

func parseInt(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}
