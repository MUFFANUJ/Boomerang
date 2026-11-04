// Package cache keeps Postgres off the hot path. Lookups go to an in-process cache first,
// then Redis, then Postgres. Writers publish invalidations over Redis pub/sub so every
// replica drops stale entries immediately.
//
// Signing secrets are never written to Redis; endpoints with secrets are cached in-process only.
package cache

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/stats"
	"github.com/boomerang-dev/boomerang/internal/store"
)

type entry[T any] struct {
	v   T
	exp time.Time
}

// KeyInfo is a cached API key resolution.
type KeyInfo struct {
	KeyID    string `json:"key_id"`
	TenantID string `json:"tenant_id"`
	Revoked  bool   `json:"revoked"`
}

// Subscriber is the secret-free view of an endpoint used by fan-out.
type Subscriber struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	EventTypes []string `json:"event_types"`
}

// Cache is a two-level read-through cache.
type Cache struct {
	st       *store.Store
	rdb      *redis.Client
	localTTL time.Duration
	redisTTL time.Duration
	sf       singleflight.Group

	mu        sync.RWMutex
	tenants   map[string]entry[*model.Tenant]
	subs      map[string]entry[[]Subscriber]
	endpoints map[string]entry[*model.Endpoint]
	keys      map[string]entry[*KeyInfo]
}

// New creates a cache.
func New(st *store.Store, rdb *redis.Client) *Cache {
	return &Cache{
		st: st, rdb: rdb, localTTL: 15 * time.Second, redisTTL: 5 * time.Minute,
		tenants: map[string]entry[*model.Tenant]{}, subs: map[string]entry[[]Subscriber]{},
		endpoints: map[string]entry[*model.Endpoint]{}, keys: map[string]entry[*KeyInfo]{},
	}
}

func getLocal[T any](c *Cache, m map[string]entry[T], k string) (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := m[k]
	if !ok || time.Now().After(e.exp) {
		var zero T
		return zero, false
	}
	return e.v, true
}

func setLocal[T any](c *Cache, m map[string]entry[T], k string, v T) {
	c.mu.Lock()
	m[k] = entry[T]{v: v, exp: time.Now().Add(c.localTTL)}
	c.mu.Unlock()
}

func (c *Cache) redisGet(ctx context.Context, key string, dst any) bool {
	b, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return false
	}
	return json.Unmarshal(b, dst) == nil
}

func (c *Cache) redisSet(ctx context.Context, key string, v any) {
	b, err := json.Marshal(v)
	if err == nil {
		c.rdb.Set(ctx, key, b, c.redisTTL)
	}
}

// Tenant returns a tenant.
func (c *Cache) Tenant(ctx context.Context, id string) (*model.Tenant, error) {
	if v, ok := getLocal(c, c.tenants, id); ok {
		return v, nil
	}
	v, err, _ := c.sf.Do("t:"+id, func() (any, error) {
		var t model.Tenant
		if c.redisGet(ctx, "cache:tenant:"+id, &t) {
			return &t, nil
		}
		tp, err := c.st.GetTenant(ctx, id)
		if err != nil {
			return nil, err
		}
		c.redisSet(ctx, "cache:tenant:"+id, tp)
		return tp, nil
	})
	if err != nil {
		return nil, err
	}
	t := v.(*model.Tenant)
	setLocal(c, c.tenants, id, t)
	return t, nil
}

// Subscribers returns a tenant's live endpoints (without secrets) for fan-out.
func (c *Cache) Subscribers(ctx context.Context, tenantID string) ([]Subscriber, error) {
	if v, ok := getLocal(c, c.subs, tenantID); ok {
		return v, nil
	}
	v, err, _ := c.sf.Do("s:"+tenantID, func() (any, error) {
		var subs []Subscriber
		if c.redisGet(ctx, "cache:subs:"+tenantID, &subs) {
			return subs, nil
		}
		eps, err := c.st.ListEndpoints(ctx, tenantID)
		if err != nil {
			return nil, err
		}
		subs = make([]Subscriber, 0, len(eps))
		for _, e := range eps {
			subs = append(subs, Subscriber{ID: e.ID, Name: e.Name, EventTypes: e.EventTypes})
		}
		c.redisSet(ctx, "cache:subs:"+tenantID, subs)
		return subs, nil
	})
	if err != nil {
		return nil, err
	}
	s := v.([]Subscriber)
	setLocal(c, c.subs, tenantID, s)
	return s, nil
}

// Endpoint returns an endpoint including its secrets (in-process cache only).
// A deleted endpoint returns store.ErrNotFound.
func (c *Cache) Endpoint(ctx context.Context, id string) (*model.Endpoint, error) {
	if v, ok := getLocal(c, c.endpoints, id); ok {
		if v == nil {
			return nil, store.ErrNotFound
		}
		return v, nil
	}
	v, err, _ := c.sf.Do("e:"+id, func() (any, error) {
		return c.st.GetEndpoint(ctx, id)
	})
	if errors.Is(err, store.ErrNotFound) {
		setLocal[*model.Endpoint](c, c.endpoints, id, nil)
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	e := v.(*model.Endpoint)
	setLocal(c, c.endpoints, id, e)
	return e, nil
}

// APIKey resolves a raw API key.
func (c *Cache) APIKey(ctx context.Context, raw string) (*KeyInfo, error) {
	h := hex.EncodeToString(store.HashAPIKey(raw))
	if v, ok := getLocal(c, c.keys, h); ok {
		if v == nil {
			return nil, store.ErrNotFound
		}
		return v, nil
	}
	v, err, _ := c.sf.Do("k:"+h, func() (any, error) {
		var ki KeyInfo
		if c.redisGet(ctx, "cache:key:"+h, &ki) {
			return &ki, nil
		}
		k, err := c.st.LookupKey(ctx, store.HashAPIKey(raw))
		if err != nil {
			return nil, err
		}
		kp := &KeyInfo{KeyID: k.ID, TenantID: k.TenantID, Revoked: k.RevokedAt != nil}
		c.redisSet(ctx, "cache:key:"+h, kp)
		return kp, nil
	})
	if errors.Is(err, store.ErrNotFound) {
		setLocal[*KeyInfo](c, c.keys, h, nil)
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	k := v.(*KeyInfo)
	setLocal(c, c.keys, h, k)
	return k, nil
}

// InvalidateTenant drops a tenant and its subscriptions everywhere.
func (c *Cache) InvalidateTenant(ctx context.Context, tenantID string) {
	c.rdb.Del(ctx, "cache:tenant:"+tenantID, "cache:subs:"+tenantID)
	c.rdb.Publish(ctx, stats.InvalidateChan, "tenant:"+tenantID)
}

// InvalidateEndpoint drops an endpoint and its tenant's subscriptions everywhere.
func (c *Cache) InvalidateEndpoint(ctx context.Context, tenantID, endpointID string) {
	c.rdb.Del(ctx, "cache:subs:"+tenantID)
	c.rdb.Publish(ctx, stats.InvalidateChan, "endpoint:"+tenantID+":"+endpointID)
	c.rdb.Publish(ctx, stats.EndpointChanged, endpointID)
}

// InvalidateKeys drops all cached API keys (on revoke).
func (c *Cache) InvalidateKeys(ctx context.Context) {
	iter := c.rdb.Scan(ctx, 0, "cache:key:*", 1000).Iterator()
	for iter.Next(ctx) {
		c.rdb.Del(ctx, iter.Val())
	}
	c.rdb.Publish(ctx, stats.InvalidateChan, "keys")
}

// Listen applies invalidations published by other replicas until ctx is done.
func (c *Cache) Listen(ctx context.Context) {
	sub := c.rdb.Subscribe(ctx, stats.InvalidateChan)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			c.apply(m.Payload)
		}
	}
}

func (c *Cache) apply(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	parts := strings.Split(msg, ":")
	switch parts[0] {
	case "tenant":
		if len(parts) == 2 {
			delete(c.tenants, parts[1])
			delete(c.subs, parts[1])
		}
	case "endpoint":
		if len(parts) == 3 {
			delete(c.subs, parts[1])
			delete(c.endpoints, parts[2])
		}
	case "keys":
		c.keys = map[string]entry[*KeyInfo]{}
	default:
		slog.Warn("unknown invalidation", "msg", msg)
	}
}
