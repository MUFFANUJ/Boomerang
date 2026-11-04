// Package app builds the shared dependencies each service needs.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/boomerang-dev/boomerang/internal/blob"
	"github.com/boomerang-dev/boomerang/internal/cache"
	"github.com/boomerang-dev/boomerang/internal/chstore"
	"github.com/boomerang-dev/boomerang/internal/config"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/obs"
	"github.com/boomerang-dev/boomerang/internal/store"
)

// Deps are lazily-opened shared clients.
type Deps struct {
	Cfg      config.Config
	Health   *obs.Health
	Store    *store.Store
	Redis    *redis.Client
	CH       *chstore.Client
	Blob     *blob.Store
	Producer *kgo.Client
	Cache    *cache.Cache
	closers  []func()
}

// Need flags.
type Need struct {
	Postgres, Redis, ClickHouse, Blob, Producer, Cache bool
}

// Open connects to the requested dependencies, retrying while they start up.
func Open(ctx context.Context, cfg config.Config, n Need) (*Deps, error) {
	d := &Deps{Cfg: cfg, Health: obs.NewHealth()}
	if n.Cache {
		n.Postgres, n.Redis = true, true
	}
	if n.Postgres {
		err := retry(ctx, "postgres", func() error {
			st, err := store.Open(ctx, cfg.PostgresURL, cfg.SecretsKey)
			if err != nil {
				return err
			}
			if err := st.Ping(ctx); err != nil {
				st.Close()
				return err
			}
			d.Store = st
			return nil
		})
		if err != nil {
			return nil, err
		}
		d.closers = append(d.closers, d.Store.Close)
		d.Health.Add("postgres", d.Store.Ping)
	}
	if n.Redis {
		d.Redis = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 64, MinIdleConns: 4})
		if err := retry(ctx, "redis", func() error { return d.Redis.Ping(ctx).Err() }); err != nil {
			return nil, err
		}
		d.closers = append(d.closers, func() { _ = d.Redis.Close() })
		d.Health.Add("redis", func(c context.Context) error { return d.Redis.Ping(c).Err() })
	}
	if n.ClickHouse {
		d.CH = chstore.New(cfg.ClickHouseURL, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHousePass)
		if err := retry(ctx, "clickhouse", func() error { return d.CH.Ping(ctx) }); err != nil {
			return nil, err
		}
	}
	if n.Blob {
		b, err := blob.New(cfg.MinIOEndpoint, cfg.MinIOAccessKey, cfg.MinIOSecretKey, cfg.MinIOBucket, cfg.MinIOUseSSL)
		if err != nil {
			return nil, err
		}
		d.Blob = b
	}
	if n.Producer {
		p, err := kafkax.NewProducer(cfg.KafkaBrokers, cfg.Service+"-"+cfg.ServiceID)
		if err != nil {
			return nil, err
		}
		if err := retry(ctx, "kafka", func() error { return p.Ping(ctx) }); err != nil {
			return nil, err
		}
		d.Producer = p
		d.closers = append(d.closers, func() {
			fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = p.Flush(fctx)
			p.Close()
		})
		d.Health.Add("kafka", p.Ping)
	}
	if n.Cache {
		d.Cache = cache.New(d.Store, d.Redis)
		go d.Cache.Listen(ctx)
	}
	return d, nil
}

// Close releases everything in reverse order.
func (d *Deps) Close() {
	for i := len(d.closers) - 1; i >= 0; i-- {
		d.closers[i]()
	}
}

func retry(ctx context.Context, name string, fn func() error) error {
	var err error
	for i := 0; i < 60; i++ {
		if err = fn(); err == nil {
			return nil
		}
		slog.Warn("waiting for dependency", "dep", name, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("%s unavailable: %w", name, err)
}
