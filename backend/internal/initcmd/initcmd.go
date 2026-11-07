// Package initcmd prepares a fresh environment: schemas, topics, the payload bucket and the
// ShopSim demo tenant with its three subscribed receivers. It is idempotent.
package initcmd

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/chstore"
	"github.com/boomerang-dev/boomerang/internal/config"
	"github.com/boomerang-dev/boomerang/internal/kafkax"
	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/stats"
	"github.com/boomerang-dev/boomerang/internal/store"
)

// Run performs initialization.
func Run(ctx context.Context, cfg config.Config) error {
	// Migrations use a direct connection (not PgBouncer).
	direct := cfg
	direct.PostgresURL = cfg.PostgresDirectURL
	d, err := app.Open(ctx, direct, app.Need{Postgres: true, ClickHouse: true, Blob: true})
	if err != nil {
		return err
	}
	defer d.Close()

	slog.Info("migrating postgres")
	if err := d.Store.Migrate(ctx); err != nil {
		return err
	}
	slog.Info("migrating clickhouse")
	ch := chstore.New(cfg.ClickHouseURL, cfg.ClickHouseDB, cfg.ClickHouseUser, cfg.ClickHousePass)
	if err := ch.Migrate(ctx); err != nil {
		return err
	}
	slog.Info("creating kafka topics")
	for i := 0; ; i++ {
		err := kafkax.EnsureTopics(ctx, cfg.KafkaBrokers)
		if err == nil {
			break
		}
		if i >= 60 {
			return err
		}
		slog.Warn("waiting for kafka", "err", err)
		time.Sleep(2 * time.Second)
	}
	slog.Info("creating payload bucket")
	for i := 0; ; i++ {
		err := d.Blob.EnsureBucket(ctx)
		if err == nil {
			break
		}
		if i >= 60 {
			return err
		}
		slog.Warn("waiting for object storage", "err", err)
		time.Sleep(2 * time.Second)
	}
	if err := seed(ctx, d.Store, cfg); err != nil {
		return err
	}
	markStart(ctx, cfg)
	return seedDLQSet(ctx, cfg, ch)
}

// seedDLQSet rebuilds the Redis DLQ membership set from ClickHouse if it is missing (e.g. after
// a Redis flush or an upgrade), so the dashboard's DLQ depth stays exact.
func seedDLQSet(ctx context.Context, cfg config.Config, ch *chstore.Client) error {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if n, err := rdb.Exists(ctx, stats.DLQSet).Result(); err != nil || n > 0 {
		return err
	}
	ids, err := ch.DeadDeliveryIDs(ctx)
	if err != nil || len(ids) == 0 {
		return err
	}
	for start := 0; start < len(ids); start += 5000 {
		end := min(start+5000, len(ids))
		batch := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			batch = append(batch, id)
		}
		if err := rdb.SAdd(ctx, stats.DLQSet, batch...).Err(); err != nil {
			return err
		}
	}
	slog.Info("seeded DLQ set from ClickHouse", "dead", len(ids))
	return nil
}

// markStart starts the integrity audit window at first boot (later resets move it).
func markStart(ctx context.Context, cfg config.Config) {
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	rdb.SetNX(ctx, stats.IntegritySince, time.Now().UnixMilli(), 0)
}

func seed(ctx context.Context, st *store.Store, cfg config.Config) error {
	t, err := st.CreateTenant(ctx, &model.Tenant{
		ID: "ten_shopsim", Name: "ShopSim", RateLimitRPS: 10000, RateLimitBurst: 20000,
		RetryMaxAttempts: 5, RetryScheduleMs: config.DefaultRetrySchedule(cfg.DemoMode),
	})
	if err != nil {
		return err
	}
	if _, _, err := st.CreateKey(ctx, t.ID, "ShopSim traffic generator", cfg.ShopSimAPIKey); err != nil {
		return err
	}
	base := strings.TrimRight(cfg.ReceiversURL, "/")
	eps := []model.Endpoint{
		{ID: "ep_inventory", Name: "Inventory Service", URL: base + "/inventory",
			Description: "Updates stock levels when orders are created, shipped or refunded.",
			EventTypes:  []string{"order.*"}, RateLimitRPS: 1000},
		{ID: "ep_email", Name: "Email Service", URL: base + "/email",
			Description: "Sends receipts and shipping notifications to customers.",
			EventTypes:  []string{"payment.*", "order.shipped"}, RateLimitRPS: 500},
		{ID: "ep_analytics", Name: "Analytics Service", URL: base + "/analytics",
			Description: "Receives every event for reporting.",
			EventTypes:  []string{"*"}},
	}
	for i := range eps {
		eps[i].TenantID = t.ID
		if _, err := st.CreateEndpoint(ctx, &eps[i]); err != nil {
			return err
		}
	}
	slog.Info("seeded demo tenant", "tenant", t.ID, "endpoints", len(eps))
	return nil
}
