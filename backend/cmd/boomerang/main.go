// Command boomerang runs any Boomerang service: one binary, one image, many roles.
//
//	boomerang <api|fanout|worker|retry|recorder|realtime|shopsim|receivers|init>
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/boomerang-dev/boomerang/internal/api"
	"github.com/boomerang-dev/boomerang/internal/app"
	"github.com/boomerang-dev/boomerang/internal/config"
	"github.com/boomerang-dev/boomerang/internal/fanout"
	"github.com/boomerang-dev/boomerang/internal/httpx"
	"github.com/boomerang-dev/boomerang/internal/initcmd"
	"github.com/boomerang-dev/boomerang/internal/obs"
	"github.com/boomerang-dev/boomerang/internal/realtime"
	"github.com/boomerang-dev/boomerang/internal/receivers"
	"github.com/boomerang-dev/boomerang/internal/recorder"
	"github.com/boomerang-dev/boomerang/internal/retry"
	"github.com/boomerang-dev/boomerang/internal/shopsim"
	"github.com/boomerang-dev/boomerang/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: boomerang <api|fanout|worker|retry|recorder|realtime|shopsim|receivers|init>")
		os.Exit(2)
	}
	role := os.Args[1]
	cfg := config.Load(role)
	obs.Logger(role, cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTracing := obs.InitTracing(ctx, role, cfg.OTLPEndpoint)
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()
	if err := run(ctx, role, cfg); err != nil {
		slog.Error("exited with error", "err", err)
		os.Exit(1)
	}
	slog.Info("shut down cleanly")
}

func run(ctx context.Context, role string, cfg config.Config) error {
	if role == "init" {
		return initcmd.Run(ctx, cfg)
	}
	needs := map[string]app.Need{
		"api":       {Cache: true, ClickHouse: true, Blob: true, Producer: true},
		"fanout":    {Cache: true, Producer: true},
		"worker":    {Cache: true, Blob: true, Producer: true},
		"retry":     {Producer: true},
		"recorder":  {Redis: true, ClickHouse: true},
		"realtime":  {Postgres: true, Redis: true, ClickHouse: true, Blob: true},
		"shopsim":   {Redis: true},
		"receivers": {Redis: true},
	}
	need, ok := needs[role]
	if !ok {
		return fmt.Errorf("unknown role %q", role)
	}
	d, err := app.Open(ctx, cfg, need)
	if err != nil {
		return err
	}
	defer d.Close()

	mux := http.NewServeMux()
	d.Health.Mount(mux)
	var wg sync.WaitGroup
	bg := func(fn func()) {
		wg.Add(1)
		go func() { defer wg.Done(); fn() }()
	}
	var runErr error
	switch role {
	case "api":
		a := api.New(d)
		a.Mount(mux)
		bg(func() { a.Run(ctx) })
	case "fanout":
		bg(func() { runErr = fanout.Run(ctx, d) })
	case "worker":
		w := worker.New(d)
		bg(func() { runErr = w.Run(ctx) })
	case "retry":
		bg(func() { runErr = retry.Run(ctx, d) })
	case "recorder":
		bg(func() { runErr = recorder.Run(ctx, d) })
	case "realtime":
		s, err := realtime.New(d)
		if err != nil {
			return err
		}
		s.Mount(mux)
		bg(func() { runErr = s.Run(ctx) })
	case "shopsim":
		s := shopsim.New(d)
		s.Mount(mux)
		bg(func() { s.Run(ctx) })
	case "receivers":
		receivers.New(d).Mount(mux)
	}
	go func() {
		<-ctx.Done()
		d.Health.SetStopping() // fail readiness first so the load balancer drains us
	}()
	handler := httpx.Recover(httpx.Logging(mux))
	if err := obs.Serve(ctx, cfg.HTTPAddr, handler); err != nil {
		return err
	}
	wg.Wait()
	return runErr
}
