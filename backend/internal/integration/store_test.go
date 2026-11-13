//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/boomerang-dev/boomerang/internal/model"
	"github.com/boomerang-dev/boomerang/internal/store"
)

func TestStoreAgainstPostgres(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, startPostgres(t), "test-key")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrations must be idempotent: %v", err)
	}

	ten, err := st.CreateTenant(ctx, &model.Tenant{Name: "Acme", RateLimitRPS: 10, RateLimitBurst: 20, RetryMaxAttempts: 3, RetryScheduleMs: []int{1000, 2000}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := st.GetTenant(ctx, ten.ID); got.RetryScheduleMs[1] != 2000 {
		t.Fatalf("retry schedule round trip: %+v", got)
	}

	// API keys are stored hashed and resolve by hash.
	k, raw, err := st.CreateKey(ctx, ten.ID, "ci", "")
	if err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := st.Pool.QueryRow(ctx, `SELECT key_hash FROM api_keys WHERE id=$1`, k.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored), raw) {
		t.Fatal("raw API key must not be stored")
	}
	if got, err := st.LookupKey(ctx, store.HashAPIKey(raw)); err != nil || got.TenantID != ten.ID {
		t.Fatalf("lookup by hash: %v %+v", err, got)
	}

	// Signing secrets are encrypted at rest.
	ep, err := st.CreateEndpoint(ctx, &model.Endpoint{TenantID: ten.ID, Name: "Hook", URL: "https://example.com/hook", EventTypes: []string{"order.*"}})
	if err != nil {
		t.Fatal(err)
	}
	var enc string
	if err := st.Pool.QueryRow(ctx, `SELECT secret_enc FROM endpoints WHERE id=$1`, ep.ID).Scan(&enc); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(enc, ep.Secret) || !strings.HasPrefix(ep.Secret, "whsec_") {
		t.Fatal("secret must be encrypted at rest")
	}

	// Rotation keeps the old secret valid for the grace period.
	rot, err := st.RotateSecret(ctx, ten.ID, ep.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if rot.PreviousSecret != ep.Secret || rot.Secret == ep.Secret {
		t.Fatal("rotation should move the current secret to previous")
	}
	if s := rot.Secrets(time.Now()); len(s) != 2 {
		t.Fatalf("both secrets should be active during the grace period, got %d", len(s))
	}

	paused := true
	up, err := st.UpdateEndpoint(ctx, ten.ID, ep.ID, store.EndpointPatch{Paused: &paused})
	if err != nil || !up.Paused || up.Name != "Hook" {
		t.Fatalf("partial update: %v %+v", err, up)
	}
	if err := st.DeleteEndpoint(ctx, ten.ID, ep.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetEndpoint(ctx, ep.ID); err != store.ErrNotFound {
		t.Fatalf("deleted endpoint should be gone, got %v", err)
	}
}
