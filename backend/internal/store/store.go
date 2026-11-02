// Package store is the PostgreSQL repository for configuration data: tenants, API keys,
// endpoints and subscriptions.
package store

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boomerang-dev/boomerang/internal/model"
)

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// Store wraps a pgx pool.
type Store struct {
	Pool *pgxpool.Pool
	aead cipher.AEAD
}

// Open connects to Postgres. PgBouncer in transaction mode is supported because statements
// are sent without server-side prepared statement caching.
func Open(ctx context.Context, url, encryptionKey string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	key := sha256.Sum256([]byte(encryptionKey))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Store{Pool: pool, aead: aead}, nil
}

// Close closes the pool.
func (s *Store) Close() { s.Pool.Close() }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

const schema = `
CREATE TABLE IF NOT EXISTS tenants (
  id                 text PRIMARY KEY,
  name               text NOT NULL,
  created_at         timestamptz NOT NULL DEFAULT now(),
  rate_limit_rps     int NOT NULL DEFAULT 1000,
  rate_limit_burst   int NOT NULL DEFAULT 2000,
  retry_max_attempts int NOT NULL DEFAULT 5,
  retry_schedule_ms  int[] NOT NULL
);
CREATE TABLE IF NOT EXISTS api_keys (
  id           text PRIMARY KEY,
  tenant_id    text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name         text NOT NULL,
  prefix       text NOT NULL,
  key_hash     bytea NOT NULL UNIQUE,
  created_at   timestamptz NOT NULL DEFAULT now(),
  last_used_at timestamptz,
  revoked_at   timestamptz
);
CREATE INDEX IF NOT EXISTS api_keys_tenant ON api_keys(tenant_id);
CREATE TABLE IF NOT EXISTS endpoints (
  id                  text PRIMARY KEY,
  tenant_id           text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  name                text NOT NULL,
  url                 text NOT NULL,
  description         text NOT NULL DEFAULT '',
  event_types         text[] NOT NULL,
  paused              boolean NOT NULL DEFAULT false,
  rate_limit_rps      int NOT NULL DEFAULT 0,
  secret_enc          text NOT NULL,
  previous_secret_enc text NOT NULL DEFAULT '',
  previous_expires_at timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  deleted_at          timestamptz
);
CREATE INDEX IF NOT EXISTS endpoints_tenant ON endpoints(tenant_id) WHERE deleted_at IS NULL;
`

// Migrate creates the schema.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.Pool.Exec(ctx, schema, pgx.QueryExecModeSimpleProtocol)
	return err
}

// --- secret encryption (AES-GCM at rest) ---

func (s *Store) encrypt(plain string) (string, error) {
	if plain == "" {
		return "", nil
	}
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	out := s.aead.Seal(nonce, nonce, []byte(plain), nil)
	return base64.StdEncoding.EncodeToString(out), nil
}

func (s *Store) decrypt(enc string) (string, error) {
	if enc == "" {
		return "", nil
	}
	b, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return "", err
	}
	ns := s.aead.NonceSize()
	if len(b) < ns {
		return "", errors.New("ciphertext too short")
	}
	p, err := s.aead.Open(nil, b[:ns], b[ns:], nil)
	if err != nil {
		return "", err
	}
	return string(p), nil
}

// NewSigningSecret generates a whsec_ secret.
func NewSigningSecret() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return "whsec_" + base64.StdEncoding.EncodeToString(b)
}

// HashAPIKey hashes a raw API key for storage/lookup.
func HashAPIKey(raw string) []byte {
	h := sha256.Sum256([]byte(raw))
	return h[:]
}

// NewAPIKey generates a raw API key.
func NewAPIKey() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return "bmr_live_" + strings.TrimRight(base64.URLEncoding.EncodeToString(b), "=")
}

// --- tenants ---

const tenantCols = `id, name, created_at, rate_limit_rps, rate_limit_burst, retry_max_attempts, retry_schedule_ms`

func scanTenant(row pgx.Row) (*model.Tenant, error) {
	var t model.Tenant
	var sched []int32
	if err := row.Scan(&t.ID, &t.Name, &t.CreatedAt, &t.RateLimitRPS, &t.RateLimitBurst, &t.RetryMaxAttempts, &sched); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	for _, v := range sched {
		t.RetryScheduleMs = append(t.RetryScheduleMs, int(v))
	}
	return &t, nil
}

// ListTenants returns all tenants.
func (s *Store) ListTenants(ctx context.Context) ([]*model.Tenant, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+tenantCols+` FROM tenants ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Tenant
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetTenant returns a tenant.
func (s *Store) GetTenant(ctx context.Context, id string) (*model.Tenant, error) {
	return scanTenant(s.Pool.QueryRow(ctx, `SELECT `+tenantCols+` FROM tenants WHERE id=$1`, id))
}

// CreateTenant inserts a tenant (id may be preset for seeding).
func (s *Store) CreateTenant(ctx context.Context, t *model.Tenant) (*model.Tenant, error) {
	if t.ID == "" {
		t.ID = model.NewID("ten")
	}
	return scanTenant(s.Pool.QueryRow(ctx, `
		INSERT INTO tenants (id, name, rate_limit_rps, rate_limit_burst, retry_max_attempts, retry_schedule_ms)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (id) DO UPDATE SET name = tenants.name
		RETURNING `+tenantCols,
		t.ID, t.Name, t.RateLimitRPS, t.RateLimitBurst, t.RetryMaxAttempts, toInt32(t.RetryScheduleMs)))
}

// TenantPatch holds optional tenant updates.
type TenantPatch struct {
	Name             *string `json:"name"`
	RateLimitRPS     *int    `json:"rate_limit_rps"`
	RateLimitBurst   *int    `json:"rate_limit_burst"`
	RetryMaxAttempts *int    `json:"retry_max_attempts"`
	RetryScheduleMs  []int   `json:"retry_schedule_ms"`
}

// UpdateTenant applies a patch.
func (s *Store) UpdateTenant(ctx context.Context, id string, p TenantPatch) (*model.Tenant, error) {
	var sched any
	if p.RetryScheduleMs != nil {
		sched = toInt32(p.RetryScheduleMs)
	}
	return scanTenant(s.Pool.QueryRow(ctx, `
		UPDATE tenants SET
		  name = COALESCE($2, name),
		  rate_limit_rps = COALESCE($3, rate_limit_rps),
		  rate_limit_burst = COALESCE($4, rate_limit_burst),
		  retry_max_attempts = COALESCE($5, retry_max_attempts),
		  retry_schedule_ms = COALESCE($6::int[], retry_schedule_ms)
		WHERE id=$1 RETURNING `+tenantCols,
		id, p.Name, p.RateLimitRPS, p.RateLimitBurst, p.RetryMaxAttempts, sched))
}

func toInt32(v []int) []int32 {
	out := make([]int32, len(v))
	for i, x := range v {
		out[i] = int32(x)
	}
	return out
}

// --- API keys ---

const keyCols = `id, tenant_id, name, prefix, created_at, last_used_at, revoked_at`

func scanKey(row pgx.Row) (*model.APIKey, error) {
	var k model.APIKey
	if err := row.Scan(&k.ID, &k.TenantID, &k.Name, &k.Prefix, &k.CreatedAt, &k.LastUsedAt, &k.RevokedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &k, nil
}

// ListKeys lists a tenant's keys.
func (s *Store) ListKeys(ctx context.Context, tenantID string) ([]*model.APIKey, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+keyCols+` FROM api_keys WHERE tenant_id=$1 ORDER BY created_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.APIKey
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// CreateKey stores a key. If raw is empty a new key is generated. Returns the raw key.
func (s *Store) CreateKey(ctx context.Context, tenantID, name, raw string) (*model.APIKey, string, error) {
	if raw == "" {
		raw = NewAPIKey()
	}
	prefix := raw
	if len(prefix) > 16 {
		prefix = prefix[:16]
	}
	k, err := scanKey(s.Pool.QueryRow(ctx, `
		INSERT INTO api_keys (id, tenant_id, name, prefix, key_hash) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (key_hash) DO UPDATE SET revoked_at = NULL
		RETURNING `+keyCols, model.NewID("key"), tenantID, name, prefix, HashAPIKey(raw)))
	return k, raw, err
}

// RevokeKey revokes a key.
func (s *Store) RevokeKey(ctx context.Context, tenantID, id string) (*model.APIKey, error) {
	return scanKey(s.Pool.QueryRow(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE tenant_id=$1 AND id=$2 RETURNING `+keyCols, tenantID, id))
}

// LookupKey resolves a raw key hash to its key row (including revoked ones).
func (s *Store) LookupKey(ctx context.Context, hash []byte) (*model.APIKey, error) {
	return scanKey(s.Pool.QueryRow(ctx, `SELECT `+keyCols+` FROM api_keys WHERE key_hash=$1`, hash))
}

// TouchKey records key usage.
func (s *Store) TouchKey(ctx context.Context, id string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE api_keys SET last_used_at = now() WHERE id=$1`, id)
	return err
}

// --- endpoints ---

const epCols = `id, tenant_id, name, url, description, event_types, paused, rate_limit_rps, secret_enc, previous_secret_enc, previous_expires_at, created_at, updated_at`

func (s *Store) scanEndpoint(row pgx.Row) (*model.Endpoint, error) {
	var e model.Endpoint
	var secEnc, prevEnc string
	if err := row.Scan(&e.ID, &e.TenantID, &e.Name, &e.URL, &e.Description, &e.EventTypes, &e.Paused, &e.RateLimitRPS, &secEnc, &prevEnc, &e.PreviousExpiresAt, &e.CreatedAt, &e.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var err error
	if e.Secret, err = s.decrypt(secEnc); err != nil {
		return nil, fmt.Errorf("decrypt secret: %w", err)
	}
	if e.PreviousSecret, err = s.decrypt(prevEnc); err != nil {
		return nil, fmt.Errorf("decrypt previous secret: %w", err)
	}
	if e.EventTypes == nil {
		e.EventTypes = []string{}
	}
	return &e, nil
}

func (s *Store) queryEndpoints(ctx context.Context, q string, args ...any) ([]*model.Endpoint, error) {
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*model.Endpoint{}
	for rows.Next() {
		e, err := s.scanEndpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListEndpoints lists a tenant's live endpoints.
func (s *Store) ListEndpoints(ctx context.Context, tenantID string) ([]*model.Endpoint, error) {
	return s.queryEndpoints(ctx, `SELECT `+epCols+` FROM endpoints WHERE tenant_id=$1 AND deleted_at IS NULL ORDER BY created_at`, tenantID)
}

// ListAllEndpoints lists every live endpoint.
func (s *Store) ListAllEndpoints(ctx context.Context) ([]*model.Endpoint, error) {
	return s.queryEndpoints(ctx, `SELECT `+epCols+` FROM endpoints WHERE deleted_at IS NULL ORDER BY created_at`)
}

// GetEndpoint returns an endpoint by id (deleted endpoints return ErrNotFound).
func (s *Store) GetEndpoint(ctx context.Context, id string) (*model.Endpoint, error) {
	return s.scanEndpoint(s.Pool.QueryRow(ctx, `SELECT `+epCols+` FROM endpoints WHERE id=$1 AND deleted_at IS NULL`, id))
}

// CreateEndpoint inserts an endpoint with a fresh secret (or e.Secret if set, for seeding).
func (s *Store) CreateEndpoint(ctx context.Context, e *model.Endpoint) (*model.Endpoint, error) {
	if e.ID == "" {
		e.ID = model.NewID("ep")
	}
	if e.Secret == "" {
		e.Secret = NewSigningSecret()
	}
	enc, err := s.encrypt(e.Secret)
	if err != nil {
		return nil, err
	}
	return s.scanEndpoint(s.Pool.QueryRow(ctx, `
		INSERT INTO endpoints (id, tenant_id, name, url, description, event_types, rate_limit_rps, secret_enc)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (id) DO UPDATE SET updated_at = endpoints.updated_at
		RETURNING `+epCols,
		e.ID, e.TenantID, e.Name, e.URL, e.Description, e.EventTypes, e.RateLimitRPS, enc))
}

// EndpointPatch holds optional endpoint updates.
type EndpointPatch struct {
	Name         *string  `json:"name"`
	URL          *string  `json:"url"`
	Description  *string  `json:"description"`
	EventTypes   []string `json:"event_types"`
	RateLimitRPS *int     `json:"rate_limit_rps"`
	Paused       *bool    `json:"-"`
}

// UpdateEndpoint applies a patch.
func (s *Store) UpdateEndpoint(ctx context.Context, tenantID, id string, p EndpointPatch) (*model.Endpoint, error) {
	var types any
	if p.EventTypes != nil {
		types = p.EventTypes
	}
	return s.scanEndpoint(s.Pool.QueryRow(ctx, `
		UPDATE endpoints SET
		  name = COALESCE($3, name),
		  url = COALESCE($4, url),
		  description = COALESCE($5, description),
		  event_types = COALESCE($6::text[], event_types),
		  rate_limit_rps = COALESCE($7, rate_limit_rps),
		  paused = COALESCE($8, paused),
		  updated_at = now()
		WHERE tenant_id=$1 AND id=$2 AND deleted_at IS NULL RETURNING `+epCols,
		tenantID, id, p.Name, p.URL, p.Description, types, p.RateLimitRPS, p.Paused))
}

// DeleteEndpoint soft-deletes an endpoint.
func (s *Store) DeleteEndpoint(ctx context.Context, tenantID, id string) error {
	ct, err := s.Pool.Exec(ctx, `UPDATE endpoints SET deleted_at = now(), updated_at = now() WHERE tenant_id=$1 AND id=$2 AND deleted_at IS NULL`, tenantID, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RotateSecret makes a new secret current and keeps the old one valid for grace.
func (s *Store) RotateSecret(ctx context.Context, tenantID, id string, grace time.Duration) (*model.Endpoint, error) {
	cur, err := s.GetEndpoint(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.TenantID != tenantID {
		return nil, ErrNotFound
	}
	newEnc, err := s.encrypt(NewSigningSecret())
	if err != nil {
		return nil, err
	}
	prevEnc, err := s.encrypt(cur.Secret)
	if err != nil {
		return nil, err
	}
	return s.scanEndpoint(s.Pool.QueryRow(ctx, `
		UPDATE endpoints SET secret_enc=$3, previous_secret_enc=$4, previous_expires_at=$5, updated_at=now()
		WHERE tenant_id=$1 AND id=$2 AND deleted_at IS NULL RETURNING `+epCols,
		tenantID, id, newEnc, prevEnc, time.Now().Add(grace)))
}
