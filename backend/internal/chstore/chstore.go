// Package chstore talks to ClickHouse over its HTTP interface. ClickHouse stores the
// write-heavy data: events, delivery state and every delivery attempt.
package chstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a minimal ClickHouse HTTP client with server-side query parameters.
type Client struct {
	base, db, user, pass string
	hc                   *http.Client
}

// New creates a client.
func New(base, db, user, pass string) *Client {
	return &Client{base: strings.TrimRight(base, "/"), db: db, user: user, pass: pass,
		hc: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 16}}}
}

func (c *Client) do(ctx context.Context, sql string, params map[string]string, body io.Reader, settings map[string]string) (io.ReadCloser, error) {
	q := url.Values{}
	if c.db != "" {
		q.Set("database", c.db)
	}
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	for k, v := range settings {
		q.Set(k, v)
	}
	var reqBody io.Reader
	if body != nil {
		q.Set("query", sql)
		reqBody = body
	} else {
		reqBody = strings.NewReader(sql)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/?"+q.Encode(), reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.pass)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("clickhouse: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	return resp.Body, nil
}

// Exec runs a statement.
func (c *Client) Exec(ctx context.Context, sql string, params map[string]string) error {
	rc, err := c.do(ctx, sql, params, nil, nil)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, rc)
	return rc.Close()
}

// ExecNoDB runs a statement without selecting the database (for CREATE DATABASE).
func (c *Client) ExecNoDB(ctx context.Context, sql string) error {
	c2 := *c
	c2.db = ""
	return c2.Exec(ctx, sql, nil)
}

// Query runs a SELECT and decodes each JSONEachRow line into a new T.
func Query[T any](ctx context.Context, c *Client, sql string, params map[string]string) ([]T, error) {
	rc, err := c.do(ctx, sql+" FORMAT JSONEachRow", params, nil, map[string]string{
		"output_format_json_quote_64bit_integers": "0",
	})
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var out []T
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var v T
		if err := json.Unmarshal(line, &v); err != nil {
			return nil, fmt.Errorf("decode row: %w", err)
		}
		out = append(out, v)
	}
	return out, sc.Err()
}

// Insert writes rows (structs with json tags matching column names) using JSONEachRow.
func (c *Client) Insert(ctx context.Context, table string, rows []any) error {
	if len(rows) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	rc, err := c.do(ctx, "INSERT INTO "+table+" FORMAT JSONEachRow", nil, &buf, map[string]string{
		"date_time_input_format":           "best_effort",
		"input_format_skip_unknown_fields": "1",
		"input_format_null_as_default":     "1",
		// Server-side batching: many small inserts from parallel partitions are merged
		// into few parts. The call still waits until the data is durable.
		"async_insert":                 "1",
		"wait_for_async_insert":        "1",
		"async_insert_busy_timeout_ms": "1000",
		"async_insert_max_data_size":   "16777216",
	})
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, rc)
	return rc.Close()
}

// Ping checks connectivity.
func (c *Client) Ping(ctx context.Context) error {
	return c.Exec(ctx, "SELECT 1", nil)
}

// Schema creates the database and tables.
func (c *Client) Migrate(ctx context.Context) error {
	if err := c.ExecNoDB(ctx, "CREATE DATABASE IF NOT EXISTS "+c.db); err != nil {
		return err
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS events (
			tenant_id LowCardinality(String),
			event_id String,
			event_type LowCardinality(String),
			created_at DateTime64(3, 'UTC'),
			body String CODEC(ZSTD(3)),
			body_ref String,
			body_bytes UInt32,
			idempotency_key String,
			ordering_key String
		) ENGINE = ReplacingMergeTree
		PARTITION BY toYYYYMMDD(created_at)
		ORDER BY (tenant_id, event_id)
		TTL toDateTime(created_at) + INTERVAL 30 DAY`,
		`CREATE TABLE IF NOT EXISTS deliveries (
			tenant_id LowCardinality(String),
			delivery_id String,
			event_id String,
			endpoint_id LowCardinality(String),
			endpoint_name LowCardinality(String),
			event_type LowCardinality(String),
			event_at DateTime64(3, 'UTC'),
			state LowCardinality(String),
			attempts UInt16,
			updated_at DateTime64(3, 'UTC'),
			version UInt64,
			INDEX idx_event event_id TYPE bloom_filter GRANULARITY 4
		) ENGINE = ReplacingMergeTree(version)
		PARTITION BY toYYYYMMDD(event_at)
		ORDER BY (tenant_id, delivery_id)
		TTL toDateTime(event_at) + INTERVAL 30 DAY`,
		`CREATE TABLE IF NOT EXISTS attempts (
			tenant_id LowCardinality(String),
			delivery_id String,
			event_id String,
			endpoint_id LowCardinality(String),
			endpoint_name LowCardinality(String),
			event_type LowCardinality(String),
			event_at DateTime64(3, 'UTC'),
			attempt UInt16,
			attempted UInt8,
			outcome LowCardinality(String),
			state LowCardinality(String),
			status_code UInt16,
			response_body String CODEC(ZSTD(3)),
			latency_ms UInt32,
			error String,
			reason String,
			next_retry_at Nullable(DateTime64(3, 'UTC')),
			signature String,
			worker_id LowCardinality(String),
			ts DateTime64(3, 'UTC'),
			INDEX idx_ts ts TYPE minmax GRANULARITY 1
		) ENGINE = MergeTree
		PARTITION BY toYYYYMMDD(ts)
		ORDER BY (tenant_id, event_id, ts)
		TTL toDateTime(ts) + INTERVAL 30 DAY`,
	}
	for _, s := range stmts {
		if err := c.Exec(ctx, s, nil); err != nil {
			return err
		}
	}
	return nil
}

// FormatTime renders a time for DateTime64 parameters.
func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000")
}
