// Package config loads twelve-factor configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is shared by every Boomerang service. Each service only reads the fields it needs.
type Config struct {
	Service   string
	HTTPAddr  string
	LogLevel  string
	DemoMode  bool
	ServiceID string // unique per process (hostname by default), used as Kafka client id / worker id

	KafkaBrokers      []string
	RedisAddr         string
	PostgresURL       string // via PgBouncer for request paths
	PostgresDirectURL string // direct connection, used for migrations
	ClickHouseURL     string
	ClickHouseDB      string
	ClickHouseUser    string
	ClickHousePass    string

	MinIOEndpoint         string
	MinIOAccessKey        string
	MinIOSecretKey        string
	MinIOBucket           string
	MinIOUseSSL           bool
	PayloadInlineMaxBytes int

	// Delivery
	DeliveryTimeout           time.Duration
	WorkerEndpointInflight    int           // per worker, per endpoint concurrency
	WorkerBackpressureWait    time.Duration // how long a task waits for a slot before being deferred
	WorkerPartitionMaxPending int
	CircuitFailureThreshold   int
	CircuitCooldown           time.Duration
	DeferMaxAge               time.Duration // circuit/paused deferrals stop after this and go to the DLQ
	SecretRotationGrace       time.Duration

	// SSRF
	SSRFAllowHosts []string

	// Internal auth between services (receivers → api secrets lookup)
	InternalToken string
	// Key used to encrypt signing secrets at rest
	SecretsKey string

	// ShopSim
	GatewayURL    string
	ShopSimAPIKey string

	// Receivers
	APIInternalURL string

	// Scaler
	ScalerKind      string // docker | kubernetes | none
	DockerSocket    string
	ComposeProject  string
	WorkerService   string
	K8sNamespace    string
	K8sWorkerDeploy string

	ShopSimURL   string // api → shopsim (status in overview)
	ReceiversURL string // base URL of the mock receivers, as registered on endpoints

	OTLPEndpoint string
}

// Load reads configuration for the named service.
func Load(service string) Config {
	host, _ := os.Hostname()
	demo := envBool("DEMO_MODE", true)
	c := Config{
		Service:   service,
		HTTPAddr:  env("HTTP_ADDR", ":8080"),
		LogLevel:  env("LOG_LEVEL", "info"),
		DemoMode:  demo,
		ServiceID: env("SERVICE_ID", host),

		KafkaBrokers:      strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		RedisAddr:         env("REDIS_ADDR", "localhost:6379"),
		PostgresURL:       env("POSTGRES_URL", "postgres://boomerang:boomerang@localhost:5432/boomerang?sslmode=disable"),
		PostgresDirectURL: env("POSTGRES_DIRECT_URL", env("POSTGRES_URL", "postgres://boomerang:boomerang@localhost:5432/boomerang?sslmode=disable")),
		ClickHouseURL:     env("CLICKHOUSE_URL", "http://localhost:8123"),
		ClickHouseDB:      env("CLICKHOUSE_DB", "boomerang"),
		ClickHouseUser:    env("CLICKHOUSE_USER", "boomerang"),
		ClickHousePass:    env("CLICKHOUSE_PASSWORD", "boomerang"),

		MinIOEndpoint:         env("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:        env("MINIO_ACCESS_KEY", "boomerang"),
		MinIOSecretKey:        env("MINIO_SECRET_KEY", "boomerang-secret"),
		MinIOBucket:           env("MINIO_BUCKET", "boomerang-payloads"),
		MinIOUseSSL:           envBool("MINIO_USE_SSL", false),
		PayloadInlineMaxBytes: envInt("PAYLOAD_INLINE_MAX_BYTES", 64*1024),

		DeliveryTimeout:           envDur("DELIVERY_TIMEOUT", 5*time.Second),
		WorkerEndpointInflight:    envInt("WORKER_ENDPOINT_INFLIGHT", 128),
		WorkerBackpressureWait:    envDur("WORKER_BACKPRESSURE_WAIT", 5*time.Second),
		WorkerPartitionMaxPending: envInt("WORKER_PARTITION_MAX_PENDING", 2000),
		CircuitFailureThreshold:   envInt("CIRCUIT_FAILURE_THRESHOLD", 5),
		CircuitCooldown:           envDur("CIRCUIT_COOLDOWN", pick(demo, 10*time.Second, 60*time.Second)),
		DeferMaxAge:               envDur("DEFER_MAX_AGE", pick(demo, 10*time.Minute, 72*time.Hour)),
		SecretRotationGrace:       envDur("SECRET_ROTATION_GRACE", pick(demo, 5*time.Minute, 24*time.Hour)),

		SSRFAllowHosts: splitNonEmpty(env("SSRF_ALLOW_HOSTS", "")),
		InternalToken:  env("INTERNAL_TOKEN", "dev-internal-token"),
		SecretsKey:     env("SECRETS_ENCRYPTION_KEY", "dev-only-secrets-key-change-me"),

		GatewayURL:    env("GATEWAY_URL", "http://localhost:8080"),
		ShopSimAPIKey: env("SHOPSIM_API_KEY", "bmr_live_shopsim_demo_key_0000000000000000"),

		APIInternalURL: env("API_INTERNAL_URL", "http://localhost:8080"),

		ScalerKind:      env("SCALER", "none"),
		DockerSocket:    env("DOCKER_SOCKET", "/var/run/docker.sock"),
		ComposeProject:  env("COMPOSE_PROJECT", "boomerang"),
		WorkerService:   env("WORKER_SERVICE", "worker"),
		K8sNamespace:    env("K8S_NAMESPACE", "boomerang"),
		K8sWorkerDeploy: env("K8S_WORKER_DEPLOYMENT", "boomerang-worker"),

		ShopSimURL:   env("SHOPSIM_URL", "http://localhost:8090"),
		ReceiversURL: env("RECEIVERS_URL", "http://receivers:9000"),

		OTLPEndpoint: env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	}
	return c
}

// DefaultRetrySchedule is the backoff applied to new tenants.
func DefaultRetrySchedule(demo bool) []int {
	if demo {
		return []int{2000, 5000, 10000, 20000}
	}
	return []int{10_000, 60_000, 300_000, 1_800_000}
}

func pick[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := env(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s must be an integer: %v", k, err))
	}
	return n
}

func envBool(k string, def bool) bool {
	v := env(k, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s must be a boolean: %v", k, err))
	}
	return b
}

func envDur(k string, def time.Duration) time.Duration {
	v := env(k, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s must be a duration: %v", k, err))
	}
	return d
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
