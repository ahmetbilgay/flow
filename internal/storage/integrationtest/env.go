//go:build integration

package integrationtest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// MySQLDSN returns the integration MySQL DSN.
func MySQLDSN() string {
	return getenv("FLOW_MYSQL_DSN", "root:root@tcp(127.0.0.1:3306)/dgmos_flow?parseTime=true")
}

// PostgresDSN returns the integration PostgreSQL DSN.
func PostgresDSN() string {
	return getenv("FLOW_POSTGRES_DSN", "postgres://postgres:root@127.0.0.1:5432/dgmos_flow?sslmode=disable")
}

// RedisAddr returns the integration Redis address.
func RedisAddr() string {
	return getenv("FLOW_REDIS_ADDR", "127.0.0.1:6379")
}

// SkipIfUnavailable skips the test when a dependency is not reachable.
// Set FLOW_INTEGRATION_REQUIRED=1 to fail instead (used in CI).
func SkipIfUnavailable(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if os.Getenv("FLOW_INTEGRATION_REQUIRED") != "" {
		t.Fatalf("integration dependency required but unavailable: %v", err)
	}
	t.Skipf("integration dependency not available: %v", err)
}

// UniqueID returns a unique test identifier.
func UniqueID() string {
	return uuid.New().String()
}

// UniqueNamespace returns a unique tenant namespace for isolation.
func UniqueNamespace() string {
	return fmt.Sprintf("it-%s", uuid.New().String()[:8])
}

// PingRedis checks Redis connectivity.
func PingRedis(ctx context.Context, addr string) error {
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return c.Ping(ctx).Err()
}
