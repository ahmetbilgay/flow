//go:build integration

package n8n

import (
	"context"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/storage/integrationtest"
	"github.com/redis/go-redis/v9"
)

func TestRedisRateLimiter(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: integrationtest.RedisAddr()})
	defer func() { _ = client.Close() }()
	ctx := context.Background()
	integrationtest.SkipIfUnavailable(t, client.Ping(ctx).Err())
	key := "test:" + integrationtest.UniqueNamespace()
	// Two limiters on one Redis behave like two replicas sharing a limit.
	a, b := NewRedisRateLimiter(client, ""), NewRedisRateLimiter(client, "")
	for i := 0; i < 4; i++ {
		l := a
		if i%2 == 1 {
			l = b
		}
		if ok, _, err := l.Allow(ctx, key, 4, time.Minute); err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v", i+1, ok, err)
		}
	}
	ok, wait, err := b.Allow(ctx, key, 4, time.Minute)
	if err != nil || ok || wait <= 0 || wait > 15*time.Second {
		t.Fatalf("5th request: ok=%v wait=%v err=%v", ok, wait, err)
	}
	ttl, err := client.PTTL(ctx, "flow:n8n:rl:"+key).Result()
	if err != nil || ttl <= 0 || ttl > time.Minute+time.Second {
		t.Fatalf("key must expire on its own: ttl=%v err=%v", ttl, err)
	}
}
