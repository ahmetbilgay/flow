package n8n

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestMemoryRateLimiter(t *testing.T) {
	l := &memoryRateLimiter{tat: map[string]time.Time{}}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if ok, _, _ := l.Allow(ctx, "k", 5, time.Minute); !ok {
			t.Fatalf("request %d of the burst refused", i+1)
		}
	}
	ok, wait, _ := l.Allow(ctx, "k", 5, time.Minute)
	if ok || wait <= 0 || wait > 12*time.Second {
		t.Fatalf("6th request: ok=%v wait=%v (one token refills every 12s)", ok, wait)
	}
	if ok, _, _ := l.Allow(ctx, "other", 5, time.Minute); !ok {
		t.Fatal("keys must not share a bucket")
	}
	// Tokens refill over the period.
	l.tat["k"] = time.Now().Add(time.Minute - 13*time.Second)
	if ok, _, _ := l.Allow(ctx, "k", 5, time.Minute); !ok {
		t.Fatal("a token should have refilled")
	}
}

type fixedLimiter struct{ calls []string }

func (f *fixedLimiter) Allow(_ context.Context, key string, limit int, _ time.Duration) (bool, time.Duration, error) {
	f.calls = append(f.calls, key+"/"+strconv.Itoa(limit))
	return false, 1500 * time.Millisecond, nil
}

func TestHandlerRateLimitResponse(t *testing.T) {
	f := &fixedLimiter{}
	h := NewHandler(nil, nil, HandlerOptions{Namespace: "cust_1", RateLimit: RateLimit{PerClient: 10, Limiter: f,
		ClientIP: func(r *http.Request) string { return r.Header.Get("X-Real-IP") }}})
	req := httptest.NewRequest("POST", "/webhook/anything", nil)
	req.Header.Set("X-Real-IP", "203.0.113.9")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("got %d Retry-After=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if len(f.calls) != 1 || f.calls[0] != "ip:cust_1:203.0.113.9/10" {
		t.Fatalf("limiter calls: %v", f.calls)
	}
}
