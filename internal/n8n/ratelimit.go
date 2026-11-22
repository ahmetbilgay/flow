package n8n

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimit throttles the public entry points of N8NHandler (webhook, chat,
// form and MCP routes). Every request starts an execution, so without a
// limit anyone who knows a URL can fill the workers and spend the
// workflow owner's API credits.
type RateLimit struct {
	// PerWorkflow is how many requests per minute one workflow's routes
	// accept from all callers together. Zero means 600; negative disables.
	PerWorkflow int
	// PerClient is how many requests per minute one client IP may send to
	// the namespace's routes, unknown paths included. Zero disables. Behind
	// a load balancer set ClientIP, or every caller shares the proxy's IP.
	PerClient int
	// ClientIP returns the caller's IP (e.g. from X-Forwarded-For as set by
	// your trusted proxy). Nil uses the connection's remote address.
	ClientIP func(r *http.Request) string
	// Limiter keeps the counters. Nil keeps them in process memory, which
	// limits each replica separately; use NewRedisRateLimiter to share them.
	Limiter RateLimiter
}

// RateLimiter decides whether a request under key is within limit requests
// per period. When it is not, retryAfter says when the next one is.
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, period time.Duration) (ok bool, retryAfter time.Duration, err error)
}

const defaultPerWorkflow = 600

// memLimiter is shared by every handler of the process, so hosts that build
// a handler per request still keep their counters.
var memLimiter = &memoryRateLimiter{tat: map[string]time.Time{}}

// memoryRateLimiter is a GCRA limiter (a token bucket of size limit that
// refills over period) in process memory.
type memoryRateLimiter struct {
	mu     sync.Mutex
	tat    map[string]time.Time
	pruned time.Time
}

func (m *memoryRateLimiter) Allow(_ context.Context, key string, limit int, period time.Duration) (bool, time.Duration, error) {
	now := time.Now()
	interval := period / time.Duration(limit)
	m.mu.Lock()
	defer m.mu.Unlock()
	if now.Sub(m.pruned) > time.Minute {
		for k, t := range m.tat {
			if t.Before(now) {
				delete(m.tat, k)
			}
		}
		m.pruned = now
	}
	tat := m.tat[key]
	if tat.Before(now) {
		tat = now
	}
	next := tat.Add(interval)
	if wait := next.Sub(now) - period; wait > 0 {
		return false, wait, nil
	}
	m.tat[key] = next
	return true, 0, nil
}

// NewRedisRateLimiter returns a RateLimiter whose counters live in Redis, so
// all replicas share one limit. Keys are prefix + the handler's key and
// expire on their own. It uses the Redis server clock.
func NewRedisRateLimiter(client redis.UniversalClient, prefix string) RateLimiter {
	if prefix == "" {
		prefix = "flow:n8n:rl:"
	}
	return &redisRateLimiter{client: client, prefix: prefix}
}

type redisRateLimiter struct {
	client redis.UniversalClient
	prefix string
}

// GCRA in one round trip: KEYS[1] holds the theoretical arrival time in
// microseconds; ARGV are the emission interval and the burst window (µs).
var gcraScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local period = tonumber(ARGV[2])
local tat = tonumber(redis.call('GET', KEYS[1]) or now)
if tat < now then tat = now end
local nxt = tat + interval
local wait = nxt - now - period
if wait > 0 then return {0, wait} end
redis.call('SET', KEYS[1], nxt, 'PX', math.ceil((nxt - now) / 1000))
return {1, 0}
`)

func (l *redisRateLimiter) Allow(ctx context.Context, key string, limit int, period time.Duration) (bool, time.Duration, error) {
	interval := period.Microseconds() / int64(limit)
	res, err := gcraScript.Run(ctx, l.client, []string{l.prefix + key}, interval, period.Microseconds()).Int64Slice()
	if err != nil || len(res) != 2 {
		return true, 0, fmt.Errorf("rate limiter: %v", err)
	}
	return res[0] == 1, time.Duration(res[1]) * time.Microsecond, nil
}

func (h *Handler) limiter() RateLimiter {
	if h.opts.RateLimit.Limiter != nil {
		return h.opts.RateLimit.Limiter
	}
	return memLimiter
}

func (h *Handler) clientIP(r *http.Request) string {
	if f := h.opts.RateLimit.ClientIP; f != nil {
		if ip := f(r); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// allowClient applies PerClient; it writes a 429 and returns false when the
// caller is over the limit.
func (h *Handler) allowClient(w http.ResponseWriter, r *http.Request) bool {
	n := h.opts.RateLimit.PerClient
	if n <= 0 {
		return true
	}
	return h.allow(w, r, "ip:"+h.opts.Namespace+":"+h.clientIP(r), n)
}

// allowWorkflow applies PerWorkflow to a matched route.
func (h *Handler) allowWorkflow(w http.ResponseWriter, r *http.Request, workflowID string) bool {
	n := h.opts.RateLimit.PerWorkflow
	if n < 0 {
		return true
	}
	if n == 0 {
		n = defaultPerWorkflow
	}
	return h.allow(w, r, "wf:"+h.opts.Namespace+":"+workflowID, n)
}

func (h *Handler) allow(w http.ResponseWriter, r *http.Request, key string, limit int) bool {
	ok, wait, err := h.limiter().Allow(r.Context(), key, limit, time.Minute)
	if err != nil {
		// A limiter outage must not take every webhook down with it.
		return true
	}
	if ok {
		return true
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
	httpError(w, http.StatusTooManyRequests, "Too many requests, try again later.")
	return false
}
