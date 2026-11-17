package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// DefaultMaxResponseBytes caps how much of an HTTP response body is read.
const DefaultMaxResponseBytes int64 = 10 << 20 // 10 MiB

// ErrDestinationBlocked is returned when an outbound request targets an
// address the OutboundPolicy does not allow (e.g. loopback, private ranges,
// or cloud metadata endpoints).
var ErrDestinationBlocked = errors.New("outbound destination blocked by policy")

// ErrResponseTooLarge is returned when a response body exceeds the limit.
var ErrResponseTooLarge = errors.New("response body exceeds size limit")

// policyError wraps policy violations. They are permanent: retrying cannot
// succeed, so the worker fails the step immediately.
type policyError struct {
	kind   error
	detail string
}

func (e *policyError) Error() string   { return e.kind.Error() + ": " + e.detail }
func (e *policyError) Unwrap() error   { return e.kind }
func (e *policyError) Permanent() bool { return true }

// OutboundPolicy controls which destinations the http and notify nodes may
// reach. The zero value is the secure default: only public internet
// addresses over http/https, 30s timeout, 10 MiB response cap.
//
// Workflow definitions may come from untrusted sources (imported templates,
// tenant users), so by default nodes cannot be used to reach the host itself,
// the internal network, or cloud metadata services (SSRF).
type OutboundPolicy struct {
	// AllowPrivateNetworks permits loopback, RFC1918/ULA private, link-local
	// (including 169.254.169.254 metadata) and other non-public addresses.
	AllowPrivateNetworks bool
	// AllowedHosts are always reachable even when private: exact hostnames
	// ("billing.internal"), IPs, or CIDRs ("10.20.0.0/16").
	AllowedHosts []string
	// MaxResponseBytes caps response bodies; 0 means DefaultMaxResponseBytes,
	// negative means unlimited.
	MaxResponseBytes int64
	// Timeout is the whole-request timeout; 0 means 30s.
	Timeout time.Duration
}

func (p OutboundPolicy) maxBytes() int64 {
	if p.MaxResponseBytes == 0 {
		return DefaultMaxResponseBytes
	}
	return p.MaxResponseBytes
}

// guard holds the parsed policy.
type guard struct {
	policy  OutboundPolicy
	hosts   map[string]bool
	allowed []*net.IPNet
}

func newGuard(p OutboundPolicy) *guard {
	g := &guard{policy: p, hosts: map[string]bool{}}
	for _, h := range p.AllowedHosts {
		h = strings.TrimSpace(strings.ToLower(h))
		if h == "" {
			continue
		}
		if _, cidr, err := net.ParseCIDR(h); err == nil {
			g.allowed = append(g.allowed, cidr)
			continue
		}
		if ip := net.ParseIP(h); ip != nil {
			bits := 128
			if ip.To4() != nil {
				ip, bits = ip.To4(), 32
			}
			g.allowed = append(g.allowed, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		g.hosts[h] = true
	}
	return g
}

var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0).To4(), Mask: net.CIDRMask(10, 32)}

// IPv6 transition ranges that embed (possibly private) IPv4 addresses.
var transitionNets = func() []*net.IPNet {
	var out []*net.IPNet
	for _, c := range []string{"64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32"} {
		_, n, _ := net.ParseCIDR(c)
		out = append(out, n)
	}
	return out
}()

var safeRedirectHeaders = map[string]bool{
	"Accept": true, "Accept-Encoding": true, "Accept-Language": true, "User-Agent": true,
	"Content-Type": true, "Content-Length": true, "Referer": true,
}

// isPublicIP reports whether ip is a globally routable unicast address.
func isPublicIP(ip net.IP) bool {
	if v4 := ip.To4(); v4 != nil {
		ip = v4
		if v4[0] == 0 || cgnat.Contains(v4) || v4.Equal(net.IPv4bcast) {
			return false
		}
	}
	for _, n := range transitionNets {
		if n.Contains(ip) {
			return false
		}
	}
	return !ip.IsLoopback() && !ip.IsPrivate() && !ip.IsUnspecified() &&
		!ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() &&
		!ip.IsInterfaceLocalMulticast() && !ip.IsMulticast()
}

func (g *guard) ipAllowed(ip net.IP) bool {
	if g.policy.AllowPrivateNetworks || isPublicIP(ip) {
		return true
	}
	for _, n := range g.allowed {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// control runs after DNS resolution, right before connect, so it also covers
// redirects and DNS-rebinding tricks.
func (g *guard) control(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !g.ipAllowed(ip) {
		return &policyError{kind: ErrDestinationBlocked, detail: host}
	}
	return nil
}

// newClient builds an HTTP client that enforces the policy on every dial.
func newClient(p OutboundPolicy) *http.Client {
	g := newGuard(p)
	guarded := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second, Control: g.control}
	open := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}

	transport := &http.Transport{
		// Environment proxies are ignored on purpose: dialing a proxy would
		// bypass the per-destination check.
		Proxy: nil,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err == nil && g.hosts[strings.ToLower(host)] {
				return open.DialContext(ctx, network, addr)
			}
			return guarded.DialContext(ctx, network, addr)
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	timeout := p.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			// Go only drops Authorization/Cookie on cross-host redirects;
			// also drop custom credential headers (X-API-Key, apikey, …).
			if req.URL.Host != via[0].URL.Host {
				for k := range req.Header {
					if !safeRedirectHeaders[http.CanonicalHeaderKey(k)] {
						req.Header.Del(k)
					}
				}
			}
			return checkScheme(req.URL)
		},
	}
}

func checkScheme(u *url.URL) error {
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return nil
	default:
		return &policyError{kind: ErrDestinationBlocked, detail: fmt.Sprintf("unsupported scheme %q", u.Scheme)}
	}
}

// readLimited reads at most limit bytes (limit < 0: unlimited).
func readLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit < 0 {
		return io.ReadAll(r)
	}
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, &policyError{kind: ErrResponseTooLarge, detail: fmt.Sprintf("limit %d bytes", limit)}
	}
	return b, nil
}

// NewOutboundClient returns an HTTP client that enforces the policy on every
// connection (for other node packages that make outbound requests).
func NewOutboundClient(p OutboundPolicy) *http.Client { return newClient(p) }

// MaxResponseBytes returns the effective response cap of a policy
// (negative = unlimited).
func (p OutboundPolicy) MaxResponseBytesOrDefault() int64 { return p.maxBytes() }

// ReadLimited reads at most limit bytes (limit < 0: unlimited) and fails with
// ErrResponseTooLarge beyond that.
func ReadLimited(r io.Reader, limit int64) ([]byte, error) { return readLimited(r, limit) }

// CheckScheme rejects non-http(s) URLs with ErrDestinationBlocked.
func CheckScheme(u *url.URL) error { return checkScheme(u) }

// NewOutboundDialer returns a dial function that enforces the policy (for
// non-HTTP protocols such as SMTP).
func NewOutboundDialer(p OutboundPolicy) func(ctx context.Context, network, addr string) (net.Conn, error) {
	g := newGuard(p)
	guarded := &net.Dialer{Timeout: 15 * time.Second, Control: g.control}
	open := &net.Dialer{Timeout: 15 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err == nil && g.hosts[strings.ToLower(host)] {
			return open.DialContext(ctx, network, addr)
		}
		return guarded.DialContext(ctx, network, addr)
	}
}
