package node

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type permanent interface{ Permanent() bool }

func TestHTTPNodeBlocksLoopbackByDefault(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("secret"))
	}))
	defer srv.Close()

	_, err := NewHTTPNode().Execute(context.Background(), map[string]interface{}{"url": srv.URL}, "")
	if !errors.Is(err, ErrDestinationBlocked) {
		t.Fatalf("want ErrDestinationBlocked, got %v", err)
	}
	var p permanent
	if !errors.As(err, &p) || !p.Permanent() {
		t.Fatal("policy errors must be permanent")
	}
}

func TestHTTPNodeBlocksRedirectToPrivate(t *testing.T) {
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("internal"))
	}))
	defer internal.Close()
	// "public" server allowed by hostname; it redirects to a loopback IP that
	// is not allowed. The redirect must be blocked at dial time.
	public := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, strings.Replace(internal.URL, "127.0.0.1", "[::1]", 1), http.StatusFound)
	}))
	defer public.Close()

	h := NewHTTPNodeWithPolicy(OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}})
	_, err := h.Execute(context.Background(), map[string]interface{}{"url": public.URL}, "")
	if !errors.Is(err, ErrDestinationBlocked) {
		t.Fatalf("redirect to non-allowed address should be blocked, got %v", err)
	}
}

func TestHTTPNodeAllowedHostAndCIDR(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	for _, allow := range []string{"127.0.0.1", "127.0.0.0/8"} {
		h := NewHTTPNodeWithPolicy(OutboundPolicy{AllowedHosts: []string{allow}})
		out, err := h.Execute(context.Background(), map[string]interface{}{"url": srv.URL}, "")
		if err != nil || out != `{"ok":true}` {
			t.Fatalf("allow %s: out=%q err=%v", allow, out, err)
		}
	}
	h := NewHTTPNodeWithPolicy(OutboundPolicy{AllowPrivateNetworks: true})
	if _, err := h.Execute(context.Background(), map[string]interface{}{"url": srv.URL}, ""); err != nil {
		t.Fatalf("AllowPrivateNetworks: %v", err)
	}
}

func TestHTTPNodeResponseLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 2048)))
	}))
	defer srv.Close()

	h := NewHTTPNodeWithPolicy(OutboundPolicy{AllowPrivateNetworks: true, MaxResponseBytes: 1024})
	_, err := h.Execute(context.Background(), map[string]interface{}{"url": srv.URL}, "")
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("want ErrResponseTooLarge, got %v", err)
	}
	h = NewHTTPNodeWithPolicy(OutboundPolicy{AllowPrivateNetworks: true, MaxResponseBytes: 2048})
	if out, err := h.Execute(context.Background(), map[string]interface{}{"url": srv.URL}, ""); err != nil || len(out) != 2048 {
		t.Fatalf("exact-limit body should pass: len=%d err=%v", len(out), err)
	}
}

func TestHTTPNodeRejectsNonHTTPScheme(t *testing.T) {
	_, err := NewHTTPNode().Execute(context.Background(), map[string]interface{}{"url": "ftp://example.com/x"}, "")
	if !errors.Is(err, ErrDestinationBlocked) {
		t.Fatalf("want ErrDestinationBlocked, got %v", err)
	}
}

func TestNotifyNodeBlocksMetadataAddress(t *testing.T) {
	n := NewNotifyNode()
	_, err := n.Execute(context.Background(), map[string]interface{}{"url": "http://169.254.169.254/latest/meta-data/"}, `{}`)
	if !errors.Is(err, ErrDestinationBlocked) {
		t.Fatalf("metadata endpoint must be blocked, got %v", err)
	}
}

func TestIsPublicIP(t *testing.T) {
	cases := map[string]bool{
		"8.8.8.8": true, "1.1.1.1": true, "2606:4700:4700::1111": true,
		"127.0.0.1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false,
		"169.254.169.254": false, "100.64.0.1": false, "0.0.0.0": false, "::1": false,
		"fc00::1": false, "fe80::1": false, "::ffff:127.0.0.1": false, "224.0.0.1": false,
	}
	for s, want := range cases {
		if got := isPublicIP(net.ParseIP(s)); got != want {
			t.Errorf("isPublicIP(%s) = %v, want %v", s, got, want)
		}
	}
}
