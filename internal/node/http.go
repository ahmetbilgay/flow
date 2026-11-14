package node

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

type HTTPNode struct {
	client   *http.Client
	maxBytes int64
}

// NewHTTPNode returns an HTTP node with the secure default OutboundPolicy.
func NewHTTPNode() *HTTPNode {
	return NewHTTPNodeWithPolicy(OutboundPolicy{})
}

// NewHTTPNodeWithPolicy returns an HTTP node restricted by policy.
func NewHTTPNodeWithPolicy(p OutboundPolicy) *HTTPNode {
	return &HTTPNode{client: newClient(p), maxBytes: p.maxBytes()}
}

func (h *HTTPNode) Execute(ctx context.Context, config map[string]interface{}, input string) (string, error) {
	url, _ := config["url"].(string)
	method, _ := config["method"].(string)
	if method == "" {
		method = "GET"
	}

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(input))
	if err != nil {
		return "", err
	}
	if err := checkScheme(req.URL); err != nil {
		return "", err
	}

	// Add headers from config
	if headers, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			req.Header.Add(k, fmt.Sprintf("%v", v))
		}
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := readLimited(resp.Body, h.maxBytes)
	if err != nil {
		return "", err
	}

	if resp.StatusCode >= 400 {
		return string(body), fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	return string(body), nil
}

func (h *HTTPNode) Validate(config map[string]interface{}) error {
	if _, ok := config["url"].(string); !ok {
		return fmt.Errorf("missing 'url' in http config")
	}
	return nil
}
