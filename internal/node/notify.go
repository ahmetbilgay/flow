package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// NotifyNode sends an HTTP webhook / notification to a target URL.
// It supports Go template rendering in both the URL and the body,
// using the step's input JSON as template data.
//
// Config:
//   - "url"     (string, required) : destination URL (can contain {{.field}} templates)
//   - "method"  (string, optional) : HTTP method, defaults to POST
//   - "headers" (map, optional)    : extra headers e.g. {"Authorization": "Bearer xyz"}
//   - "body"    (string, optional) : Go template for the request body
//     If not set, the raw input JSON is forwarded.
//
// Output: raw HTTP response body from the webhook endpoint
type NotifyNode struct {
	client *http.Client
}

// NewNotifyNode returns a notify node with the secure default OutboundPolicy
// (15s timeout).
func NewNotifyNode() *NotifyNode {
	return NewNotifyNodeWithPolicy(OutboundPolicy{Timeout: 15 * time.Second})
}

// NewNotifyNodeWithPolicy returns a notify node restricted by policy.
func NewNotifyNodeWithPolicy(p OutboundPolicy) *NotifyNode {
	if p.Timeout == 0 {
		p.Timeout = 15 * time.Second
	}
	return &NotifyNode{client: newClient(p)}
}

func (n *NotifyNode) Execute(ctx context.Context, config map[string]interface{}, input string) (string, error) {
	urlTmpl, _ := config["url"].(string)
	method, _ := config["method"].(string)
	if method == "" {
		method = "POST"
	}

	// Parse input as template data
	var inMap map[string]interface{}
	_ = json.Unmarshal([]byte(input), &inMap)
	if inMap == nil {
		inMap = map[string]interface{}{}
	}

	// Render URL template
	renderedURL, err := renderTemplateNamed("notify_url", urlTmpl, inMap)
	if err != nil {
		return "", fmt.Errorf("notify: url template error: %w", err)
	}

	// Render body template (or fall through to raw input)
	requestBody := input
	if bodyTmpl, ok := config["body"].(string); ok && bodyTmpl != "" {
		requestBody, err = renderTemplateNamed("notify_body", bodyTmpl, inMap)
		if err != nil {
			return "", fmt.Errorf("notify: body template error: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, renderedURL, bytes.NewBufferString(requestBody))
	if err != nil {
		return "", fmt.Errorf("notify: failed to build request: %w", err)
	}
	if err := checkScheme(req.URL); err != nil {
		return "", fmt.Errorf("notify: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Optional custom headers
	if headers, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprintf("%v", v))
		}
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("notify: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain (bounded) so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("notify: webhook returned HTTP %d", resp.StatusCode)
	}

	// Pass input downstream (notify is a side-effect node, not a data transformer)
	return input, nil
}

func (n *NotifyNode) Validate(config map[string]interface{}) error {
	if _, ok := config["url"].(string); !ok {
		return fmt.Errorf("notify: missing 'url' in config")
	}
	return nil
}
