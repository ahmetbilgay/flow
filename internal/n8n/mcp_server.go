package n8n

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dgmos/flow/internal/models"
)

// mcpRequestKey carries an MCP request into the MCP Server Trigger's
// execution.
const mcpRequestKey = "__mcp"

// mcpTriggerNode answers one MCP request (tools/list or tools/call) with
// the tools connected to the MCP Server Trigger.
func mcpTriggerNode(ec *execCtx) ([][]Item, error) {
	items := ec.items()
	if len(items) == 0 {
		return [][]Item{nil}, nil
	}
	req := asMap(items[0].JSON[mcpRequestKey])
	if req == nil {
		return passthrough(ec)
	}
	tools, err := ec.buildTools(0)
	if err != nil {
		return nil, err
	}
	switch toString(req["method"]) {
	case "tools/list":
		list := make([]interface{}, len(tools))
		for k, t := range tools {
			schema := t.def.Schema
			if schema == nil {
				schema = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
			}
			list[k] = map[string]interface{}{"name": t.def.Name, "description": t.def.Description, "inputSchema": schema}
		}
		return [][]Item{{{JSON: map[string]interface{}{"tools": list}}}}, nil
	case "tools/call":
		name := toString(req["name"])
		for _, t := range tools {
			if t.def.Name != name {
				continue
			}
			args := asMap(req["arguments"])
			if args == nil {
				args = map[string]interface{}{}
			}
			out, err := t.run(ec.ctx, args)
			res := map[string]interface{}{"content": []interface{}{map[string]interface{}{"type": "text", "text": out}}}
			if err != nil {
				res = map[string]interface{}{"isError": true, "content": []interface{}{map[string]interface{}{"type": "text", "text": err.Error()}}}
			}
			return [][]Item{{{JSON: res}}}, nil
		}
		return [][]Item{{{JSON: map[string]interface{}{"isError": true, "content": []interface{}{map[string]interface{}{"type": "text", "text": "unknown tool " + name}}}}}}, nil
	}
	return nil, permanentError{fmt.Sprintf("MCP: method %q is not supported", req["method"])}
}

// ---------------------------------------------------------------- HTTP endpoint

type mcpSSESession struct {
	events chan []byte
	done   chan struct{}
}

type mcpSessions struct {
	mu sync.Mutex
	m  map[string]*mcpSSESession
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// serveMCP handles /mcp/{path} (Streamable HTTP), /mcp/{path}/sse and
// /mcp/{path}/messages (legacy SSE transport).
func (h *Handler) serveMCP(w http.ResponseWriter, r *http.Request, rest string) {
	path, sub := rest, ""
	for _, s := range []string{"/sse", "/messages"} {
		if strings.HasSuffix(rest, s) {
			path, sub = strings.TrimSuffix(rest, s), s
		}
	}
	routes, err := h.routes(r.Context(), false)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	target, ok := routes["mcp "+path]
	if !ok {
		if routes, err = h.routes(r.Context(), true); err == nil {
			target, ok = routes["mcp "+path]
		}
	}
	if !ok || !h.live(r.Context(), target) {
		httpError(w, http.StatusNotFound, fmt.Sprintf("MCP server %q is not registered", path))
		return
	}
	if !h.allowWorkflow(w, r, target.workflowID) {
		return
	}
	if err := h.authorize(r, target); err != nil {
		httpError(w, http.StatusUnauthorized, "Authorization data is wrong!")
		return
	}
	switch {
	case sub == "/sse" && r.Method == http.MethodGet:
		h.mcpSSE(w, r)
	case sub == "/messages" && r.Method == http.MethodPost:
		h.sessions.mu.Lock()
		s := h.sessions.m[r.URL.Query().Get("sessionId")]
		h.sessions.mu.Unlock()
		if s == nil {
			httpError(w, http.StatusNotFound, "unknown MCP session")
			return
		}
		msgs, batch, err := h.readRPC(w, r)
		if err != nil {
			return
		}
		w.WriteHeader(http.StatusAccepted)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), h.opts.ResponseTimeout)
			defer cancel()
			if out := h.handleRPC(ctx, target, msgs, batch); out != nil {
				select {
				case s.events <- out:
				case <-s.done:
				}
			}
		}()
	case sub == "" && r.Method == http.MethodPost:
		msgs, batch, err := h.readRPC(w, r)
		if err != nil {
			return
		}
		for _, m := range msgs {
			if m.Method == "initialize" {
				w.Header().Set("Mcp-Session-Id", randomID())
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), h.opts.ResponseTimeout)
		defer cancel()
		out := h.handleRPC(ctx, target, msgs, batch)
		if out == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	case sub == "" && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusOK)
	default:
		w.Header().Set("Allow", "POST")
		httpError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) readRPC(w http.ResponseWriter, r *http.Request) ([]rpcMsg, bool, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.opts.MaxBodyBytes)).Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, rpcMsg{JSONRPC: "2.0", Error: &rpcError{Code: -32700, Message: "parse error"}})
		return nil, false, err
	}
	var msgs []rpcMsg
	batch := strings.HasPrefix(strings.TrimSpace(string(raw)), "[")
	if batch {
		if err := json.Unmarshal(raw, &msgs); err != nil {
			writeJSON(w, http.StatusBadRequest, rpcMsg{JSONRPC: "2.0", Error: &rpcError{Code: -32600, Message: "invalid request"}})
			return nil, false, err
		}
	} else {
		var m rpcMsg
		if err := json.Unmarshal(raw, &m); err != nil {
			writeJSON(w, http.StatusBadRequest, rpcMsg{JSONRPC: "2.0", Error: &rpcError{Code: -32600, Message: "invalid request"}})
			return nil, false, err
		}
		msgs = []rpcMsg{m}
	}
	return msgs, batch, nil
}

// handleRPC answers JSON-RPC messages; notifications produce no output.
func (h *Handler) handleRPC(ctx context.Context, t routeTarget, msgs []rpcMsg, batch bool) []byte {
	var out []rpcMsg
	for _, m := range msgs {
		if m.ID == nil {
			continue
		}
		res := rpcMsg{JSONRPC: "2.0", ID: m.ID}
		switch m.Method {
		case "initialize":
			ver := toString(asMap(m.Params)["protocolVersion"])
			if ver == "" {
				ver = mcpProtocolVersion
			}
			res.Result, _ = json.Marshal(map[string]interface{}{"protocolVersion": ver, "capabilities": map[string]interface{}{"tools": map[string]interface{}{"listChanged": false}},
				"serverInfo": map[string]interface{}{"name": "flow-n8n-mcp", "version": "1.0.0"}})
		case "ping":
			res.Result = json.RawMessage(`{}`)
		case "tools/list", "tools/call":
			p := asMap(m.Params)
			req := map[string]interface{}{"method": m.Method}
			if m.Method == "tools/call" {
				req["name"], req["arguments"] = p["name"], p["arguments"]
			}
			result, err := h.runMCP(ctx, t, req)
			if err != nil {
				res.Error = &rpcError{Code: -32603, Message: redactSecrets(err.Error())}
			} else {
				res.Result, _ = json.Marshal(result)
			}
		default:
			res.Error = &rpcError{Code: -32601, Message: "method not found: " + m.Method}
		}
		out = append(out, res)
	}
	if len(out) == 0 {
		return nil
	}
	if batch {
		b, _ := json.Marshal(out)
		return b
	}
	b, _ := json.Marshal(out[0])
	return b
}

// runMCP runs the trigger's workflow for one request and returns the
// trigger step's result.
func (h *Handler) runMCP(ctx context.Context, t routeTarget, req map[string]interface{}) (map[string]interface{}, error) {
	input, _ := json.Marshal(map[string]interface{}{TriggerKey: t.node.Name, "items": []interface{}{map[string]interface{}{mcpRequestKey: req}}})
	id, err := h.eng.StartWorkflow(ctx, h.opts.Namespace, t.workflowID, string(input))
	if err != nil {
		return nil, err
	}
	wait := 20 * time.Millisecond
	for {
		exec, _, steps, err := h.eng.ExecutionSnapshot(ctx, h.opts.Namespace, id)
		if err == nil {
			for _, s := range steps {
				if s.NodeID == t.node.ID && s.Status == models.TaskCompleted {
					if items := OutputItems(s.Output); len(items) > 0 {
						return items[0].JSON, nil
					}
				}
			}
			if exec != nil && exec.Status == models.TaskFailed {
				return nil, fmt.Errorf("%s", exec.ErrorMessage)
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("MCP request timed out")
		case <-time.After(wait):
			if wait < 500*time.Millisecond {
				wait *= 2
			}
		}
	}
}

func (h *Handler) mcpSSE(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		httpError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	id := randomID()
	s := &mcpSSESession{events: make(chan []byte, 16), done: make(chan struct{})}
	h.sessions.mu.Lock()
	if h.sessions.m == nil {
		h.sessions.m = map[string]*mcpSSESession{}
	}
	h.sessions.m[id] = s
	h.sessions.mu.Unlock()
	defer func() {
		h.sessions.mu.Lock()
		delete(h.sessions.m, id)
		h.sessions.mu.Unlock()
		close(s.done)
	}()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "event: endpoint\ndata: messages?sessionId=%s\n\n", id)
	fl.Flush()
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case b := <-s.events:
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", b)
			fl.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}
