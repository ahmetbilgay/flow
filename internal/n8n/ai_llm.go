package n8n

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/node"
)

// chatMsg is a provider-neutral chat message.
type chatMsg struct {
	Role       string // system | user | assistant | tool
	Content    string
	ToolCalls  []toolCall // assistant tool requests
	ToolCallID string     // tool result reference
	ToolName   string
}

type toolCall struct {
	ID   string
	Name string
	Args string // JSON object
}

type toolDef struct {
	Name        string
	Description string
	Schema      map[string]interface{}
}

type llmRequest struct {
	Messages    []chatMsg
	Tools       []toolDef
	Temperature *float64
	MaxTokens   int
	JSONMode    bool
}

type llmResponse struct {
	Content   string
	ToolCalls []toolCall
	Raw       map[string]interface{}
}

type chatModel interface {
	Chat(ctx context.Context, req llmRequest) (*llmResponse, error)
}

// llmHTTP posts JSON with small retries on 429/5xx.
func llmHTTP(ctx context.Context, client *http.Client, limit int64, url string, headers map[string]string, body interface{}) (map[string]interface{}, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, sanitizeErr(err)
		}
		raw, err := node.ReadLimited(resp.Body, limit)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("model API returned %d: %s", resp.StatusCode, truncate(string(raw), 300))
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, permanentError{fmt.Sprintf("model API returned %d: %s", resp.StatusCode, truncate(string(raw), 500))}
		}
		var out map[string]interface{}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("model API returned invalid JSON: %w", err)
		}
		return out, nil
	}
	return nil, lastErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---------------------------------------------------------------- OpenAI compatible

type openAIModel struct {
	client  *http.Client
	limit   int64
	baseURL string
	headers map[string]string
	model   string
	extra   map[string]interface{}
}

func (m *openAIModel) Chat(ctx context.Context, req llmRequest) (*llmResponse, error) {
	var msgs []map[string]interface{}
	for _, cm := range req.Messages {
		msg := map[string]interface{}{"role": cm.Role, "content": cm.Content}
		if cm.Role == "tool" {
			msg["tool_call_id"] = cm.ToolCallID
		}
		if len(cm.ToolCalls) > 0 {
			var calls []map[string]interface{}
			for _, tc := range cm.ToolCalls {
				calls = append(calls, map[string]interface{}{"id": tc.ID, "type": "function", "function": map[string]interface{}{"name": tc.Name, "arguments": tc.Args}})
			}
			msg["tool_calls"] = calls
			if cm.Content == "" {
				msg["content"] = nil
			}
		}
		msgs = append(msgs, msg)
	}
	body := map[string]interface{}{"model": m.model, "messages": msgs}
	for k, v := range m.extra {
		body[k] = v
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if req.MaxTokens > 0 {
		body["max_tokens"] = req.MaxTokens
	}
	if req.JSONMode && len(req.Tools) == 0 {
		body["response_format"] = map[string]interface{}{"type": "json_object"}
	}
	if len(req.Tools) > 0 {
		var tools []map[string]interface{}
		for _, t := range req.Tools {
			tools = append(tools, map[string]interface{}{"type": "function", "function": map[string]interface{}{"name": t.Name, "description": t.Description, "parameters": t.Schema}})
		}
		body["tools"] = tools
	}
	out, err := llmHTTP(ctx, m.client, m.limit, m.baseURL+"/chat/completions", m.headers, body)
	if err != nil {
		return nil, err
	}
	choices, _ := out["choices"].([]interface{})
	if len(choices) == 0 {
		return nil, fmt.Errorf("model returned no choices")
	}
	c0, _ := choices[0].(map[string]interface{})
	msg, _ := c0["message"].(map[string]interface{})
	res := &llmResponse{Content: toString(msg["content"]), Raw: c0}
	for _, raw := range asList(msg["tool_calls"]) {
		tc, _ := raw.(map[string]interface{})
		fn, _ := tc["function"].(map[string]interface{})
		res.ToolCalls = append(res.ToolCalls, toolCall{ID: toString(tc["id"]), Name: toString(fn["name"]), Args: toString(fn["arguments"])})
	}
	return res, nil
}

// ---------------------------------------------------------------- Anthropic

type anthropicModel struct {
	client  *http.Client
	limit   int64
	baseURL string
	apiKey  string
	model   string
}

func (m *anthropicModel) Chat(ctx context.Context, req llmRequest) (*llmResponse, error) {
	var system []string
	var msgs []map[string]interface{}
	for _, cm := range req.Messages {
		switch cm.Role {
		case "system":
			system = append(system, cm.Content)
		case "tool":
			msgs = append(msgs, map[string]interface{}{"role": "user", "content": []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": cm.ToolCallID, "content": cm.Content}}})
		case "assistant":
			var blocks []interface{}
			if cm.Content != "" {
				blocks = append(blocks, map[string]interface{}{"type": "text", "text": cm.Content})
			}
			for _, tc := range cm.ToolCalls {
				var input map[string]interface{}
				_ = json.Unmarshal([]byte(tc.Args), &input)
				if input == nil {
					input = map[string]interface{}{}
				}
				blocks = append(blocks, map[string]interface{}{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": input})
			}
			msgs = append(msgs, map[string]interface{}{"role": "assistant", "content": blocks})
		default:
			msgs = append(msgs, map[string]interface{}{"role": "user", "content": cm.Content})
		}
	}
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	body := map[string]interface{}{"model": m.model, "messages": msgs, "max_tokens": maxTokens}
	if len(system) > 0 {
		body["system"] = strings.Join(system, "\n\n")
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if len(req.Tools) > 0 {
		var tools []map[string]interface{}
		for _, t := range req.Tools {
			tools = append(tools, map[string]interface{}{"name": t.Name, "description": t.Description, "input_schema": t.Schema})
		}
		body["tools"] = tools
	}
	headers := map[string]string{"x-api-key": m.apiKey, "anthropic-version": "2023-06-01"}
	out, err := llmHTTP(ctx, m.client, m.limit, m.baseURL+"/v1/messages", headers, body)
	if err != nil {
		return nil, err
	}
	res := &llmResponse{Raw: out}
	var texts []string
	for _, raw := range asList(out["content"]) {
		b, _ := raw.(map[string]interface{})
		switch b["type"] {
		case "text":
			texts = append(texts, toString(b["text"]))
		case "tool_use":
			args, _ := json.Marshal(b["input"])
			res.ToolCalls = append(res.ToolCalls, toolCall{ID: toString(b["id"]), Name: toString(b["name"]), Args: string(args)})
		}
	}
	res.Content = strings.Join(texts, "")
	return res, nil
}

// ---------------------------------------------------------------- Gemini

type geminiModel struct {
	client  *http.Client
	limit   int64
	baseURL string
	apiKey  string
	model   string
}

func (m *geminiModel) Chat(ctx context.Context, req llmRequest) (*llmResponse, error) {
	var system []string
	var contents []map[string]interface{}
	for _, cm := range req.Messages {
		switch cm.Role {
		case "system":
			system = append(system, cm.Content)
		case "assistant":
			var parts []interface{}
			if cm.Content != "" {
				parts = append(parts, map[string]interface{}{"text": cm.Content})
			}
			for _, tc := range cm.ToolCalls {
				var args map[string]interface{}
				_ = json.Unmarshal([]byte(tc.Args), &args)
				parts = append(parts, map[string]interface{}{"functionCall": map[string]interface{}{"name": tc.Name, "args": args}})
			}
			contents = append(contents, map[string]interface{}{"role": "model", "parts": parts})
		case "tool":
			contents = append(contents, map[string]interface{}{"role": "user", "parts": []interface{}{
				map[string]interface{}{"functionResponse": map[string]interface{}{"name": cm.ToolName, "response": map[string]interface{}{"content": cm.Content}}}}})
		default:
			contents = append(contents, map[string]interface{}{"role": "user", "parts": []interface{}{map[string]interface{}{"text": cm.Content}}})
		}
	}
	body := map[string]interface{}{"contents": contents}
	if len(system) > 0 {
		body["systemInstruction"] = map[string]interface{}{"parts": []interface{}{map[string]interface{}{"text": strings.Join(system, "\n\n")}}}
	}
	gen := map[string]interface{}{}
	if req.Temperature != nil {
		gen["temperature"] = *req.Temperature
	}
	if req.MaxTokens > 0 {
		gen["maxOutputTokens"] = req.MaxTokens
	}
	if req.JSONMode && len(req.Tools) == 0 {
		gen["responseMimeType"] = "application/json"
	}
	if len(gen) > 0 {
		body["generationConfig"] = gen
	}
	if len(req.Tools) > 0 {
		var decls []interface{}
		for _, t := range req.Tools {
			decls = append(decls, map[string]interface{}{"name": t.Name, "description": t.Description, "parameters": geminiSchema(t.Schema)})
		}
		body["tools"] = []interface{}{map[string]interface{}{"functionDeclarations": decls}}
	}
	model := m.model
	if !strings.HasPrefix(model, "models/") {
		model = "models/" + model
	}
	out, err := llmHTTP(ctx, m.client, m.limit, m.baseURL+"/v1beta/"+model+":generateContent", map[string]string{"x-goog-api-key": m.apiKey}, body)
	if err != nil {
		return nil, err
	}
	cands := asList(out["candidates"])
	if len(cands) == 0 {
		return nil, fmt.Errorf("model returned no candidates: %s", truncate(mustJSON(out), 300))
	}
	c0, _ := cands[0].(map[string]interface{})
	content, _ := c0["content"].(map[string]interface{})
	res := &llmResponse{Raw: c0}
	var texts []string
	for i, raw := range asList(content["parts"]) {
		p, _ := raw.(map[string]interface{})
		if t, ok := p["text"].(string); ok {
			texts = append(texts, t)
		}
		if fc, ok := p["functionCall"].(map[string]interface{}); ok {
			args, _ := json.Marshal(fc["args"])
			res.ToolCalls = append(res.ToolCalls, toolCall{ID: fmt.Sprintf("call_%d", i), Name: toString(fc["name"]), Args: string(args)})
		}
	}
	res.Content = strings.Join(texts, "")
	return res, nil
}

// geminiSchema removes JSON-schema keywords Gemini rejects.
func geminiSchema(s map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range s {
		switch k {
		case "additionalProperties", "$schema", "default", "examples":
			continue
		}
		switch t := v.(type) {
		case map[string]interface{}:
			out[k] = geminiSchema(t)
		case []interface{}:
			var l []interface{}
			for _, x := range t {
				if m, ok := x.(map[string]interface{}); ok {
					l = append(l, geminiSchema(m))
				} else {
					l = append(l, x)
				}
			}
			out[k] = l
		default:
			out[k] = v
		}
	}
	return out
}

// ---------------------------------------------------------------- model sub-nodes

var openAICompatible = map[string]struct {
	cred    string
	baseURL string
}{
	"lmChatOpenAi":          {"openAiApi", "https://api.openai.com/v1"},
	"lmOpenAi":              {"openAiApi", "https://api.openai.com/v1"},
	"lmChatOpenRouter":      {"openRouterApi", "https://openrouter.ai/api/v1"},
	"lmChatDeepSeek":        {"deepSeekApi", "https://api.deepseek.com"},
	"lmChatGroq":            {"groqApi", "https://api.groq.com/openai/v1"},
	"lmChatXAiGrok":         {"xAiApi", "https://api.x.ai/v1"},
	"lmChatMistralCloud":    {"mistralCloudApi", "https://api.mistral.ai/v1"},
	"lmChatOllama":          {"ollamaApi", "http://localhost:11434"},
	"lmChatVercelAiGateway": {"vercelAiGatewayApi", "https://ai-gateway.vercel.sh/v1"},
}

func modelSubNodeSupported(typ string) bool {
	s := short(typ)
	_, ok := openAICompatible[s]
	return ok || s == "lmChatAnthropic" || s == "lmChatGoogleGemini"
}

// subNode is an AI sub-node folded into its root node's config.
type subNode struct {
	typ    string
	config map[string]interface{}
}

func (s subNode) params() map[string]interface{} {
	p, _ := s.config[cfgParams].(map[string]interface{})
	if p == nil {
		return map[string]interface{}{}
	}
	return p
}

func (s subNode) name() string { return toString(s.config[cfgName]) }

func aiSubNodes(n map[string]interface{}, connType string) []subNode {
	ai, _ := n[cfgAI].(map[string]interface{})
	var out []subNode
	for _, raw := range asList(ai[connType]) {
		m, _ := raw.(map[string]interface{})
		cfg, _ := m["config"].(map[string]interface{})
		out = append(out, subNode{typ: toString(m["type"]), config: cfg})
	}
	return out
}

// resolveSub resolves a sub-node's parameters for item i using the root's
// runtime (n8n evaluates sub-node expressions in the root item's context).
func (ec *execCtx) resolveSub(s subNode, i int) (map[string]interface{}, error) {
	p := s.params()
	if !containsExpr(p) {
		return p, nil
	}
	rt, err := ec.runtime()
	if err != nil {
		return nil, err
	}
	rt.setItem(i)
	v, err := rt.resolve(p)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", s.name(), err)
	}
	m, _ := v.(map[string]interface{})
	return m, nil
}

// subCredential loads a credential referenced by a sub-node.
func (ec *execCtx) subCredential(s subNode, credType string) (map[string]interface{}, error) {
	saved := ec.node
	defer func() { ec.node = saved }()
	tmp := *ec.node
	tmp.Config = s.config
	tmp.Name = s.name()
	ec.node = &tmp
	return ec.credential(credType)
}

func rlValue(v interface{}) string {
	if m, ok := v.(map[string]interface{}); ok {
		return toString(m["value"])
	}
	return toString(v)
}

// buildModel creates the chat model of a root node for item i.
func (ec *execCtx) buildModel(i int) (chatModel, llmRequest, error) {
	subs := aiSubNodes(ec.node.Config, "ai_languageModel")
	if len(subs) == 0 {
		return nil, llmRequest{}, permanentError{fmt.Sprintf("%s: no chat model connected", ec.node.Name)}
	}
	s := subs[0]
	p, err := ec.resolveSub(s, i)
	if err != nil {
		return nil, llmRequest{}, err
	}
	opts, _ := p["options"].(map[string]interface{})
	var base llmRequest
	if t, ok := toFloat(opts["temperature"]); ok {
		base.Temperature = &t
	}
	for _, k := range []string{"maxTokens", "maxTokensToSample", "maxOutputTokens"} {
		if n, ok := toFloat(opts[k]); ok && n > 0 {
			base.MaxTokens = int(n)
		}
	}
	if toString(opts["responseFormat"]) == "json_object" {
		base.JSONMode = true
	}
	client := outboundClient(ec.opts.Outbound)
	limit := ec.opts.Outbound.MaxResponseBytesOrDefault()
	st := short(s.typ)
	if spec, ok := openAICompatible[st]; ok {
		c, err := ec.subCredential(s, spec.cred)
		if err != nil {
			return nil, base, err
		}
		model := rlValue(p["model"])
		if model == "" {
			model = "gpt-4o-mini"
		}
		baseURL := credBaseURL(c, spec.baseURL)
		if u := toString(opts["baseURL"]); u != "" {
			baseURL = strings.TrimRight(u, "/")
		}
		if st == "lmChatOllama" && !strings.HasSuffix(baseURL, "/v1") {
			baseURL += "/v1"
		}
		headers := map[string]string{}
		if key := credString(c, "apiKey"); key != "" {
			headers["Authorization"] = "Bearer " + key
		}
		extra := map[string]interface{}{}
		for _, k := range []string{"topP", "frequencyPenalty", "presencePenalty"} {
			if v, ok := toFloat(opts[k]); ok {
				extra[map[string]string{"topP": "top_p", "frequencyPenalty": "frequency_penalty", "presencePenalty": "presence_penalty"}[k]] = v
			}
		}
		return &openAIModel{client: client, limit: limit, baseURL: baseURL, headers: headers, model: model, extra: extra}, base, nil
	}
	switch st {
	case "lmChatAnthropic":
		c, err := ec.subCredential(s, "anthropicApi")
		if err != nil {
			return nil, base, err
		}
		model := rlValue(p["model"])
		if model == "" {
			model = "claude-3-5-sonnet-latest"
		}
		return &anthropicModel{client: client, limit: limit, baseURL: credBaseURL(c, "https://api.anthropic.com"), apiKey: credString(c, "apiKey"), model: model}, base, nil
	case "lmChatGoogleGemini":
		c, err := ec.subCredential(s, "googlePalmApi")
		if err != nil {
			return nil, base, err
		}
		model := toString(p["modelName"])
		if model == "" {
			model = "models/gemini-1.5-flash"
		}
		return &geminiModel{client: client, limit: limit, baseURL: credBaseURL(c, "https://generativelanguage.googleapis.com"), apiKey: credString(c, "apiKey"), model: model}, base, nil
	}
	return nil, base, permanentError{fmt.Sprintf("chat model %s is not supported", s.typ)}
}
