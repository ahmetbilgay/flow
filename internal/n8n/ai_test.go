package n8n

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

func lc(name, typ string, version float64, params map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"name": name, "type": "@n8n/n8n-nodes-langchain." + typ, "typeVersion": version, "parameters": params}
}

func withCred(n map[string]interface{}, typ string) map[string]interface{} {
	n["credentials"] = map[string]interface{}{typ: map[string]interface{}{"id": "c1", "name": typ}}
	return n
}

// fakeLLM replies with a tool call first, then a final answer, and records
// request bodies.
type fakeLLM struct {
	mu       sync.Mutex
	requests []map[string]interface{}
	srv      *httptest.Server
}

func newFakeLLM(t *testing.T, reply func(n int, body map[string]interface{}) interface{}) *fakeLLM {
	f := &fakeLLM{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]interface{}
		_ = json.Unmarshal(raw, &body)
		body["__path"] = r.URL.Path
		body["__auth"] = r.Header.Get("Authorization") + r.Header.Get("x-api-key") + r.Header.Get("x-goog-api-key")
		f.mu.Lock()
		n := len(f.requests)
		f.requests = append(f.requests, body)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply(n, body))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func agentWorkflow(t *testing.T, modelType, credType string, modelParams map[string]interface{}) []byte {
	return wfJSON(t, []map[string]interface{}{
		lc("Chat", "chatTrigger", 1.1, nil),
		lc("Agent", "agent", 1.7, map[string]interface{}{"options": map[string]interface{}{"systemMessage": "You are a math bot"}}),
		withCred(lc("Model", modelType, 1, modelParams), credType),
		lc("Calc", "toolCalculator", 1, nil),
		lc("Memory", "memoryBufferWindow", 1.3, map[string]interface{}{"contextWindowLength": 3}),
	}, "Chat->Agent", "Model~ai_languageModel->Agent", "Calc~ai_tool->Agent", "Memory~ai_memory->Agent")
}

func aiHarness(t *testing.T, credType, url string) *harness {
	creds := NewStaticCredentials()
	creds.Set(credType, "*", map[string]interface{}{"apiKey": "k-123", "url": url, "host": url})
	return newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}, ChatMemory: &memStore{data: map[string][]ChatMessage{}}})
}

func TestAgentOpenAIToolLoopAndMemory(t *testing.T) {
	f := newFakeLLM(t, func(n int, body map[string]interface{}) interface{} {
		msgs := asList(body["messages"])
		last, _ := msgs[len(msgs)-1].(map[string]interface{})
		if last["role"] == "tool" {
			return map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant", "content": "The answer is " + toString(last["content"])}}}}
		}
		return map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{
			"role": "assistant", "content": nil,
			"tool_calls": []interface{}{map[string]interface{}{"id": "call_1", "type": "function", "function": map[string]interface{}{"name": "calculator", "arguments": `{"input":"6*7"}`}}},
		}}}}
	})
	h := aiHarness(t, "openAiApi", f.srv.URL)
	data := agentWorkflow(t, "lmChatOpenAi", "openAiApi", map[string]interface{}{"model": map[string]interface{}{"__rl": true, "value": "gpt-4o-mini", "mode": "list"}})

	_, steps := h.run(t, data, `{"sessionId":"s1","chatInput":"what is 6 times 7?"}`)
	if got := itemsOf(t, steps["Agent"], 0); len(got) != 1 || got[0]["output"] != "The answer is 42" {
		t.Fatalf("agent output: %v", got)
	}
	first := f.requests[0]
	if first["__path"] != "/chat/completions" || first["__auth"] != "Bearer k-123" || first["model"] != "gpt-4o-mini" {
		t.Fatalf("request: %v", first)
	}
	tools := asList(first["tools"])
	if len(tools) != 1 || !strings.Contains(mustJSON(tools[0]), `"calculator"`) {
		t.Fatalf("tools: %v", tools)
	}

	// Second turn in the same session sees the first exchange.
	_, _ = h.run(t, data, `{"sessionId":"s1","chatInput":"and again?"}`)
	msgs := asList(f.requests[2]["messages"])
	joined := mustJSON(msgs)
	if !strings.Contains(joined, "what is 6 times 7?") || !strings.Contains(joined, "The answer is 42") {
		t.Fatalf("memory not applied: %s", joined)
	}
}

func TestAgentAnthropicAndGemini(t *testing.T) {
	anth := newFakeLLM(t, func(n int, body map[string]interface{}) interface{} {
		if n == 0 {
			return map[string]interface{}{"content": []interface{}{map[string]interface{}{"type": "tool_use", "id": "tu1", "name": "calculator", "input": map[string]interface{}{"input": "2+3"}}}}
		}
		return map[string]interface{}{"content": []interface{}{map[string]interface{}{"type": "text", "text": "five"}}}
	})
	h := aiHarness(t, "anthropicApi", anth.srv.URL)
	data := agentWorkflow(t, "lmChatAnthropic", "anthropicApi", map[string]interface{}{"model": "claude-x"})
	_, steps := h.run(t, data, `{"sessionId":"a","chatInput":"2+3?"}`)
	if got := itemsOf(t, steps["Agent"], 0); got[0]["output"] != "five" {
		t.Fatalf("anthropic: %v", got)
	}
	second := mustJSON(anth.requests[1])
	if anth.requests[0]["system"] != "You are a math bot" || !strings.Contains(second, `"tool_result"`) || !strings.Contains(second, `"content":"5"`) {
		t.Fatalf("anthropic request shape: %v / %s", anth.requests[0]["system"], second)
	}

	gem := newFakeLLM(t, func(n int, body map[string]interface{}) interface{} {
		part := map[string]interface{}{"text": "done"}
		if n == 0 {
			part = map[string]interface{}{"functionCall": map[string]interface{}{"name": "calculator", "args": map[string]interface{}{"input": "10/4"}}}
		}
		return map[string]interface{}{"candidates": []interface{}{map[string]interface{}{"content": map[string]interface{}{"role": "model", "parts": []interface{}{part}}}}}
	})
	h = aiHarness(t, "googlePalmApi", gem.srv.URL)
	data = agentWorkflow(t, "lmChatGoogleGemini", "googlePalmApi", map[string]interface{}{"modelName": "models/gemini-test"})
	_, steps = h.run(t, data, `{"sessionId":"g","chatInput":"10/4?"}`)
	if got := itemsOf(t, steps["Agent"], 0); got[0]["output"] != "done" {
		t.Fatalf("gemini: %v", got)
	}
	if gem.requests[0]["__path"] != "/v1beta/models/gemini-test:generateContent" || !strings.Contains(mustJSON(gem.requests[1]), `"content":"2.5"`) {
		t.Fatalf("gemini request: %v / %s", gem.requests[0]["__path"], mustJSON(gem.requests[1]))
	}
}

func TestChainWithStructuredParserAndClassifier(t *testing.T) {
	f := newFakeLLM(t, func(n int, body map[string]interface{}) interface{} {
		sys := mustJSON(body["messages"])
		content := "```json\n{\"city\": \"Paris\", \"population\": 2100000}\n```"
		if strings.Contains(sys, "categories") {
			content = `{"categories": ["Billing"]}`
		}
		return map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant", "content": content}}}}
	})
	h := aiHarness(t, "openAiApi", f.srv.URL)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		lc("Chain", "chainLlm", 1.5, map[string]interface{}{"promptType": "define", "text": "=Tell me about {{ $json.q }}", "hasOutputParser": true}),
		withCred(lc("Model", "lmChatOpenAi", 1, nil), "openAiApi"),
		lc("Parser", "outputParserStructured", 1.2, map[string]interface{}{"jsonSchemaExample": `{"city":"x","population":1}`}),
		lc("Route", "textClassifier", 1, map[string]interface{}{"inputText": "={{ $('Start').item.json.q }}", "categories": map[string]interface{}{"categories": []interface{}{
			map[string]interface{}{"category": "Sales"}, map[string]interface{}{"category": "Billing"}}}}),
		withCred(lc("Model2", "lmChatOpenAi", 1, nil), "openAiApi"),
		nd("Billing", "noOp", 1, nil),
	}, "Start->Chain", "Model~ai_languageModel->Chain", "Parser~ai_outputParser->Chain", "Chain->Route", "Model2~ai_languageModel->Route", "Route:1->Billing")
	_, steps := h.run(t, data, `{"q":"Paris"}`)
	got := itemsOf(t, steps["Chain"], 0)
	out, _ := got[0]["output"].(map[string]interface{})
	if out["city"] != "Paris" || out["population"] != 2100000.0 {
		t.Fatalf("parsed output: %v", got)
	}
	if !strings.Contains(mustJSON(f.requests[0]["messages"]), "Tell me about Paris") {
		t.Fatalf("prompt not resolved: %v", f.requests[0]["messages"])
	}
	if b := itemsOf(t, steps["Billing"], 0); len(b) != 1 {
		t.Fatalf("classifier should route to Billing: %v", b)
	}
}
