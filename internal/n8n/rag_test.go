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

// keyword embedding: dimension per topic word, so similar texts score high.
var topics = []string{"cat", "dog", "invoice", "refund", "pasta"}

func fakeEmbedding(text string) []float64 {
	v := make([]float64, len(topics)+1)
	t := strings.ToLower(text)
	for i, w := range topics {
		v[i] = float64(strings.Count(t, w))
	}
	v[len(topics)] = 0.01
	return v
}

func ragServer(t *testing.T) (*httptest.Server, *sync.Map) {
	seen := &sync.Map{}
	var points []map[string]interface{}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]interface{}
		_ = json.Unmarshal(b, &body)
		w.Header().Set("Content-Type", "application/json")
		seen.Store(r.Method+" "+r.URL.Path, string(b))
		switch {
		case strings.HasSuffix(r.URL.Path, "/embeddings"):
			var data []interface{}
			for i, in := range asList(body["input"]) {
				data = append(data, map[string]interface{}{"index": i, "embedding": fakeEmbedding(toString(in))})
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": data})
		case strings.HasSuffix(r.URL.Path, "/chat/completions"):
			msgs := mustJSON(body["messages"])
			answer := "no context"
			if strings.Contains(msgs, "Refunds are processed within 5 days") {
				answer = "Refunds take 5 days."
			}
			if strings.Contains(msgs, `"role":"tool"`) {
				answer = "Tool says: refunds take 5 days."
			} else if strings.Contains(mustJSON(body["tools"]), "policy_search") {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant",
					"tool_calls": []interface{}{map[string]interface{}{"id": "1", "type": "function", "function": map[string]interface{}{"name": "policy_search", "arguments": `{"input":"refund"}`}}}}}}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant", "content": answer}}}})
		case r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/collections/"):
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `{"status":"not found"}`)
		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/points"):
			mu.Lock()
			for _, p := range asList(body["points"]) {
				points = append(points, p.(map[string]interface{}))
			}
			mu.Unlock()
			_, _ = io.WriteString(w, `{"result":{"status":"completed"}}`)
		case r.Method == "PUT":
			_, _ = io.WriteString(w, `{"result":true}`)
		case strings.HasSuffix(r.URL.Path, "/points/search"):
			mu.Lock()
			var res []interface{}
			for _, p := range points {
				res = append(res, map[string]interface{}{"score": cosine(floats(body["vector"]), floats(p["vector"])), "payload": p["payload"]})
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"result": res})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, seen
}

func TestRAGInMemoryAndQdrant(t *testing.T) {
	srv, seen := ragServer(t)
	creds := NewStaticCredentials()
	creds.Set("openAiApi", "*", map[string]interface{}{"apiKey": "k", "url": srv.URL})
	creds.Set("qdrantApi", "*", map[string]interface{}{"qdrantUrl": srv.URL})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	emb := func(name string) map[string]interface{} {
		return withCred(lc(name, "embeddingsOpenAi", 1.2, nil), "openAiApi")
	}
	pdf := minimalPDF("Refunds are processed within 5 days")
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Docs", "code", 2, map[string]interface{}{"jsCode": "return [{json:{text:'The cat sat on the mat. Cats love naps.'}}, {json:{text:'Pasta recipe: boil pasta for 9 minutes.'}}];"}),
		lc("Store", "vectorStoreInMemory", 1.3, map[string]interface{}{"mode": "insert", "memoryKey": map[string]interface{}{"__rl": true, "mode": "id", "value": "kb-test"}, "clearStore": true}),
		lc("Loader", "documentDefaultDataLoader", 1, map[string]interface{}{"jsonMode": "expressionData", "jsonData": "={{ $json.text }}"}),
		lc("Splitter", "textSplitterRecursiveCharacterTextSplitter", 1, map[string]interface{}{"chunkSize": 30, "chunkOverlap": 0}),
		emb("Emb1"),
		lc("Find", "vectorStoreInMemory", 1.3, map[string]interface{}{"mode": "load", "memoryKey": "kb-test", "prompt": "where is the cat", "topK": 1}),
		emb("Emb2"),
		// Qdrant insert from a PDF, then QA chain + agent tool.
		nd("PDF", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("b", "string", base64Std(pdf))}}}),
		nd("ToFile", "convertToFile", 1.1, map[string]interface{}{"operation": "toBinary", "sourceProperty": "b", "options": map[string]interface{}{"fileName": "policy.pdf", "mimeType": "application/pdf"}}),
		withCred(lc("Qdrant", "vectorStoreQdrant", 1.1, map[string]interface{}{"mode": "insert", "qdrantCollection": map[string]interface{}{"__rl": true, "mode": "id", "value": "policies"}}), "qdrantApi"),
		lc("PdfLoader", "documentDefaultDataLoader", 1.1, map[string]interface{}{"dataType": "binary"}),
		emb("Emb3"),
		lc("QA", "chainRetrievalQa", 1.3, map[string]interface{}{"promptType": "define", "text": "How long do refunds take?"}),
		withCred(lc("Model", "lmChatOpenAi", 1, nil), "openAiApi"),
		lc("Retriever", "retrieverVectorStore", 1, map[string]interface{}{"topK": 2}),
		withCred(lc("QdrantR", "vectorStoreQdrant", 1.1, map[string]interface{}{"qdrantCollection": map[string]interface{}{"__rl": true, "mode": "id", "value": "policies"}}), "qdrantApi"),
		emb("Emb4"),
		lc("Agent", "agent", 1.7, map[string]interface{}{"promptType": "define", "text": "refund policy?"}),
		withCred(lc("Model2", "lmChatOpenAi", 1, nil), "openAiApi"),
		withCred(lc("Policy tool", "vectorStoreQdrant", 1.1, map[string]interface{}{"mode": "retrieve-as-tool", "toolName": "policy_search", "toolDescription": "Search policies", "qdrantCollection": map[string]interface{}{"__rl": true, "mode": "id", "value": "policies"}}), "qdrantApi"),
		emb("Emb5"),
	},
		"Start->Docs", "Docs->Store", "Loader~ai_document->Store", "Splitter~ai_textSplitter->Loader", "Emb1~ai_embedding->Store",
		"Store->Find", "Emb2~ai_embedding->Find",
		"Start->PDF", "PDF->ToFile", "ToFile->Qdrant", "PdfLoader~ai_document->Qdrant", "Emb3~ai_embedding->Qdrant",
		"Qdrant->QA", "Model~ai_languageModel->QA", "Retriever~ai_retriever->QA", "QdrantR~ai_vectorStore->Retriever", "Emb4~ai_embedding->QdrantR",
		"QA->Agent", "Model2~ai_languageModel->Agent", "Policy tool~ai_tool->Agent", "Emb5~ai_embedding->Policy tool",
	)
	_, steps := h.run(t, data, "")

	inserted := itemsOf(t, steps["Store"], 0)
	if len(inserted) < 3 {
		t.Fatalf("splitter should create several chunks: %v", inserted)
	}
	found := itemsOf(t, steps["Find"], 0)
	doc, _ := found[0]["document"].(map[string]interface{})
	// load runs once per input item (n8n semantics): one top-1 hit per chunk.
	if len(found) != len(inserted) || !strings.Contains(strings.ToLower(toString(doc["pageContent"])), "cat") {
		t.Fatalf("similarity search: %v", found)
	}
	if qa := itemsOf(t, steps["QA"], 0)[0]; mustJSON(qa["response"]) != `{"text":"Refunds take 5 days."}` {
		t.Fatalf("QA chain: %v", qa)
	}
	if a := itemsOf(t, steps["Agent"], 0)[0]; a["output"] != "Tool says: refunds take 5 days." {
		t.Fatalf("agent with vector store tool: %v", a)
	}
	put, _ := seen.Load("PUT /collections/policies/points")
	if !strings.Contains(toString(put), `"content":"Refunds are processed within 5 days"`) {
		t.Fatalf("qdrant payload should use LangChain layout: %v", put)
	}
	if c, _ := seen.Load("PUT /collections/policies"); !strings.Contains(toString(c), `"size":6`) {
		t.Fatalf("collection should be created with the embedding size: %v", c)
	}
}
