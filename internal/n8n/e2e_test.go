package n8n

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/engine"
	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/node"
	"github.com/dgmos/flow/internal/storage/memory"
)

// wfJSON builds an n8n workflow export from nodes and "A->B" style links.
// Links: "Src:out->Dst:in" (out/in default 0).
func wfJSON(t *testing.T, nodes []map[string]interface{}, links ...string) []byte {
	t.Helper()
	conns := map[string]map[string][][]map[string]interface{}{}
	for _, l := range links {
		parts := strings.Split(l, "->")
		src, dst := parts[0], parts[1]
		if i := strings.Index(src, "~"); i > 0 {
			// AI connection: "Sub~ai_type->Root"
			ct := src[i+1:]
			src = src[:i]
			if conns[src] == nil {
				conns[src] = map[string][][]map[string]interface{}{}
			}
			conns[src][ct] = [][]map[string]interface{}{{{"node": dst, "type": ct, "index": 0}}}
			continue
		}
		out, in := 0, 0
		if i := strings.LastIndex(src, ":"); i > 0 {
			fmt.Sscanf(src[i+1:], "%d", &out)
			src = src[:i]
		}
		if i := strings.LastIndex(dst, ":"); i > 0 {
			fmt.Sscanf(dst[i+1:], "%d", &in)
			dst = dst[:i]
		}
		if conns[src] == nil {
			conns[src] = map[string][][]map[string]interface{}{"main": {}}
		}
		for len(conns[src]["main"]) <= out {
			conns[src]["main"] = append(conns[src]["main"], nil)
		}
		conns[src]["main"][out] = append(conns[src]["main"][out], map[string]interface{}{"node": dst, "type": "main", "index": in})
	}
	for i, n := range nodes {
		if _, ok := n["id"]; !ok {
			n["id"] = fmt.Sprintf("id-%d", i)
		}
		if _, ok := n["parameters"]; !ok {
			n["parameters"] = map[string]interface{}{}
		}
	}
	b, _ := json.Marshal(map[string]interface{}{"name": "test", "nodes": nodes, "connections": conns})
	return b
}

func nd(name, typ string, version float64, params map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"name": name, "type": "n8n-nodes-base." + typ, "typeVersion": version, "parameters": params}
}

type harness struct {
	eng   *engine.Engine
	store *memory.MemoryStorage
	// fixedID pins the workflow ID across runs (polling cursors are keyed
	// by workflow).
	fixedID string
}

func newHarness(t *testing.T, opts *Options) *harness {
	store := memory.NewMemoryStorage()
	reg := engine.NewRegistry()
	if opts == nil {
		opts = &Options{}
	}
	Register(reg, opts)
	return &harness{eng: engine.NewEngine(store, reg), store: store}
}

// run imports, registers and executes a workflow; it returns the final
// execution and its steps by node name.
func (h *harness) run(t *testing.T, data []byte, input string) (*models.Execution, map[string]*models.ExecutionStep) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wfID := h.fixedID
	if wfID == "" {
		wfID = fmt.Sprintf("wf-%d", time.Now().UnixNano())
	}
	wf, rep, err := Import(data, ImportOptions{ID: wfID})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	if err := h.eng.RegisterWorkflow(ctx, "ns", wf); err != nil {
		t.Fatal(err)
	}
	id, err := h.eng.StartWorkflow(ctx, "ns", wf.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	go h.eng.StartWorkerWithOptions(ctx, "ns", "w", engine.WorkerOptions{Concurrency: 4, PollInterval: 2 * time.Millisecond})
	deadline := time.Now().Add(10 * time.Second)
	var exec *models.Execution
	for time.Now().Before(deadline) {
		exec, _ = h.store.GetExecution(ctx, "ns", id)
		if exec != nil && exec.Status != models.TaskRunning {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	steps, _ := h.store.GetExecutionSteps(ctx, "ns", id)
	byName := map[string]*models.ExecutionStep{}
	names := map[string]string{}
	for _, n := range wf.Nodes {
		names[n.ID] = n.Name
	}
	for _, s := range steps {
		byName[names[s.NodeID]] = s
	}
	if exec.Status != models.TaskCompleted {
		for n, s := range byName {
			if s.Error != "" {
				t.Logf("%s: %s", n, s.Error)
			}
		}
		t.Fatalf("execution %s: %s", exec.Status, exec.ErrorMessage)
	}
	return exec, byName
}

func itemsOf(t *testing.T, s *models.ExecutionStep, port int) []map[string]interface{} {
	t.Helper()
	if s == nil {
		t.Fatal("step missing")
	}
	var out []map[string]interface{}
	for _, it := range decodeOutput(s.Output)[port] {
		out = append(out, it.JSON)
	}
	return out
}

func assign(name, typ string, value interface{}) map[string]interface{} {
	return map[string]interface{}{"id": name, "name": name, "type": typ, "value": value}
}

func TestE2EBranchMergeCode(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Numbers", "code", 2, map[string]interface{}{"jsCode": "return [1, 5, 10].map(n => ({json: {n}}));"}),
		nd("Big?", "if", 2.2, map[string]interface{}{"conditions": map[string]interface{}{
			"combinator": "and", "options": map[string]interface{}{"caseSensitive": true, "typeValidation": "strict"},
			"conditions": []interface{}{map[string]interface{}{"leftValue": "={{ $json.n }}", "rightValue": 4, "operator": map[string]interface{}{"type": "number", "operation": "gt"}}},
		}}),
		nd("Tag big", "set", 3.4, map[string]interface{}{"includeOtherFields": true, "assignments": map[string]interface{}{"assignments": []interface{}{assign("size", "string", "big")}}}),
		nd("Tag small", "set", 3.4, map[string]interface{}{"includeOtherFields": true, "assignments": map[string]interface{}{"assignments": []interface{}{assign("size", "string", "small")}}}),
		nd("Merge", "merge", 3, map[string]interface{}{}),
		nd("Label", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{
			assign("label", "string", "={{ $json.size }}-{{ $json.n }}"),
			assign("orig", "number", "={{ $('Numbers').item.json.n }}"),
			assign("meta.double", "number", "={{ $json.n * 2 }}"),
		}}}),
	}, "Start->Numbers", "Numbers->Big?", "Big?:0->Tag big", "Big?:1->Tag small", "Tag big->Merge:0", "Tag small->Merge:1", "Merge->Label")

	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["Label"], 0)
	if len(got) != 3 {
		t.Fatalf("want 3 items, got %v", got)
	}
	want := map[string]float64{"big-5": 5, "big-10": 10, "small-1": 1}
	for _, it := range got {
		label := toString(it["label"])
		if want[label] != it["orig"] {
			t.Errorf("label %s orig %v (paired item tracing through IF/Merge)", label, it["orig"])
		}
		meta, _ := it["meta"].(map[string]interface{})
		if meta["double"] != it["orig"].(float64)*2 {
			t.Errorf("dot notation: %v", it)
		}
		if _, leaked := it["n"]; leaked {
			t.Errorf("includeOtherFields=false should drop n: %v", it)
		}
	}
}

func TestE2ETriggerSelection(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Manual", "manualTrigger", 1, nil),
		nd("Schedule", "scheduleTrigger", 1.2, map[string]interface{}{"rule": map[string]interface{}{"interval": []interface{}{map[string]interface{}{"triggerAtHour": 1, "triggerAtMinute": 33}}}}),
		nd("Orphan", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("x", "number", 1)}}}),
		nd("Out", "set", 3.4, map[string]interface{}{"includeOtherFields": true, "assignments": map[string]interface{}{"assignments": []interface{}{assign("via", "string", "={{ $prevNode.name }}")}}}),
	}, "Manual->Out", "Schedule->Out")

	_, steps := h.run(t, data, `{"hello":"world"}`)
	got := itemsOf(t, steps["Out"], 0)
	if len(got) != 1 || got[0]["via"] != "Manual" || got[0]["hello"] != "world" {
		t.Fatalf("default trigger: %v", got)
	}
	if steps["Orphan"].Status != models.TaskCompleted || len(decodeOutput(steps["Orphan"].Output)[0]) != 0 {
		t.Fatalf("orphan node must not produce data: %+v", steps["Orphan"])
	}

	_, steps = h.run(t, data, `{"__trigger":"Schedule"}`)
	got = itemsOf(t, steps["Out"], 0)
	if len(got) != 1 || got[0]["via"] != "Schedule" || got[0]["Timezone"] == nil {
		t.Fatalf("schedule trigger: %v", got)
	}

	_, rep, _ := Import(data, ImportOptions{})
	var cron []string
	for _, tr := range rep.Triggers {
		if tr.Kind == "schedule" {
			cron = tr.Cron
		}
	}
	if len(cron) != 1 || cron[0] != "0 33 1 * * *" {
		t.Fatalf("cron = %v", cron)
	}
}

func TestE2ELoopOverItems(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Make", "code", 2, map[string]interface{}{"jsCode": "return [1,2,3,4,5].map(i => ({json: {i}}));"}),
		nd("Loop", "splitInBatches", 3, map[string]interface{}{"batchSize": 2}),
		nd("Work", "code", 2, map[string]interface{}{"jsCode": "return $input.all().map(it => ({json: {i: it.json.i, sq: it.json.i * it.json.i, batch: $input.all().length}}));"}),
		nd("Collect", "aggregate", 1, map[string]interface{}{"aggregate": "aggregateAllItemData"}),
		nd("After", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{
			assign("total", "number", "={{ $json.data.map(d => d.sq).sum() }}"),
			assign("lastWork", "number", "={{ $('Work').all().length }}"),
		}}}),
	}, "Start->Make", "Make->Loop", "Loop:1->Work", "Work->Loop", "Loop:0->Collect", "Collect->After")

	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["After"], 0)
	if len(got) != 1 || got[0]["total"] != 55.0 {
		t.Fatalf("loop result: %v", got)
	}
	if got[0]["lastWork"] != 1.0 {
		t.Fatalf("$('Work') should see the last batch (1 item): %v", got)
	}
}

func TestE2EDataNodes(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Seed", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"orders":[{"id":1,"c":"a","v":30},{"id":2,"c":"b","v":10},{"id":3,"c":"a","v":20},{"id":3,"c":"a","v":20}]}`}),
		nd("Split", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "orders"}),
		nd("Dedupe", "removeDuplicates", 2, map[string]interface{}{"compare": "selectedFields", "fieldsToCompare": "id"}),
		nd("Sort", "sort", 1, map[string]interface{}{"sortFieldsUi": map[string]interface{}{"sortField": []interface{}{map[string]interface{}{"fieldName": "v", "order": "descending"}}}}),
		nd("Route", "switch", 3.2, map[string]interface{}{
			"rules":   map[string]interface{}{"values": []interface{}{rule("c", "a")}},
			"options": map[string]interface{}{"fallbackOutput": "extra"},
		}),
		nd("Top", "limit", 1, map[string]interface{}{"maxItems": 1}),
		nd("Customers", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"c":"a","name":"Alice"}`}),
		nd("Join", "merge", 3, map[string]interface{}{"mode": "combine", "fieldsToMatchString": "c"}),
		nd("Others", "aggregate", 1, map[string]interface{}{"fieldsToAggregate": map[string]interface{}{"fieldToAggregate": []interface{}{map[string]interface{}{"fieldToAggregate": "id"}}}}),
	}, "Start->Seed", "Seed->Split", "Split->Dedupe", "Dedupe->Sort", "Sort->Route", "Route:0->Top", "Route:1->Others",
		"Start->Customers", "Top->Join:0", "Customers->Join:1")

	_, steps := h.run(t, data, "")
	if got := itemsOf(t, steps["Join"], 0); len(got) != 1 || got[0]["id"] != 1.0 || got[0]["name"] != "Alice" {
		t.Fatalf("join: %v", got)
	}
	if got := itemsOf(t, steps["Others"], 0); len(got) != 1 || fmt.Sprint(got[0]["id"]) != "[2]" {
		t.Fatalf("fallback route: %v", got)
	}
}

func rule(field, value string) map[string]interface{} {
	return map[string]interface{}{"conditions": map[string]interface{}{
		"combinator": "and", "options": map[string]interface{}{"caseSensitive": true, "typeValidation": "strict"},
		"conditions": []interface{}{map[string]interface{}{"leftValue": "={{ $json." + field + " }}", "rightValue": value, "operator": map[string]interface{}{"type": "string", "operation": "equals"}}},
	}}
}

func TestE2EHTTPRequest(t *testing.T) {
	var gotAuth, gotQuery, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotQuery = r.Header.Get("X-Key"), r.URL.RawQuery
		b := make([]byte, 1024)
		n, _ := r.Body.Read(b)
		gotBody = string(b[:n])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":1},{"id":2}]`))
	}))
	defer srv.Close()

	creds := NewStaticCredentials()
	creds.Set("httpHeaderAuth", "*", map[string]interface{}{"name": "X-Key", "value": "secret"})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	httpNode := nd("Call", "httpRequest", 4.2, map[string]interface{}{
		"method": "POST", "url": "=" + srv.URL + "/items",
		"authentication": "genericCredentialType", "genericAuthType": "httpHeaderAuth",
		"sendQuery": true, "queryParameters": map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "q", "value": "={{ $json.term }}"}}},
		"sendBody": true, "specifyBody": "json", "jsonBody": `={ "term": "{{ $json.term }}" }`,
	})
	httpNode["credentials"] = map[string]interface{}{"httpHeaderAuth": map[string]interface{}{"id": "1", "name": "My key"}}
	data := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), httpNode}, "Start->Call")

	_, steps := h.run(t, data, `{"term":"go lang"}`)
	got := itemsOf(t, steps["Call"], 0)
	if len(got) != 2 || got[1]["id"] != 2.0 {
		t.Fatalf("array response should become items: %v", got)
	}
	if gotAuth != "secret" || gotQuery != "q=go+lang" || !strings.Contains(gotBody, `"term":"go lang"`) {
		t.Fatalf("request: auth=%q query=%q body=%q", gotAuth, gotQuery, gotBody)
	}

	// Without an allowed host the SSRF guard blocks loopback.
	h2 := newHarness(t, &Options{Credentials: creds})
	wf, _, _ := Import(data, ImportOptions{ID: "blocked"})
	_ = h2.eng.RegisterWorkflow(context.Background(), "ns", wf)
	id, _ := h2.eng.StartWorkflow(context.Background(), "ns", "blocked", `{"term":"x"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go h2.eng.StartWorker(ctx, "ns", "w")
	for ctx.Err() == nil {
		if e, _ := h2.store.GetExecution(ctx, "ns", id); e.Status != models.TaskRunning {
			if e.Status != models.TaskFailed || !strings.Contains(e.ErrorMessage, "blocked") {
				t.Fatalf("want SSRF block, got %s %s", e.Status, e.ErrorMessage)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout")
}

func TestE2EExecuteWorkflowInline(t *testing.T) {
	h := newHarness(t, nil)
	child := wfJSON(t, []map[string]interface{}{
		nd("In", "executeWorkflowTrigger", 1.1, map[string]interface{}{"inputSource": "passthrough"}),
		nd("Double", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("v", "number", "={{ $json.v * 2 }}")}}}),
	}, "In->Double")
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Vals", "code", 2, map[string]interface{}{"jsCode": "return [{json:{v:1}},{json:{v:2}}];"}),
		nd("Sub", "executeWorkflow", 1.2, map[string]interface{}{"source": "parameter", "workflowJson": string(child), "mode": "each"}),
	}, "Start->Vals", "Vals->Sub")
	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["Sub"], 0)
	if len(got) != 2 || got[0]["v"] != 2.0 || got[1]["v"] != 4.0 {
		t.Fatalf("sub-workflow: %v", got)
	}
}

func TestImportReport(t *testing.T) {
	n := nd("Sheets", "acmeCrm", 4, nil)
	n["credentials"] = map[string]interface{}{"acmeCrmOAuth2Api": map[string]interface{}{"id": "x", "name": "Google"}}
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil), n,
		nd("Bad", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("x", "string", "={{ $json.a + }}")}}}),
	}, "Start->Sheets", "Sheets->Bad")
	_, rep, err := Import(data, ImportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The unknown node blocks; the broken expression is a warning (it fails
	// only when its node runs, as in n8n).
	if rep.Runnable || len(rep.Blocking) != 1 || rep.Supported != 2 || len(rep.Warnings) != 1 || !strings.Contains(rep.Warnings[0].Message, "$json.a +") {
		t.Fatalf("report: %+v", rep)
	}
	if len(rep.Credentials) != 1 || rep.Credentials[0].Type != "acmeCrmOAuth2Api" {
		t.Fatalf("credentials: %+v", rep.Credentials)
	}
}

// Polling loop: Check -> Done? -(no)-> Wait -> Check ... -(yes)-> Out
func TestE2EPollingCycle(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Init", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("tries", "number", 0)}}}),
		nd("Check", "code", 2, map[string]interface{}{"jsCode": "return $input.all().map(i => ({json: {tries: i.json.tries + 1}}));"}),
		nd("Done?", "if", 2.2, map[string]interface{}{"conditions": map[string]interface{}{"combinator": "and", "options": map[string]interface{}{},
			"conditions": []interface{}{map[string]interface{}{"leftValue": "={{ $json.tries }}", "rightValue": 3, "operator": map[string]interface{}{"type": "number", "operation": "gte"}}}}}),
		nd("Wait", "wait", 1.1, map[string]interface{}{"amount": 0.001, "unit": "seconds"}),
		nd("Out", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{
			assign("tries", "number", "={{ $json.tries }}"), assign("checks", "number", "={{ $('Check').all().length }}")}}}),
	}, "Start->Init", "Init->Check", "Check->Done?", "Done?:1->Wait", "Wait->Check", "Done?:0->Out")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("polling loop should be runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["Out"], 0)
	if len(got) != 1 || got[0]["tries"] != 3.0 || got[0]["checks"] != 1.0 {
		t.Fatalf("polling loop: %v", got)
	}
}
