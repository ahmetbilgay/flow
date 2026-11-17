package n8n

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/node"
)

func TestLogicNodes(t *testing.T) {
	h := newHarness(t, nil)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Seed", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"a":1,"b":"x"}`}),
		nd("ToBin", "moveBinaryData", 1, map[string]interface{}{"mode": "jsonToBinary", "options": map[string]interface{}{"useRawData": false}}),
		nd("ToJSON", "moveBinaryData", 1, map[string]interface{}{}),
		nd("People", "n8nTrainingCustomerDatastore", 1, map[string]interface{}{"operation": "getAllPeople", "limit": 3}),
		nd("Random", "debugHelper", 1, map[string]interface{}{"category": "randomData", "randomDataCount": 4, "randomDataSeed": "s"}),
		nd("Transform", "aiTransform", 1, map[string]interface{}{"jsCode": "return [{json: {n: $input.all().length}}];"}),
		nd("A", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"rows":[{"id":1,"v":"same"},{"id":2,"v":"old"},{"id":3,"v":"onlyA"}]}`}),
		nd("ASplit", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "rows"}),
		nd("B", "set", 3.4, map[string]interface{}{"mode": "raw", "jsonOutput": `{"rows":[{"id":"1","v":"same"},{"id":2,"v":"new"},{"id":4,"v":"onlyB"}]}`}),
		nd("BSplit", "splitOut", 1, map[string]interface{}{"fieldToSplitOut": "rows"}),
		nd("Compare", "compareDatasets", 2.3, map[string]interface{}{"fuzzyCompare": true, "mergeByFields": map[string]interface{}{"values": []interface{}{map[string]interface{}{"field1": "id", "field2": "id"}}}}),
	}, "Start->Seed", "Seed->ToBin", "ToBin->ToJSON", "Start->People", "Start->Random", "People->Transform",
		"Start->A", "A->ASplit", "ASplit->Compare:0", "Start->B", "B->BSplit", "BSplit->Compare:1")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	if got := itemsOf(t, steps["ToJSON"], 0); len(got) != 1 || got[0]["a"] != 1.0 || got[0]["b"] != "x" {
		t.Fatalf("move binary round trip: %v", got)
	}
	if got := itemsOf(t, steps["People"], 0); len(got) != 3 || got[0]["name"] != "Jay Gatsby" {
		t.Fatalf("training datastore: %v", got)
	}
	if got := itemsOf(t, steps["Random"], 0); len(got) != 4 || !strings.Contains(toString(got[0]["email"]), "@example.com") {
		t.Fatalf("random data: %v", got)
	}
	if got := itemsOf(t, steps["Transform"], 0); len(got) != 1 || got[0]["n"] != 3.0 {
		t.Fatalf("ai transform: %v", got)
	}
	aOnly, same, diff, bOnly := itemsOf(t, steps["Compare"], 0), itemsOf(t, steps["Compare"], 1), itemsOf(t, steps["Compare"], 2), itemsOf(t, steps["Compare"], 3)
	if len(aOnly) != 1 || aOnly[0]["id"] != 3.0 || len(same) != 1 || len(bOnly) != 1 || bOnly[0]["id"] != 4.0 || len(diff) != 1 {
		t.Fatalf("compare: aOnly=%v same=%v diff=%v bOnly=%v", aOnly, same, diff, bOnly)
	}
	if d := asMap(asMap(diff[0]["different"])["v"]); d["inputA"] != "old" || d["inputB"] != "new" {
		t.Fatalf("different: %v", diff[0])
	}
}

func TestMemoryManagerAndRespondToChat(t *testing.T) {
	store := &memStore{data: map[string][]ChatMessage{}}
	h := newHarness(t, &Options{ChatMemory: store})
	mem := func(name string) map[string]interface{} {
		return lc(name, "memoryBufferWindow", 1.3, map[string]interface{}{"sessionIdType": "customKey", "sessionKey": "s1"})
	}
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		lc("Insert", "memoryManager", 1.1, map[string]interface{}{"mode": "insert", "messages": map[string]interface{}{"messageValues": []interface{}{
			map[string]interface{}{"type": "user", "message": "hi"}, map[string]interface{}{"type": "ai", "message": "hello"}, map[string]interface{}{"type": "user", "message": "bye"}}}}),
		mem("M1"),
		lc("Delete", "memoryManager", 1.1, map[string]interface{}{"mode": "delete", "deleteMode": "lastN", "lastMessagesCount": 1}),
		mem("M2"),
		lc("Load", "memoryManager", 1.1, map[string]interface{}{}),
		mem("M3"),
		lc("Say", "chat", 1, map[string]interface{}{"message": "={{ $json.messages[0].ai }}", "waitUserReply": false}),
	}, "Start->Insert", "M1~ai_memory->Insert", "Insert->Delete", "M2~ai_memory->Delete", "Delete->Load", "M3~ai_memory->Load", "Load->Say")
	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["Load"], 0)
	msgs := asList(got[0]["messages"])
	if len(msgs) != 1 || asMap(msgs[0])["human"] != "hi" || asMap(msgs[0])["ai"] != "hello" {
		t.Fatalf("memory after insert+delete: %v", got)
	}
	var list []*models.ExecutionStep
	for _, s := range steps {
		list = append(list, s)
	}
	if cm := chatMessages(list); len(cm) != 1 || cm[0] != "hello" {
		t.Fatalf("chat messages: %v", cm)
	}
}

func TestCodeHelpersAndModules(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"q":"` + r.URL.Query().Get("q") + `","m":"` + r.Method + `"}`))
	}))
	defer srv.Close()
	h := newHarness(t, &Options{Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	code := `
const crypto = require('crypto');
const util = require('node:util');
const r = await this.helpers.httpRequest({ url: '` + srv.URL + `', qs: { q: 'go' }, json: true });
const full = await this.helpers.httpRequest({ method: 'POST', url: '` + srv.URL + `', body: {a: 1}, returnFullResponse: true });
const bin = await this.helpers.prepareBinaryData(Buffer.from('hello'), 'a.txt');
return [{ json: {
  q: r.q, status: full.statusCode, method: full.body.m,
  sha: crypto.createHash('sha256').update('abc').digest('hex'),
  hmac: crypto.createHmac('sha256', 'key').update('abc').digest('base64'),
  uuid: crypto.randomUUID().length,
  fmt: util.format('%s=%d %j', 'n', 5, {x: 1}),
}, binary: { data: bin } }];`
	data := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Code", "code", 2, map[string]interface{}{"jsCode": code})}, "Start->Code")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	out := decodeOutput(steps["Code"].Output)[0]
	j := out[0].JSON
	if j["q"] != "go" || j["status"] != 200.0 || j["method"] != "POST" || j["uuid"] != 36.0 || j["fmt"] != `n=5 {"x":1}` ||
		j["sha"] != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" || j["hmac"] != "nBluMtwBdfhvSxy4konWYZ3mvuaZ5MN45oMJ7Zehpqs=" {
		t.Fatalf("code helpers: %v", j)
	}
	if b, _, name, err := binaryOf(out[0], "data"); err != nil || string(b) != "hello" || name != "a.txt" {
		t.Fatalf("prepareBinaryData: %q %q %v", b, name, err)
	}

	bad := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Code", "code", 2, map[string]interface{}{"jsCode": "const fs = require('fs'); await this.helpers.httpRequestWithAuthentication({});"})}, "Start->Code")
	_, rep, _ = Import(bad, ImportOptions{})
	if rep.Runnable || len(rep.Blocking) != 2 {
		t.Fatalf("fs and httpRequestWithAuthentication must be reported: %+v", rep.Blocking)
	}
}
