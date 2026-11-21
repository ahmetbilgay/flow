package n8ntemplates

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func node(name, typ string, params map[string]interface{}, creds map[string]interface{}) map[string]interface{} {
	n := map[string]interface{}{"id": name, "name": name, "type": typ, "typeVersion": 1, "position": []interface{}{0, 0}, "parameters": params}
	if creds != nil {
		n["credentials"] = creds
	}
	return n
}

func link(to string) map[string]interface{} {
	return map[string]interface{}{"main": []interface{}{[]interface{}{map[string]interface{}{"node": to, "type": "main", "index": 0}}}}
}

// fakeGallery serves /search and /workflows/{id} like api.n8n.io.
func fakeGallery(t *testing.T) *httptest.Server {
	t.Helper()
	runnable := map[string]interface{}{
		"nodes": []interface{}{
			node("Start", "n8n-nodes-base.manualTrigger", nil, nil),
			node("Rows", "n8n-nodes-base.googleSheets", map[string]interface{}{
				"documentId": map[string]interface{}{"__rl": true, "mode": "url", "value": "https://docs.google.com/spreadsheets/d/AUTHOR"},
				"sheetName":  map[string]interface{}{"__rl": true, "mode": "list", "value": "={{ $json.sheet }}"},
			}, map[string]interface{}{"googleSheetsOAuth2Api": map[string]interface{}{"id": "1", "name": "Sheets"}}),
			node("Notify", "n8n-nodes-base.slack", map[string]interface{}{"channelId": "C0AUTHOR", "text": "={{ $json.name }} joined"}, nil),
			node("Prompt", "n8n-nodes-base.set", map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{
				map[string]interface{}{"name": "key", "value": "YOUR_API_KEY"},
				map[string]interface{}{"name": "voice", "value": "={{voice id}}"},
				map[string]interface{}{"name": "fine", "value": "={{ $json.id }}"},
			}}}, nil),
			node("Script", "n8n-nodes-base.code", map[string]interface{}{"jsCode": "// TODO: replace with your logic\nreturn items;"}, nil),
			node("Note", "n8n-nodes-base.stickyNote", map[string]interface{}{"content": "YOUR_NOTES"}, nil),
		},
		"connections": map[string]interface{}{"Start": link("Rows"), "Rows": link("Notify"), "Notify": link("Prompt"), "Prompt": link("Script")},
		"pinData":     map[string]interface{}{"Rows": []interface{}{map[string]interface{}{"json": map[string]interface{}{"secret": "author data"}}}},
	}
	blocked := map[string]interface{}{
		"nodes":       []interface{}{node("Start", "n8n-nodes-base.manualTrigger", nil, nil), node("X", "n8n-nodes-acme.unknown", nil, nil)},
		"connections": map[string]interface{}{"Start": link("X")},
	}
	meta := func(id, name string, views int, price float64, wf map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"workflow": map[string]interface{}{
			"id": id, "name": name, "description": "**What it does**", "totalViews": views, "price": price, "createdAt": "2025-01-02T00:00:00Z",
			"user":       map[string]interface{}{"name": "Ada", "username": "ada"},
			"categories": []interface{}{map[string]interface{}{"id": 1, "name": "Sales"}},
			"workflow":   wf,
		}}
	}
	docs := map[string]interface{}{
		"10": meta("10", "Sheet to Slack", 900, 0, runnable),
		"20": meta("20", "Uses an unknown node", 5000, 0, blocked),
		"30": meta("30", "Paid template", 9000, 19, runnable),
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/search":
			if r.URL.Query().Get("page") != "1" {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"totalWorkflows": 3, "workflows": []interface{}{}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"totalWorkflows": 3, "workflows": []interface{}{
				map[string]interface{}{"id": 10, "totalViews": 900}, map[string]interface{}{"id": 20, "totalViews": 5000}, map[string]interface{}{"id": 30, "totalViews": 9000},
			}})
		case strings.HasPrefix(r.URL.Path, "/workflows/"):
			d, ok := docs[strings.TrimPrefix(r.URL.Path, "/workflows/")]
			if !ok {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(d)
		default:
			w.WriteHeader(404)
		}
	}))
}

func TestExportCatalog(t *testing.T) {
	srv := fakeGallery(t)
	defer srv.Close()
	c := &Client{API: srv.URL, HTTP: srv.Client(), UserAgent: "test"}
	ctx := context.Background()

	ids, err := c.Search(ctx, 0, "")
	if err != nil || strings.Join(ids, ",") != "30,20,10" {
		t.Fatalf("search (most viewed first): %v %v", ids, err)
	}
	if ids, _ := c.Search(ctx, 1000, ""); strings.Join(ids, ",") != "30,20" {
		t.Fatalf("min views: %v", ids)
	}

	out, st := Export(ctx, c, append(ids, "404"), 2, false, 0)
	if len(out) != 1 || st.Runnable != 1 || st.Blocked != 1 || st.Paid != 1 || st.Failed != 1 {
		t.Fatalf("export: %d templates, stats %+v", len(out), st)
	}
	tp := out[0]
	if tp.Source != "n8n" || tp.SourceID != "10" || tp.SourceURL != "https://n8n.io/workflows/10" || tp.Name != "Sheet to Slack" ||
		!tp.Runnable || tp.Nodes != 5 || tp.TotalViews != 900 || tp.Author.Username != "ada" || strings.Join(tp.Categories, ",") != "Sales" {
		t.Fatalf("template meta: %+v", tp)
	}
	if strings.Join(tp.CredentialTypes, ",") != "googleSheetsOAuth2Api" || len(tp.NeedsAccount) != 1 || tp.NeedsAccount[0].Node != "Notify" {
		t.Fatalf("accounts: creds %v needs %v", tp.CredentialTypes, tp.NeedsAccount)
	}
	if tp.Workflow["name"] != "Sheet to Slack" || tp.Workflow["pinData"] != nil || len(tp.Triggers) != 1 || tp.Triggers[0].Kind != "manual" {
		t.Fatalf("workflow: name=%v pinData=%v triggers=%v", tp.Workflow["name"], tp.Workflow["pinData"], tp.Triggers)
	}
	var got []string
	for _, f := range tp.SetupFields {
		got = append(got, f.Reason+":"+f.Node+":"+f.Parameter)
	}
	want := "account-specific:Notify:channelId placeholder:Prompt:assignments.assignments[0].value placeholder:Prompt:assignments.assignments[1].value resource:Rows:documentId"
	if strings.Join(got, " ") != want {
		t.Fatalf("setup fields:\n got %s\nwant %s", strings.Join(got, " "), want)
	}

	// Blocked templates on request, the limit, and JSON output shape.
	out, _ = Export(ctx, c, ids, 2, true, 0)
	if len(out) != 2 || out[0].Runnable || len(out[0].Blocking) == 0 || out[1].SourceID != "10" {
		t.Fatalf("include blocked: %+v", out)
	}
	if out, _ = Export(ctx, c, ids, 2, true, 1); len(out) != 1 {
		t.Fatalf("limit: %d", len(out))
	}
	b, _ := json.Marshal(out[0])
	for _, k := range []string{`"source":"n8n"`, `"setup_fields":[]`, `"needs_account":[{"node":"X"`, `"credential_types":[]`, `"runnable":false`} {
		if !strings.Contains(string(b), k) {
			t.Errorf("JSON lacks %s: %s", k, b)
		}
	}
}

func TestBuildRejectsBadResponses(t *testing.T) {
	if _, _, err := Build("1", map[string]interface{}{}, time.Now()); err == nil {
		t.Fatal("missing workflow accepted")
	}
	if _, paid, _ := Build("1", map[string]interface{}{"workflow": map[string]interface{}{"price": "5"}}, time.Now()); !paid {
		t.Fatal("paid template not detected")
	}
}

func TestListPage(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"totalWorkflows": 2, "workflows": []interface{}{
			map[string]interface{}{"id": 7, "name": "Free", "description": "d", "totalViews": 12, "price": 0, "user": map[string]interface{}{"name": "Ada", "username": "ada"},
				"nodes": []interface{}{map[string]interface{}{"name": "n8n-nodes-base.slack", "displayName": "Slack"}, map[string]interface{}{"name": "n8n-nodes-base.stickyNote", "displayName": "Sticky Note"}, map[string]interface{}{"name": "n8n-nodes-base.slack", "displayName": "Slack"}}},
			map[string]interface{}{"id": 8, "name": "Paid", "price": 25},
		}, "filters": []interface{}{
			map[string]interface{}{"field_name": "categories", "counts": []interface{}{map[string]interface{}{"value": "AI", "count": 9}}},
			map[string]interface{}{"field_name": "apps", "counts": []interface{}{map[string]interface{}{"value": "Slack", "count": 3}}},
		}})
	}))
	defer srv.Close()
	c := &Client{API: srv.URL, HTTP: srv.Client()}
	p, err := c.List(context.Background(), ListOptions{Page: 2, Rows: 10, Search: "slack bot", Category: "AI"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(gotQuery, "page=2") || !strings.Contains(gotQuery, "search=slack+bot") || !strings.Contains(gotQuery, "category=AI") {
		t.Fatalf("query: %s", gotQuery)
	}
	if len(p.Templates) != 1 || p.Templates[0].ID != "7" || strings.Join(p.Templates[0].Services, ",") != "Slack" || p.Templates[0].Author.Username != "ada" {
		t.Fatalf("templates: %+v", p.Templates)
	}
	if len(p.Categories) != 1 || p.Categories[0].Name != "AI" || p.Categories[0].Count != 9 {
		t.Fatalf("categories: %+v", p.Categories)
	}
}

func TestLoadDir(t *testing.T) {
	dir := t.TempDir()
	wf := func(name string, extra ...map[string]interface{}) []byte {
		nodes := []interface{}{node("Start", "n8n-nodes-base.manualTrigger", nil, nil), node("Chat", "n8n-nodes-base.telegram", map[string]interface{}{"chatId": "123"}, nil)}
		for _, e := range extra {
			nodes = append(nodes, e)
		}
		b, _ := json.Marshal(map[string]interface{}{"name": name, "nodes": nodes, "connections": map[string]interface{}{"Start": link("Chat")}})
		return b
	}
	write := func(rel string, b []byte) {
		p := dir + "/" + rel
		_ = os.MkdirAll(p[:strings.LastIndex(p, "/")], 0o755)
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("Telegram_Bots/hello.json", wf("Hello bot"))
	write("Telegram_Bots/z-copy.txt", wf("Hello bot copy"))                                                          // same nodes: kept once
	write("Other/bom.txt", append([]byte("\xef\xbb\xbf"), wf("", node("X", "n8n-nodes-acme.unknown", nil, nil))...)) // BOM, no name
	write("Other/notes.txt", []byte("just notes"))
	write(".github/ci.json", wf("hidden", node("Y", "n8n-nodes-base.noOp", nil, nil)))
	got, err := LoadDir(dir, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("templates: %d %+v", len(got), got)
	}
	bom, hello := got[0], got[1]
	if hello.Name != "Hello bot" || hello.Source != "local" || hello.SourceURL != "Telegram_Bots/hello.json" || strings.Join(hello.Categories, ",") != "Telegram Bots" ||
		!hello.Runnable || len(hello.SetupFields) != 1 || hello.SetupFields[0].Reason != "account-specific" {
		t.Fatalf("hello: %+v", hello)
	}
	if bom.Name != "bom" || bom.Runnable || len(bom.Blocking) == 0 {
		t.Fatalf("bom: %+v", bom)
	}
}
