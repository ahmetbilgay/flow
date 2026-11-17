package n8n

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

func TestCommunityNodes(t *testing.T) {
	var srvURL string
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch r.path {
		case "/search":
			var b map[string]interface{}
			_ = json.Unmarshal([]byte(r.body), &b)
			return 200, "application/json", map[string]interface{}{"answer": "found " + toString(b["query"]), "results": []interface{}{}}
		case "/scrape":
			return 200, "application/json", map[string]interface{}{"info": map[string]interface{}{"statusCode": 200}, "body": "<html>hn</html>"}
		case "/api/v1/graphAndAdvice":
			return 200, "application/json", map[string]interface{}{"aiAdvice": []interface{}{map[string]interface{}{"text": "agency matters"}}}
		case "/v2/posts":
			return 201, "application/json", map[string]interface{}{"postSubmissionId": "p1"}
		case "/youtubei/v1/player":
			return 200, "application/json", map[string]interface{}{"captions": map[string]interface{}{"playerCaptionsTracklistRenderer": map[string]interface{}{"captionTracks": []interface{}{
				map[string]interface{}{"languageCode": "en", "baseUrl": srvURL + "/api/timedtext?v=abc&fmt=srv3"}}}}}
		case "/api/timedtext":
			return 200, "text/xml", `<?xml version="1.0"?><transcript><text start="0.5" dur="1.2">Hello &amp;amp; welcome</text><text start="1.7" dur="2">to flow</text></transcript>`
		}
		return 404, "application/json", map[string]interface{}{}
	})
	srvURL = f.srv.URL
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		msgs := asList(body["messages"])
		if strings.Contains(mustJSON(msgs), `"role":"tool"`) {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant", "content": toString(asMap(msgs[len(msgs)-1])["content"])}}}})
			return
		}
		fn := asMap(asMap(asList(body["tools"])[0])["function"])
		if !strings.Contains(mustJSON(fn["parameters"]), "searchTerm") {
			t.Errorf("tavily tool should expose its {searchTerm} placeholder: %v", fn)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant",
			"tool_calls": []interface{}{map[string]interface{}{"id": "1", "type": "function", "function": map[string]interface{}{"name": toString(fn["name"]), "arguments": `{"searchTerm":"go \"generics\""}`}}}}}}})
	}))
	defer llm.Close()
	creds := NewStaticCredentials()
	creds.Set("tavilyApi", "*", map[string]interface{}{"apiKey": "tv"})
	creds.Set("scrapeNinjaApi", "*", map[string]interface{}{"apiKey": "sn"})
	creds.Set("infranodusApi", "*", map[string]interface{}{"apiKey": "in"})
	creds.Set("blotatoApi", "*", map[string]interface{}{"blotatoApiKey": "bl"})
	creds.Set("openAiApi", "*", map[string]interface{}{"apiKey": "k", "url": llm.URL})
	u := f.srv.URL
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}},
		APIBaseURLs: map[string]string{"tavily": u, "scrapeninja": u, "infranodus": u, "blotato": u, "youtubeweb": u}})
	tavilyQuery := "={\n  \"query\": \"{searchTerm}\",\n  \"search_depth\": \"advanced\",\n  \"max_results\": 1,\n  \"time_range\": null\n}"
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		lc("Agent", "agent", 1.7, map[string]interface{}{"promptType": "define", "text": "search generics"}),
		withCred(lc("Model", "lmChatOpenAi", 1, nil), "openAiApi"),
		credNode(map[string]interface{}{"name": "Tavily", "type": "@tavily/n8n-nodes-tavily.tavilyTool", "typeVersion": 1, "parameters": map[string]interface{}{"query": tavilyQuery, "options": map[string]interface{}{}, "toolDescription": "Search"}}, "tavilyApi"),
		credNode(map[string]interface{}{"name": "Scrape", "type": "CUSTOM.scrapeNinja", "typeVersion": 1, "parameters": map[string]interface{}{"url": "https://news.ycombinator.com/"}}, "scrapeNinjaApi"),
		credNode(map[string]interface{}{"name": "Infra", "type": "n8n-nodes-infranodus.infranodus", "typeVersion": 1, "parameters": map[string]interface{}{"name": "book", "prompt": "agency?"}}, "infranodusApi"),
		credNode(map[string]interface{}{"name": "Post", "type": "@blotato/n8n-nodes-blotato.blotato", "typeVersion": 2, "parameters": map[string]interface{}{"platform": "youtube",
			"accountId": map[string]interface{}{"__rl": true, "mode": "list", "value": "8047"}, "postContentText": "hi", "postContentMediaUrls": "https://x/v.mp4",
			"postCreateYoutubeOptionTitle": "T", "postCreateYoutubeOptionPrivacyStatus": "private"}}, "blotatoApi"),
		{"name": "Transcript", "type": "n8n-nodes-youtube-transcription.youtubeTranscripter", "typeVersion": 1, "parameters": map[string]interface{}{"youtubeId": "https://youtu.be/abcdefghijk"}},
		nd("Code", "code", 2, map[string]interface{}{"jsCode": "const { YoutubeTranscript } = require('youtube-transcript'); const t = await YoutubeTranscript.fetchTranscript('abcdefghijk'); return [{json:{n: t.length, first: t[0].text}}];"}),
	}, "Start->Agent", "Model~ai_languageModel->Agent", "Tavily~ai_tool->Agent", "Start->Scrape", "Start->Infra", "Start->Post", "Start->Transcript", "Start->Code")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable {
		t.Fatalf("not runnable: %+v", rep.Blocking)
	}
	_, steps := h.run(t, data, "")
	if got := itemsOf(t, steps["Agent"], 0); len(got) != 1 || !strings.Contains(toString(got[0]["output"]), `found go \"generics\"`) {
		t.Fatalf("tavily tool: %v (%s)", got, steps["Agent"].Error)
	}
	if b := f.find(t, "POST", "/search").body; !strings.Contains(b, `"search_depth":"advanced"`) || strings.Contains(b, "time_range") {
		t.Fatalf("tavily body: %s", b)
	}
	if got := itemsOf(t, steps["Scrape"], 0); got[0]["body"] != "<html>hn</html>" {
		t.Fatalf("scrapeninja: %v", got)
	}
	if got := itemsOf(t, steps["Infra"], 0); got[0]["response"] != "agency matters" {
		t.Fatalf("infranodus: %v", got)
	}
	if b := f.find(t, "POST", "/v2/posts").body; !strings.Contains(b, `"targetType":"youtube"`) || !strings.Contains(b, `"privacyStatus":"private"`) || !strings.Contains(b, `"mediaUrls":["https://x/v.mp4"]`) {
		t.Fatalf("blotato body: %s", b)
	}
	if got := itemsOf(t, steps["Transcript"], 0); got[0]["text"] != "Hello & welcome to flow" || got[0]["youtubeId"] == nil {
		t.Fatalf("transcript node: %v (%s)", got, steps["Transcript"].Error)
	}
	if got := itemsOf(t, steps["Code"], 0); got[0]["n"] != 2.0 || got[0]["first"] != "Hello & welcome" {
		t.Fatalf("youtube-transcript module: %v (%s)", got, steps["Code"].Error)
	}
	if r := f.find(t, "GET", "/api/timedtext"); strings.Contains(r.query, "fmt=") {
		t.Fatalf("transcript must be fetched as XML: %s", r.query)
	}
}
