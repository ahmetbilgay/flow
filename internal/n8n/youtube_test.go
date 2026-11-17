package n8n

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

func TestYouTubeNode(t *testing.T) {
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case strings.HasSuffix(r.path, "/youtube/v3/search"):
			q, _ := url.ParseQuery(r.query)
			if q.Get("pageToken") == "" {
				return 200, "application/json", map[string]interface{}{"nextPageToken": "P2", "items": []interface{}{map[string]interface{}{"id": map[string]interface{}{"videoId": "V1"}}}}
			}
			return 200, "application/json", map[string]interface{}{"items": []interface{}{map[string]interface{}{"id": map[string]interface{}{"videoId": "V2"}}}}
		case strings.HasSuffix(r.path, "/youtube/v3/videos") && r.method == "GET":
			return 200, "application/json", map[string]interface{}{"items": []interface{}{map[string]interface{}{"id": "V1", "snippet": map[string]interface{}{"title": "Old"}}}}
		case strings.HasSuffix(r.path, "/youtube/v3/videos") && r.method == "PUT":
			return 200, "application/json", map[string]interface{}{"id": "V1", "updated": true}
		case strings.HasSuffix(r.path, "/upload/youtube/v3/videos"):
			return 200, "application/json", map[string]interface{}{"id": "NEWV"}
		}
		return 404, "application/json", map[string]interface{}{}
	})
	creds := NewStaticCredentials()
	creds.Set("youTubeOAuth2Api", "*", map[string]interface{}{"oauthTokenData": map[string]interface{}{"access_token": "yt"}})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}, APIBaseURLs: map[string]string{"youtube": f.srv.URL}})
	yt := func(name string, p map[string]interface{}) map[string]interface{} {
		p["resource"] = "video"
		return credNode(nd(name, "youTube", 1, p), "youTubeOAuth2Api")
	}
	video := base64.StdEncoding.EncodeToString([]byte("MP4DATA"))
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		yt("Search", map[string]interface{}{"limit": 2, "filters": map[string]interface{}{"q": "go", "publishedAfter": "2024-01-02T03:04:05.000+02:00"}, "options": map[string]interface{}{"order": "date"}}),
		yt("Get", map[string]interface{}{"operation": "get", "videoId": "={{ $json.id.videoId }}", "options": map[string]interface{}{}}),
		yt("Update", map[string]interface{}{"operation": "update", "videoId": "={{ $json.id }}", "title": "={{ $json.snippet.title }} v2", "categoryId": "25",
			"updateFields": map[string]interface{}{"tags": "a, b", "privacyStatus": "private", "publishAt": "2030-01-01T10:00:00Z"}}),
		nd("Seed", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("v", "string", video)}}}),
		nd("Bin", "convertToFile", 1.1, map[string]interface{}{"operation": "toBinary", "sourceProperty": "v", "options": map[string]interface{}{"fileName": "a.mp4", "mimeType": "video/mp4"}}),
		yt("Upload", map[string]interface{}{"operation": "upload", "title": "New", "options": map[string]interface{}{"description": "d", "notifySubscribers": true}}),
	}, "Start->Search", "Search->Get", "Get->Update", "Start->Seed", "Seed->Bin", "Bin->Upload")
	_, steps := h.run(t, data, "")

	if got := itemsOf(t, steps["Search"], 0); len(got) != 2 {
		t.Fatalf("search should page to the limit: %v", got)
	}
	s := f.find(t, "GET", "/youtube/v3/search")
	q, _ := url.ParseQuery(s.query)
	if q.Get("q") != "go" || q.Get("order") != "date" || q.Get("type") != "video" || q.Get("forMine") != "" || q.Get("publishedAfter") != "2024-01-02T01:04:05Z" || s.auth != "Bearer yt" {
		t.Fatalf("search query: %v", q)
	}
	g := f.find(t, "GET", "/youtube/v3/videos")
	if !strings.Contains(g.query, "id=V1") || !strings.Contains(g.query, "statistics") {
		t.Fatalf("get: %s", g.query)
	}
	u := f.find(t, "PUT", "/youtube/v3/videos")
	for _, want := range []string{`"title":"Old v2"`, `"categoryId":"25"`, `"tags":["a","b"]`, `"privacyStatus":"private"`, `"publishAt":"2030-01-01T10:00:00Z"`, `"id":"V1"`} {
		if !strings.Contains(u.body, want) {
			t.Fatalf("update body missing %s: %s", want, u.body)
		}
	}
	up := f.find(t, "POST", "/upload/youtube/v3/videos")
	if !strings.Contains(up.query, "uploadType=multipart") || !strings.Contains(up.query, "notifySubscribers=true") || !strings.Contains(up.body, "MP4DATA") || !strings.Contains(up.body, `"description":"d"`) {
		t.Fatalf("upload: %+v", up)
	}
	if itemsOf(t, steps["Upload"], 0)[0]["id"] != "NEWV" {
		t.Fatal("upload output")
	}
}
