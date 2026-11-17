package n8n

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

// pagedAPI serves 7 records, 3 per page, via ?page=N (1-based), a JSON
// body offset, or a next-URL link; /status returns 404 past the last page.
func pagedAPI(t *testing.T) (*httptest.Server, *[]string) {
	var mu sync.Mutex
	var seen []string
	records := func(from int) []interface{} {
		var out []interface{}
		for i := from; i < from+3 && i < 7; i++ {
			out = append(out, map[string]interface{}{"id": float64(i + 1)})
		}
		return out
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.RequestURI())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/pages", "/status":
			p, _ := strconv.Atoi(r.URL.Query().Get("page"))
			if p < 1 {
				p = 1
			}
			if r.URL.Path == "/status" && (p-1)*3 >= 7 {
				w.WriteHeader(404)
				_, _ = w.Write([]byte(`{"error":"no such page"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"data": orEmpty(records((p - 1) * 3))})
		case "/offset":
			var body struct{ Offset int }
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
			res := map[string]interface{}{"data": orEmpty(records(body.Offset))}
			if body.Offset+3 < 7 {
				res["offset"] = body.Offset + 3
			}
			_ = json.NewEncoder(w).Encode(res)
		case "/next":
			c, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
			res := map[string]interface{}{"data": orEmpty(records(c))}
			if c+3 < 7 {
				res["next"] = fmt.Sprintf("/next?cursor=%d", c+3)
			}
			_ = json.NewEncoder(w).Encode(res)
		case "/list":
			p, _ := strconv.Atoi(r.URL.Query().Get("page"))
			_ = json.NewEncoder(w).Encode(orEmpty(records(p * 3)))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

func orEmpty(v []interface{}) []interface{} {
	if v == nil {
		return []interface{}{}
	}
	return v
}

func TestHTTPPagination(t *testing.T) {
	srv, seen := pagedAPI(t)
	httpNode := func(params map[string]interface{}) []byte {
		base := map[string]interface{}{"options": map[string]interface{}{"pagination": map[string]interface{}{"pagination": params["pagination"]}}}
		for k, v := range params {
			if k != "pagination" {
				base[k] = v
			}
		}
		return wfJSON(t, []map[string]interface{}{
			nd("Start", "manualTrigger", 1, nil),
			nd("HTTP", "httpRequest", 4.2, base),
		}, "Start->HTTP")
	}
	cases := []struct {
		name     string
		params   map[string]interface{}
		want     int // items (pages for object responses, records for array responses)
		requests int
	}{
		{"pageCount with limit", map[string]interface{}{"url": srv.URL + "/pages", "pagination": map[string]interface{}{
			"parameters":  map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "page", "value": "={{ $pageCount + 1 }}"}}},
			"maxRequests": 2, "limitPagesFetched": true}}, 2, 2},
		{"request qs + completeExpression", map[string]interface{}{"url": srv.URL + "/pages", "sendQuery": true,
			"queryParameters": map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "page", "value": "1"}}},
			"pagination": map[string]interface{}{
				"parameters":             map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "page", "value": "={{ $request.qs.page + 1 }}"}}},
				"paginationCompleteWhen": "other", "completeExpression": "={{ $response.body.data.isEmpty() }}"}}, 4, 4},
		{"body offset", map[string]interface{}{"url": srv.URL + "/offset", "method": "POST", "sendBody": true, "specifyBody": "json", "jsonBody": `{"offset":0}`,
			"pagination": map[string]interface{}{
				"parameters":             map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "offset", "type": "body", "value": "={{ $response.body.offset }}"}}},
				"paginationCompleteWhen": "other", "completeExpression": "={{ $response.body.offset == undefined }}"}}, 3, 3},
		{"next URL, empty response ends", map[string]interface{}{"url": srv.URL + "/list", "sendQuery": true,
			"queryParameters": map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "page", "value": "0"}}},
			"pagination": map[string]interface{}{"paginationMode": "responseContainsNextURL",
				"nextURL": `={{ "` + srv.URL + `/list?page=" + ($pageCount) }}`}}, 7, 4},
		{"status code ends", map[string]interface{}{"url": srv.URL + "/status", "pagination": map[string]interface{}{
			"parameters":             map[string]interface{}{"parameters": []interface{}{map[string]interface{}{"name": "page", "value": "={{ $pageCount + 1 }}"}}},
			"paginationCompleteWhen": "receiveSpecificStatusCodes", "statusCodesWhenComplete": "404"}}, 3, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			*seen = nil
			h := newHarness(t, &Options{Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
			_, steps := h.run(t, httpNode(c.params), "")
			got := itemsOf(t, steps["HTTP"], 0)
			if len(got) != c.want || len(*seen) != c.requests {
				t.Fatalf("items %d (want %d), requests %v (want %d): %v", len(got), c.want, *seen, c.requests, got)
			}
		})
	}
}
