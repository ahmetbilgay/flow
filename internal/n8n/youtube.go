package n8n

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/url"
	"strings"
	"time"
)

// youTubeVideoParts is what part "*" expands to for video get.
const youTubeVideoParts = "contentDetails,id,liveStreamingDetails,localizations,player,recordingDetails,snippet,statistics,status,topicDetails"

// youTubeVideoOps are the supported operations of the YouTube node's video
// resource (the one used by templates).
var youTubeVideoOps = map[string]bool{"getAll": true, "get": true, "update": true, "upload": true, "delete": true}

func youTubeNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("youtube", "youTubeOAuth2Api")
	if err != nil {
		return nil, err
	}
	root := ec.apiBase("youtube", "https://www.googleapis.com")
	base := root + "/youtube/v3"
	call := func(method, path string, q url.Values, body interface{}) (map[string]interface{}, error) {
		u := base + path
		if len(q) > 0 {
			u += "?" + q.Encode()
		}
		res, _, err := ec.apiCall(method, u, h, body, "")
		return res, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "channel", "getAll")
		if r != "video" || !youTubeVideoOps[o] {
			return nil, permanentError{fmt.Sprintf("YouTube: %s/%s is not supported", r, o)}
		}
		switch o {
		case "getAll":
			q := url.Values{"part": {"snippet"}, "type": {"video"}}
			filters := asMap(mustParam(ec, "filters", map[string]interface{}{}))
			opts := asMap(mustParam(ec, "options", map[string]interface{}{}))
			if len(filters) == 0 {
				q.Set("forMine", "true")
			}
			for prefix, m := range map[string]map[string]interface{}{"options": opts, "filters": filters} {
				for k := range m {
					v, err := ec.param(i, prefix+"."+k, nil)
					if err != nil {
						return nil, err
					}
					if s := toString(v); s != "" {
						q.Set(k, youTubeTime(k, s))
					}
				}
			}
			all, _ := ec.boolean(i, "returnAll", false)
			limit, _ := ec.num(i, "limit", 25)
			var out []Item
			for {
				if !all {
					q.Set("maxResults", fmt.Sprint(int(limit)-len(out)))
				} else {
					q.Set("maxResults", "50")
				}
				res, err := call("GET", "/search", q, nil)
				if err != nil {
					return nil, err
				}
				for _, v := range asList(res["items"]) {
					out = append(out, Item{JSON: asMap(v)})
				}
				next := toString(res["nextPageToken"])
				if next == "" || (!all && len(out) >= int(limit)) || len(out) >= 5000 {
					break
				}
				q.Set("pageToken", next)
			}
			return out, nil
		case "get":
			id, _ := ec.str(i, "videoId", "")
			parts := "*"
			if v, ok := ec.raw("part"); ok {
				var ps []string
				for _, p := range asList(v) {
					ps = append(ps, toString(p))
				}
				if len(ps) > 0 {
					parts = strings.Join(ps, ",")
				}
			}
			if parts == "*" {
				parts = youTubeVideoParts
			}
			res, err := call("GET", "/videos", url.Values{"id": {id}, "part": {parts}}, nil)
			if err != nil {
				return nil, err
			}
			items := asList(res["items"])
			if len(items) == 0 {
				return nil, permanentError{fmt.Sprintf("YouTube: video %q not found", id)}
			}
			return []Item{{JSON: asMap(items[0])}}, nil
		case "delete":
			id, _ := ec.str(i, "videoId", "")
			if _, err := call("DELETE", "/videos", url.Values{"id": {id}}, nil); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "update", "upload":
			fieldsKey := "updateFields"
			if o == "upload" {
				fieldsKey = "options"
			}
			title, _ := ec.str(i, "title", "")
			snippet := map[string]interface{}{"title": title}
			if c, _ := ec.str(i, "categoryId", ""); c != "" {
				snippet["categoryId"] = c
			}
			status, recording := map[string]interface{}{}, map[string]interface{}{}
			q := url.Values{"part": {"snippet,status,recordingDetails"}}
			fields := asMap(mustParam(ec, fieldsKey, map[string]interface{}{}))
			for k := range fields {
				v, err := ec.param(i, fieldsKey+"."+k, nil)
				if err != nil {
					return nil, err
				}
				switch k {
				case "description", "defaultLanguage":
					snippet[k] = toString(v)
				case "tags":
					var tags []string
					for _, t := range strings.Split(toString(v), ",") {
						if t = strings.TrimSpace(t); t != "" {
							tags = append(tags, t)
						}
					}
					snippet["tags"] = tags
				case "privacyStatus", "license", "publishAt":
					status[k] = youTubeTime(k, toString(v))
				case "embeddable", "publicStatsViewable", "selfDeclaredMadeForKids":
					status[k] = toBool(v)
				case "recordingDate":
					recording[k] = youTubeTime(k, toString(v))
				case "notifySubscribers", "onBehalfOfContentOwner", "onBehalfOfContentOwnerChannel":
					q.Set(k, toString(v))
				}
			}
			body := map[string]interface{}{"snippet": snippet, "status": status, "recordingDetails": recording}
			if o == "update" {
				id, _ := ec.str(i, "videoId", "")
				body["id"] = id
				res, err := call("PUT", "/videos", q, body)
				return one(res), err
			}
			prop, _ := ec.str(i, "binaryProperty", "data")
			data, mt, _, err := binaryOf(it, prop)
			if err != nil {
				return nil, err
			}
			var buf bytes.Buffer
			w := multipart.NewWriter(&buf)
			meta, _ := w.CreatePart(map[string][]string{"Content-Type": {"application/json; charset=UTF-8"}})
			mb, _ := json.Marshal(body)
			_, _ = meta.Write(mb)
			media, _ := w.CreatePart(map[string][]string{"Content-Type": {mt}})
			_, _ = media.Write(data)
			_ = w.Close()
			q.Set("uploadType", "multipart")
			res, _, err := ec.apiCall("POST", root+"/upload/youtube/v3/videos?"+q.Encode(), h, buf.Bytes(), "multipart/related; boundary="+w.Boundary())
			return one(res), err
		}
		return nil, nil
	})
}

// youTubeTime normalizes date filters/fields to RFC 3339, as the API needs.
func youTubeTime(key, v string) string {
	switch key {
	case "publishedAfter", "publishedBefore", "publishAt", "recordingDate":
		if t, ok := timeOf(v); ok {
			return t.UTC().Format(time.RFC3339)
		}
	}
	return v
}
