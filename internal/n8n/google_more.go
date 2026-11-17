package n8n

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/textproto"
	"net/url"
	"strings"
	"time"
)

func jsonUnmarshal(b []byte, v interface{}) error { return json.Unmarshal(b, v) }

// multipartBody builds a multipart/form-data body with fields and one file.
func multipartBody(fields map[string]string, fileField, fileName, mt string, data []byte) ([]byte, string) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = w.WriteField(k, v)
	}
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, fileField, fileName))
	h.Set("Content-Type", mt)
	p, _ := w.CreatePart(h)
	_, _ = p.Write(data)
	_ = w.Close()
	return buf.Bytes(), w.FormDataContentType()
}

// ---------------------------------------------------------------- Google Calendar

func googleCalendarNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("calendar", "googleCalendarOAuth2Api")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("calendar", "https://www.googleapis.com") + "/calendar/v3"
	call := func(method, path string, body interface{}) (map[string]interface{}, error) {
		res, _, err := ec.apiCall(method, base+path, h, body, "")
		return res, err
	}
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		r, o := ec.resourceOp(i, "event", "create")
		cal, _ := ec.rlString(i, "calendar")
		if cal == "" {
			cal = "primary"
		}
		calPath := "/calendars/" + url.PathEscape(cal)
		sendUpdates := func(path string) string {
			s, _ := ec.str(i, "options.sendUpdates", "")
			if s == "" {
				s, _ = ec.str(i, "additionalFields.sendUpdates", "")
			}
			if s != "" {
				return path + "?sendUpdates=" + url.QueryEscape(s)
			}
			return path
		}
		switch r + "." + o {
		case "event.create":
			ev, q, err := calendarEvent(ec, i, "additionalFields", true)
			if err != nil {
				return nil, err
			}
			path := calPath + "/events"
			if s, _ := ec.str(i, "additionalFields.sendUpdates", ""); s != "" {
				q.Set("sendUpdates", s)
			}
			if len(q) > 0 {
				path += "?" + q.Encode()
			}
			res, err := call("POST", path, ev)
			return one(res), err
		case "event.update":
			id, _ := ec.str(i, "eventId", "")
			ev, q, err := calendarEvent(ec, i, "updateFields", false)
			if err != nil {
				return nil, err
			}
			path := calPath + "/events/" + url.PathEscape(id)
			if len(q) > 0 {
				path += "?" + q.Encode()
			}
			res, err := call("PATCH", path, ev)
			return one(res), err
		case "event.get":
			id, _ := ec.str(i, "eventId", "")
			res, err := call("GET", calPath+"/events/"+url.PathEscape(id), nil)
			return one(res), err
		case "event.delete":
			id, _ := ec.str(i, "eventId", "")
			if _, err := call("DELETE", sendUpdates(calPath+"/events/"+url.PathEscape(id)), nil); err != nil {
				return nil, err
			}
			return one(map[string]interface{}{"success": true}), nil
		case "event.getAll":
			q := url.Values{}
			for _, k := range []string{"timeMin", "timeMax"} {
				v, _ := ec.str(i, k, "")
				if v == "" {
					v, _ = ec.str(i, "options."+k, "")
				}
				if v != "" {
					if t, ok := timeOf(v); ok {
						v = t.UTC().Format(time.RFC3339)
					}
					q.Set(k, v)
				}
			}
			for _, k := range []string{"query", "orderBy", "timeZone", "iCalUID", "updatedMin"} {
				if v, _ := ec.str(i, "options."+k, ""); v != "" {
					if k == "query" {
						k = "q"
					}
					q.Set(k, v)
				}
			}
			if s, _ := ec.boolean(i, "options.singleEvents", false); s || q.Get("orderBy") == "startTime" {
				q.Set("singleEvents", "true")
			}
			returnAll, _ := ec.boolean(i, "returnAll", false)
			limit, _ := ec.num(i, "limit", 50)
			var out []Item
			for token := ""; ; {
				if token != "" {
					q.Set("pageToken", token)
				}
				q.Set("maxResults", "250")
				res, err := call("GET", calPath+"/events?"+q.Encode(), nil)
				if err != nil {
					return nil, err
				}
				out = append(out, listItems(res["items"])...)
				token = toString(res["nextPageToken"])
				if token == "" || !returnAll && len(out) >= int(limit) {
					break
				}
			}
			if !returnAll && len(out) > int(limit) {
				out = out[:int(limit)]
			}
			return out, nil
		case "calendar.availability":
			tmin, _ := ec.str(i, "timeMin", "")
			tmax, _ := ec.str(i, "timeMax", "")
			body := map[string]interface{}{"timeMin": isoTime(tmin), "timeMax": isoTime(tmax), "items": []interface{}{map[string]interface{}{"id": cal}}}
			if tz, _ := ec.str(i, "options.timezone", ""); tz != "" {
				body["timeZone"] = tz
			}
			res, err := call("POST", "/freeBusy", body)
			if err != nil {
				return nil, err
			}
			cals, _ := res["calendars"].(map[string]interface{})
			c, _ := cals[cal].(map[string]interface{})
			busy := asList(c["busy"])
			switch f, _ := ec.str(i, "options.outputFormat", "availability"); f {
			case "raw":
				return one(res), nil
			case "bookedSlots":
				return listItems(busy), nil
			default:
				return one(map[string]interface{}{"available": len(busy) == 0}), nil
			}
		}
		return nil, unsupportedOp("Google Calendar", r, o)
	})
}

func isoTime(s string) string {
	if t, ok := timeOf(s); ok {
		return t.UTC().Format(time.RFC3339)
	}
	return s
}

func calendarEvent(ec *execCtx, i int, fieldsKey string, create bool) (map[string]interface{}, url.Values, error) {
	ev := map[string]interface{}{}
	q := url.Values{}
	fv, err := ec.param(i, fieldsKey, map[string]interface{}{})
	if err != nil {
		return nil, nil, err
	}
	f, _ := fv.(map[string]interface{})
	tz := toString(f["timezone"])
	allDay := toString(f["allday"]) == "yes" || toBool(f["allday"])
	when := func(v string) map[string]interface{} {
		if allDay {
			if t, ok := timeOf(v); ok {
				return map[string]interface{}{"date": t.Format("2006-01-02")}
			}
		}
		m := map[string]interface{}{"dateTime": isoTime(v)}
		if tz != "" {
			m["timeZone"] = tz
		}
		return m
	}
	start, _ := ec.str(i, "start", "")
	end, _ := ec.str(i, "end", "")
	if s := toString(f["start"]); s != "" {
		start = s
	}
	if e := toString(f["end"]); e != "" {
		end = e
	}
	if start != "" {
		ev["start"] = when(start)
	}
	if end != "" {
		ev["end"] = when(end)
	}
	for _, k := range []string{"summary", "description", "location", "colorId", "visibility", "id", "guestsCanInviteOthers", "guestsCanModify", "guestsCanSeeOtherGuests"} {
		if v, ok := f[k]; ok && v != "" {
			ev[k] = v
		}
	}
	if v, ok := f["showMeAs"]; ok {
		ev["transparency"] = v
	}
	if a, ok := f["attendees"]; ok {
		var att []interface{}
		list := asList(a)
		if s, ok := a.(string); ok {
			list = splitAny(s)
		}
		for _, x := range list {
			for _, e := range splitList(toString(x)) {
				att = append(att, map[string]interface{}{"email": e})
			}
		}
		ev["attendees"] = att
	}
	if r, ok := f["recurrence"]; ok {
		ev["recurrence"] = []interface{}{r}
	}
	if cd, ok := f["conferenceDataUi"].(map[string]interface{}); ok {
		if v, ok := cd["conferenceDataValues"].(map[string]interface{}); ok && toString(v["conferenceSolution"]) != "" {
			ev["conferenceData"] = map[string]interface{}{"createRequest": map[string]interface{}{
				"requestId": fmt.Sprintf("n8n-%d", time.Now().UnixNano()), "conferenceSolutionKey": map[string]interface{}{"type": v["conferenceSolution"]}}}
			q.Set("conferenceDataVersion", "1")
		}
	}
	if create {
		if def, _ := ec.boolean(i, "useDefaultReminders", true); !def {
			ev["reminders"] = map[string]interface{}{"useDefault": false}
		}
	}
	return ev, q, nil
}

// ---------------------------------------------------------------- Google Docs

func googleDocsNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("docs", "googleDocsOAuth2Api")
	if err != nil {
		return nil, err
	}
	base := ec.apiBase("docs", "https://docs.googleapis.com") + "/v1/documents"
	return eachItem(ec, func(i int, it Item) ([]Item, error) {
		o, _ := ec.str(i, "operation", "create")
		docID := func() string {
			s, _ := ec.str(i, "documentURL", "")
			return idFromURL(s)
		}
		switch o {
		case "create":
			title, _ := ec.str(i, "title", "Untitled")
			res, _, err := ec.apiCall("POST", base, h, map[string]interface{}{"title": title}, "")
			if err != nil {
				return nil, err
			}
			if folder, _ := ec.rlString(i, "folderId"); folder != "" && folder != "default" {
				q := url.Values{"addParents": {folder}, "removeParents": {"root"}, "supportsAllDrives": {"true"}}
				_, _, _ = ec.apiCall("PATCH", ec.apiBase("drive", "https://www.googleapis.com")+"/drive/v3/files/"+url.PathEscape(toString(res["documentId"]))+"?"+q.Encode(), h, map[string]interface{}{}, "")
			}
			return one(map[string]interface{}{"id": res["documentId"], "name": res["title"]}), nil
		case "get":
			res, _, err := ec.apiCall("GET", base+"/"+url.PathEscape(docID()), h, nil, "")
			if err != nil {
				return nil, err
			}
			if simple, _ := ec.boolean(i, "simple", true); simple {
				return one(map[string]interface{}{"documentId": res["documentId"], "content": docText(res)}), nil
			}
			return one(res), nil
		case "update":
			av, err := ec.param(i, "actionsUi.actionFields", nil)
			if err != nil {
				return nil, err
			}
			var reqs []interface{}
			for _, raw := range asList(av) {
				a, _ := raw.(map[string]interface{})
				switch toString(a["action"]) {
				case "insert":
					req := map[string]interface{}{"text": toString(a["text"])}
					if toString(a["locationChoice"]) == "location" {
						req["location"] = map[string]interface{}{"index": toFloatDefault(a["index"], 1)}
					} else {
						req["endOfSegmentLocation"] = map[string]interface{}{"segmentId": ""}
					}
					reqs = append(reqs, map[string]interface{}{"insertText": req})
				case "replaceAll":
					reqs = append(reqs, map[string]interface{}{"replaceAllText": map[string]interface{}{
						"replaceText": a["replaceText"], "containsText": map[string]interface{}{"text": a["text"], "matchCase": toBool(a["matchCase"])}}})
				default:
					return nil, permanentError{fmt.Sprintf("Google Docs: action %q is not supported", a["action"])}
				}
			}
			res, _, err := ec.apiCall("POST", base+"/"+url.PathEscape(docID())+":batchUpdate", h, map[string]interface{}{"requests": reqs}, "")
			return one(res), err
		}
		return nil, unsupportedOp("Google Docs", "document", o)
	})
}

func docText(doc map[string]interface{}) string {
	body, _ := doc["body"].(map[string]interface{})
	var b strings.Builder
	for _, raw := range asList(body["content"]) {
		el, _ := raw.(map[string]interface{})
		p, _ := el["paragraph"].(map[string]interface{})
		for _, e := range asList(p["elements"]) {
			em, _ := e.(map[string]interface{})
			if tr, ok := em["textRun"].(map[string]interface{}); ok {
				b.WriteString(toString(tr["content"]))
			}
		}
	}
	return b.String()
}
