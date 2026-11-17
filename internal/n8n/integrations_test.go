package n8n

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

type recorded struct {
	method, path, query, body, auth, ctype string
}

type fakeAPI struct {
	mu   sync.Mutex
	reqs []recorded
	srv  *httptest.Server
}

func newFakeAPI(t *testing.T, handle func(r recorded) (int, string, interface{})) *fakeAPI {
	f := &fakeAPI{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rec := recorded{r.Method, r.URL.Path, r.URL.RawQuery, string(b), r.Header.Get("Authorization"), r.Header.Get("Content-Type")}
		f.mu.Lock()
		f.reqs = append(f.reqs, rec)
		f.mu.Unlock()
		code, ct, body := handle(rec)
		w.Header().Set("Content-Type", ct)
		w.WriteHeader(code)
		if s, ok := body.(string); ok {
			_, _ = io.WriteString(w, s)
			return
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) find(t *testing.T, method, pathPart string) recorded {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.reqs {
		if r.method == method && strings.Contains(r.path, pathPart) {
			return r
		}
	}
	t.Fatalf("no %s request to %s; got %+v", method, pathPart, f.reqs)
	return recorded{}
}

func integrationHarness(t *testing.T, base string) *harness {
	creds := NewStaticCredentials()
	creds.Set("telegramApi", "*", map[string]interface{}{"accessToken": "123:ABC"})
	for _, c := range []string{"googleSheetsOAuth2Api", "gmailOAuth2", "googleDriveOAuth2Api"} {
		creds.Set(c, "*", map[string]interface{}{"oauthTokenData": map[string]interface{}{"access_token": "g-token"}})
	}
	return newHarness(t, &Options{
		Credentials: creds,
		Outbound:    node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}},
		APIBaseURLs: map[string]string{"telegram": base, "sheets": base, "gmail": base, "drive": base},
	})
}

func credNode(n map[string]interface{}, typ string) map[string]interface{} {
	n["credentials"] = map[string]interface{}{typ: map[string]interface{}{"id": "1", "name": typ}}
	return n
}

func TestTelegramNode(t *testing.T) {
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case strings.HasSuffix(r.path, "/getFile"):
			return 200, "application/json", map[string]interface{}{"ok": true, "result": map[string]interface{}{"file_id": "F1", "file_path": "photos/p.jpg"}}
		case strings.Contains(r.path, "/file/bot"):
			return 200, "image/jpeg", "JPEGDATA"
		}
		return 200, "application/json", map[string]interface{}{"ok": true, "result": map[string]interface{}{"message_id": 7}}
	})
	h := integrationHarness(t, f.srv.URL)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		credNode(nd("Send", "telegram", 1.2, map[string]interface{}{"chatId": "={{ $json.chat }}", "text": "=Hi {{ $json.name }}",
			"replyMarkup": "inlineKeyboard", "inlineKeyboard": map[string]interface{}{"rows": []interface{}{map[string]interface{}{"row": map[string]interface{}{"buttons": []interface{}{
				map[string]interface{}{"text": "OK", "additionalFields": map[string]interface{}{"callback_data": "ok"}}}}}}},
			"additionalFields": map[string]interface{}{"parse_mode": "HTML", "appendAttribution": false}}), "telegramApi"),
		credNode(nd("Get file", "telegram", 1.2, map[string]interface{}{"resource": "file", "fileId": "F1"}), "telegramApi"),
	}, "Start->Send", "Send->Get file")
	_, steps := h.run(t, data, `{"chat":"42","name":"Ada"}`)

	send := f.find(t, "POST", "/bot123:ABC/sendMessage")
	var body map[string]interface{}
	_ = json.Unmarshal([]byte(send.body), &body)
	if body["chat_id"] != "42" || body["text"] != "Hi Ada" || body["parse_mode"] != "HTML" || !strings.Contains(send.body, `"callback_data":"ok"`) {
		t.Fatalf("sendMessage body: %s", send.body)
	}
	if got := itemsOf(t, steps["Send"], 0); got[0]["ok"] != true {
		t.Fatalf("send output: %v", got)
	}
	file := decodeOutput(steps["Get file"].Output)[0][0]
	bin, _ := file.Binary["data"].(map[string]interface{})
	if raw, _ := base64.StdEncoding.DecodeString(toString(bin["data"])); string(raw) != "JPEGDATA" || bin["mimeType"] != "image/jpeg" || bin["fileName"] != "p.jpg" {
		t.Fatalf("file binary: %v", bin)
	}
	if strings.Contains(steps["Send"].Output, "123:ABC") {
		t.Fatal("bot token leaked into output")
	}
}

func TestGoogleSheetsNode(t *testing.T) {
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case r.method == "GET" && strings.HasSuffix(r.path, "/spreadsheets/DOC1"):
			return 200, "application/json", map[string]interface{}{"sheets": []interface{}{map[string]interface{}{"properties": map[string]interface{}{"sheetId": 0, "title": "Leads"}}}}
		case r.method == "GET" && strings.Contains(r.path, "/values/"):
			return 200, "application/json", map[string]interface{}{"values": []interface{}{
				[]interface{}{"email", "name", "status"},
				[]interface{}{"a@x.com", "Ann", "new"},
				[]interface{}{"b@x.com", "Bob", "won"},
			}}
		}
		return 200, "application/json", map[string]interface{}{}
	})
	h := integrationHarness(t, f.srv.URL)
	doc := map[string]interface{}{"__rl": true, "mode": "list", "value": "DOC1"}
	sheet := map[string]interface{}{"__rl": true, "mode": "list", "value": "gid=0"}
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		credNode(nd("Read", "googleSheets", 4.5, map[string]interface{}{"documentId": doc, "sheetName": sheet,
			"filtersUI": map[string]interface{}{"values": []interface{}{map[string]interface{}{"lookupColumn": "status", "lookupValue": "won"}}}}), "googleSheetsOAuth2Api"),
		credNode(nd("Upsert", "googleSheets", 4.5, map[string]interface{}{"operation": "appendOrUpdate", "documentId": doc, "sheetName": sheet,
			"columns": map[string]interface{}{"mappingMode": "defineBelow", "matchingColumns": []interface{}{"email"},
				"value": map[string]interface{}{"email": "={{ $json.email }}", "status": "closed", "score": 9}}}), "googleSheetsOAuth2Api"),
	}, "Start->Read", "Read->Upsert")
	_, steps := h.run(t, data, "")
	got := itemsOf(t, steps["Read"], 0)
	if len(got) != 1 || got[0]["name"] != "Bob" || got[0]["row_number"] != 3.0 {
		t.Fatalf("read: %v", got)
	}
	header := f.find(t, "PUT", "/values/")
	if !strings.Contains(header.body, `"score"`) || header.auth != "Bearer g-token" {
		t.Fatalf("new column should extend header: %+v", header)
	}
	upd := f.find(t, "POST", "/values:batchUpdate")
	if !strings.Contains(upd.body, `"'Leads'!A3"`) || !strings.Contains(upd.body, `["b@x.com","Bob","closed",9]`) {
		t.Fatalf("update row 3: %s", upd.body)
	}
}

func TestGmailAndDriveNodes(t *testing.T) {
	raw := "From: Ann <ann@x.com>\r\nTo: me@x.com\r\nSubject: =?utf-8?q?Merhaba_d=C3=BCnya?=\r\nMessage-ID: <m1@x>\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nhello body\r\n--B\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=\"a.txt\"\r\nContent-Transfer-Encoding: base64\r\n\r\naGk=\r\n--B--\r\n"
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case strings.HasSuffix(r.path, "/messages/send"):
			return 200, "application/json", map[string]interface{}{"id": "sent1", "threadId": "t1", "labelIds": []interface{}{"SENT"}}
		case strings.HasSuffix(r.path, "/messages/M1") && strings.Contains(r.query, "format=raw"):
			return 200, "application/json", map[string]interface{}{"id": "M1", "threadId": "t1", "raw": base64.URLEncoding.EncodeToString([]byte(raw))}
		case strings.HasSuffix(r.path, "/files/FID"):
			return 200, "application/json", map[string]interface{}{"id": "FID", "name": "Report", "mimeType": "application/vnd.google-apps.document"}
		case strings.HasSuffix(r.path, "/files/FID/export"):
			return 200, "text/plain", "doc text"
		case strings.Contains(r.path, "/upload/drive/v3/files"):
			return 200, "application/json", map[string]interface{}{"id": "NEW", "name": "notes.txt"}
		}
		return 404, "application/json", map[string]interface{}{"error": "nope"}
	})
	h := integrationHarness(t, f.srv.URL)
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		credNode(nd("Send", "gmail", 2.1, map[string]interface{}{"sendTo": "bob@x.com", "subject": "Çok önemli", "message": "<b>Hi</b>", "options": map[string]interface{}{"ccList": "c@x.com"}}), "gmailOAuth2"),
		credNode(nd("Get", "gmail", 2.1, map[string]interface{}{"operation": "get", "messageId": "M1", "simple": false, "options": map[string]interface{}{"downloadAttachments": true}}), "gmailOAuth2"),
		credNode(nd("Download", "googleDrive", 3, map[string]interface{}{"operation": "download", "fileId": map[string]interface{}{"__rl": true, "mode": "url", "value": "https://docs.google.com/document/d/FID/edit"},
			"options": map[string]interface{}{"googleFileConversion": map[string]interface{}{"conversion": map[string]interface{}{"docsToFormat": "text/plain"}}}}), "googleDriveOAuth2Api"),
		credNode(nd("Upload", "googleDrive", 3, map[string]interface{}{"name": "notes.txt", "folderId": map[string]interface{}{"__rl": true, "mode": "list", "value": "FOLDER"}}), "googleDriveOAuth2Api"),
	}, "Start->Send", "Send->Get", "Get->Download", "Download->Upload")
	_, steps := h.run(t, data, "")

	send := f.find(t, "POST", "/messages/send")
	var sb map[string]string
	_ = json.Unmarshal([]byte(send.body), &sb)
	mime, _ := base64.URLEncoding.DecodeString(sb["raw"])
	ms := string(mime)
	if !strings.Contains(ms, "To: bob@x.com") || !strings.Contains(ms, "Cc: c@x.com") || !strings.Contains(ms, "=?utf-8?q?") || !strings.Contains(ms, "text/html") {
		t.Fatalf("mime: %s", ms)
	}
	got := decodeOutput(steps["Get"].Output)[0][0]
	if got.JSON["subject"] != "Merhaba dünya" || got.JSON["text"] != "hello body" || got.Binary["attachment_0"] == nil {
		t.Fatalf("parsed email: %+v", got)
	}
	from, _ := got.JSON["from"].(map[string]interface{})
	if !strings.Contains(mustJSON(from), "ann@x.com") {
		t.Fatalf("from: %v", from)
	}
	exp := f.find(t, "GET", "/files/FID/export")
	if !strings.Contains(exp.query, "mimeType=text%2Fplain") {
		t.Fatalf("export: %+v", exp)
	}
	up := f.find(t, "POST", "/upload/drive/v3/files")
	if !strings.Contains(up.body, `"parents":["FOLDER"]`) || !strings.Contains(up.body, "doc text") || !strings.HasPrefix(up.ctype, "multipart/related") {
		t.Fatalf("upload: %+v", up)
	}
	if itemsOf(t, steps["Upload"], 0)[0]["id"] != "NEW" {
		t.Fatalf("upload output")
	}
}

func TestIntegrationAsAIToolAndGmailTrigger(t *testing.T) {
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case strings.HasSuffix(r.path, "/chat/completions"):
			if strings.Contains(r.body, `"role":"tool"`) {
				return 200, "application/json", map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant", "content": "Mail sent."}}}}
			}
			return 200, "application/json", map[string]interface{}{"choices": []interface{}{map[string]interface{}{"message": map[string]interface{}{"role": "assistant",
				"tool_calls": []interface{}{map[string]interface{}{"id": "c1", "type": "function", "function": map[string]interface{}{"name": "Send_mail", "arguments": `{"to":"zoe@x.com","subject":"Hi Zoe"}`}}}}}}}
		case strings.HasSuffix(r.path, "/messages/send"):
			return 200, "application/json", map[string]interface{}{"id": "S1", "threadId": "T1"}
		case strings.HasSuffix(r.path, "/messages") && r.method == "GET":
			return 200, "application/json", map[string]interface{}{"messages": []interface{}{map[string]interface{}{"id": "N1"}}}
		case strings.Contains(r.path, "/messages/N1"):
			return 200, "application/json", map[string]interface{}{"id": "N1", "threadId": "T9", "snippet": "new mail", "labelIds": []interface{}{"INBOX"},
				"payload": map[string]interface{}{"mimeType": "text/plain", "headers": []interface{}{map[string]interface{}{"name": "Subject", "value": "Fresh"}}}}
		case strings.HasSuffix(r.path, "/labels"):
			return 200, "application/json", map[string]interface{}{"labels": []interface{}{map[string]interface{}{"id": "INBOX", "name": "Inbox"}}}
		}
		return 404, "application/json", map[string]interface{}{}
	})
	sc := NewStaticCredentials()
	sc.Set("openAiApi", "*", map[string]interface{}{"apiKey": "k", "url": f.srv.URL})
	sc.Set("gmailOAuth2", "*", map[string]interface{}{"accessToken": "g"})
	h := newHarness(t, &Options{Credentials: sc, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}, APIBaseURLs: map[string]string{"gmail": f.srv.URL}})

	tool := credNode(map[string]interface{}{"name": "Send mail", "type": "n8n-nodes-base.gmailTool", "typeVersion": 2.1, "parameters": map[string]interface{}{
		"sendTo": "={{ $fromAI('to', 'recipient address') }}", "subject": "={{ $fromAI('subject') }}", "message": "Automated hello", "emailType": "text"}}, "gmailOAuth2")
	data := wfJSON(t, []map[string]interface{}{
		lc("Chat", "chatTrigger", 1.1, nil),
		lc("Agent", "agent", 1.7, nil),
		withCred(lc("Model", "lmChatOpenAi", 1, nil), "openAiApi"),
		tool,
	}, "Chat->Agent", "Model~ai_languageModel->Agent", "Send mail~ai_tool->Agent")
	_, steps := h.run(t, data, `{"chatInput":"email zoe","sessionId":"x"}`)
	if got := itemsOf(t, steps["Agent"], 0); got[0]["output"] != "Mail sent." {
		t.Fatalf("agent: %v", got)
	}
	first := f.find(t, "POST", "/chat/completions")
	if !strings.Contains(first.body, `"Send_mail"`) || !strings.Contains(first.body, `"to":{"description":"recipient address","type":"string"}`) {
		t.Fatalf("tool schema: %s", first.body)
	}
	send := f.find(t, "POST", "/messages/send")
	var sb map[string]string
	_ = json.Unmarshal([]byte(send.body), &sb)
	mime, _ := base64.URLEncoding.DecodeString(sb["raw"])
	if !strings.Contains(string(mime), "To: zoe@x.com") || !strings.Contains(string(mime), "Subject: Hi Zoe") {
		t.Fatalf("tool call did not use $fromAI args:\n%s", mime)
	}

	// Gmail trigger: first poll records the cursor, second returns new mail.
	trig := credNode(map[string]interface{}{"name": "New mail", "type": "n8n-nodes-base.gmailTrigger", "typeVersion": 1.2, "parameters": map[string]interface{}{"filters": map[string]interface{}{"q": "label:inbox"}}}, "gmailOAuth2")
	tdata := wfJSON(t, []map[string]interface{}{trig, nd("Subject", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{assign("s", "string", "={{ $json.Subject }}")}}})}, "New mail->Subject")
	_, rep, _ := Import(tdata, ImportOptions{})
	if rep.Triggers[0].Kind != "poll" || rep.Triggers[0].Cron[0] != "0 * * * * *" {
		t.Fatalf("poll trigger spec: %+v", rep.Triggers)
	}
	h.fixedID = "poll-wf"
	_, steps = h.run(t, tdata, `{"__trigger":"New mail"}`)
	if steps["Subject"].Status != "SKIPPED" {
		t.Fatalf("first poll should only set the cursor: %+v", steps["Subject"])
	}
	_, steps = h.run(t, tdata, `{"__trigger":"New mail"}`)
	if got := itemsOf(t, steps["Subject"], 0); len(got) != 1 || got[0]["s"] != "Fresh" {
		t.Fatalf("second poll: %v", got)
	}
	list := f.find(t, "GET", "/messages")
	if !strings.Contains(list.query, "label%3Ainbox+after%3A") {
		t.Fatalf("poll query: %s", list.query)
	}
}

func TestServiceNodes(t *testing.T) {
	f := newFakeAPI(t, func(r recorded) (int, string, interface{}) {
		switch {
		case strings.Contains(r.path, "/databases/") && strings.HasSuffix(r.path, "/query"):
			return 200, "application/json", map[string]interface{}{"results": []interface{}{map[string]interface{}{"id": "p1", "url": "u", "properties": map[string]interface{}{
				"Task Name": map[string]interface{}{"type": "title", "title": []interface{}{map[string]interface{}{"plain_text": "Ship it"}}},
				"Tags":      map[string]interface{}{"type": "multi_select", "multi_select": []interface{}{map[string]interface{}{"name": "go"}}},
				"Due":       map[string]interface{}{"type": "date", "date": map[string]interface{}{"start": "2026-01-01"}},
			}}}, "has_more": false}
		case strings.HasSuffix(r.path, "/chat.postMessage"):
			return 200, "application/json", map[string]interface{}{"ok": true, "ts": "1.2"}
		case strings.HasSuffix(r.path, "/PHONE/messages"):
			return 200, "application/json", map[string]interface{}{"messages": []interface{}{map[string]interface{}{"id": "wamid"}}}
		case strings.HasSuffix(r.path, "/calendars/me@x.com/events"):
			return 200, "application/json", map[string]interface{}{"id": "ev1", "htmlLink": "l"}
		case strings.HasSuffix(r.path, "/v1/documents/DOC"):
			return 200, "application/json", map[string]interface{}{"documentId": "DOC", "body": map[string]interface{}{"content": []interface{}{
				map[string]interface{}{"paragraph": map[string]interface{}{"elements": []interface{}{map[string]interface{}{"textRun": map[string]interface{}{"content": "Hello doc\n"}}}}}}}}
		case strings.HasSuffix(r.path, "/wp-json/wp/v2/posts"):
			return 201, "application/json", map[string]interface{}{"id": 99, "status": "draft"}
		case strings.HasPrefix(r.path, "/hook"):
			return 200, "application/json", map[string]interface{}{"id": "d1"}
		case strings.HasSuffix(r.path, "/v19.0/me/feed"):
			return 200, "application/json", map[string]interface{}{"id": "post1"}
		}
		return 404, "application/json", map[string]interface{}{"error": r.path}
	})
	creds := NewStaticCredentials()
	creds.Set("notionApi", "*", map[string]interface{}{"apiKey": "n"})
	creds.Set("slackApi", "*", map[string]interface{}{"accessToken": "xoxb"})
	creds.Set("whatsAppApi", "*", map[string]interface{}{"accessToken": "wa"})
	creds.Set("googleCalendarOAuth2Api", "*", map[string]interface{}{"accessToken": "g"})
	creds.Set("googleDocsOAuth2Api", "*", map[string]interface{}{"accessToken": "g"})
	creds.Set("wordpressApi", "*", map[string]interface{}{"url": f.srv.URL, "username": "u", "password": "p"})
	creds.Set("discordWebhookApi", "*", map[string]interface{}{"webhookUri": f.srv.URL + "/hook"})
	creds.Set("facebookGraphApi", "*", map[string]interface{}{"accessToken": "fb"})
	base := f.srv.URL
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}},
		APIBaseURLs: map[string]string{"notion": base, "slack": base, "whatsapp": base, "calendar": base, "docs": base, "facebook": base}})
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		credNode(nd("Notion", "notion", 2.2, map[string]interface{}{"resource": "databasePage", "operation": "getAll", "databaseId": map[string]interface{}{"__rl": true, "mode": "url", "value": "https://notion.so/abcdef0123456789abcdef0123456789?v=1"},
			"filterType": "manual", "filters": map[string]interface{}{"conditions": []interface{}{map[string]interface{}{"key": "Tags|multi_select", "condition": "contains", "multiSelectValue": "go"}}}}), "notionApi"),
		credNode(nd("Slack", "slack", 2.2, map[string]interface{}{"select": "channel", "channelId": map[string]interface{}{"__rl": true, "mode": "id", "value": "C1"}, "text": "={{ $json.name }} is due",
			"otherOptions": map[string]interface{}{"thread_ts": map[string]interface{}{"replyValues": map[string]interface{}{"thread_ts": "0.1"}}}}), "slackApi"),
		credNode(nd("WhatsApp", "whatsApp", 1, map[string]interface{}{"operation": "send", "phoneNumberId": "PHONE", "recipientPhoneNumber": "+90 555", "textBody": "hi"}), "whatsAppApi"),
		credNode(nd("Calendar", "googleCalendar", 1.3, map[string]interface{}{"calendar": map[string]interface{}{"__rl": true, "mode": "id", "value": "me@x.com"},
			"start": "2026-02-01T10:00:00Z", "end": "2026-02-01T11:00:00Z", "additionalFields": map[string]interface{}{"summary": "Sync", "attendees": "a@x.com,b@x.com"}}), "googleCalendarOAuth2Api"),
		credNode(nd("Docs", "googleDocs", 2, map[string]interface{}{"operation": "get", "documentURL": "https://docs.google.com/document/d/DOC/edit"}), "googleDocsOAuth2Api"),
		credNode(nd("WP", "wordpress", 1, map[string]interface{}{"title": "Hello", "additionalFields": map[string]interface{}{"content": "body", "status": "draft"}}), "wordpressApi"),
		credNode(nd("Discord", "discord", 2, map[string]interface{}{"content": "ping"}), "discordWebhookApi"),
		credNode(nd("FB", "facebookGraphApi", 1, map[string]interface{}{"graphApiVersion": "v19.0", "node": "me", "edge": "feed", "httpRequestMethod": "POST"}), "facebookGraphApi"),
	}, "Start->Notion", "Notion->Slack", "Slack->WhatsApp", "WhatsApp->Calendar", "Calendar->Docs", "Docs->WP", "WP->Discord", "Discord->FB")
	_, steps := h.run(t, data, "")

	page := itemsOf(t, steps["Notion"], 0)[0]
	if page["name"] != "Ship it" || page["property_task_name"] != "Ship it" || fmt.Sprint(page["property_tags"]) != "[go]" || page["property_due"] != "2026-01-01" {
		t.Fatalf("notion simplify: %v", page)
	}
	q := f.find(t, "POST", "/query")
	if !strings.Contains(q.path, "abcdef0123456789abcdef0123456789") || !strings.Contains(q.body, `"multi_select":{"contains":"go"}`) {
		t.Fatalf("notion query: %+v", q)
	}
	sl := f.find(t, "POST", "/chat.postMessage")
	if !strings.Contains(sl.body, `"text":"Ship it is due"`) || !strings.Contains(sl.body, `"thread_ts":"0.1"`) || sl.auth != "Bearer xoxb" {
		t.Fatalf("slack: %+v", sl)
	}
	if wa := f.find(t, "POST", "/PHONE/messages"); !strings.Contains(wa.body, `"to":"90555"`) || !strings.Contains(wa.body, `"body":"hi"`) {
		t.Fatalf("whatsapp: %s", wa.body)
	}
	if cal := f.find(t, "POST", "/events"); !strings.Contains(cal.body, `"dateTime":"2026-02-01T10:00:00Z"`) || !strings.Contains(cal.body, `{"email":"b@x.com"}`) {
		t.Fatalf("calendar: %s", cal.body)
	}
	if got := itemsOf(t, steps["Docs"], 0)[0]; got["content"] != "Hello doc\n" {
		t.Fatalf("docs: %v", got)
	}
	if wp := f.find(t, "POST", "/wp-json/wp/v2/posts"); !strings.HasPrefix(wp.auth, "Basic ") || !strings.Contains(wp.body, `"title":"Hello"`) {
		t.Fatalf("wordpress: %+v", wp)
	}
	if d := f.find(t, "POST", "/hook"); !strings.Contains(d.query, "wait=true") || !strings.Contains(d.body, `"content":"ping"`) {
		t.Fatalf("discord: %+v", d)
	}
	if fb := f.find(t, "POST", "/v19.0/me/feed"); !strings.Contains(fb.query, "access_token=fb") {
		t.Fatalf("facebook: %+v", fb)
	}
}
