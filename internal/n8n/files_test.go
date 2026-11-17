package n8n

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dgmos/flow/internal/node"
)

// minimalPDF builds a one-page PDF containing text.
func minimalPDF(text string) []byte {
	stream := fmt.Sprintf("BT /F1 18 Tf 72 712 Td (%s) Tj ET", text)
	objs := []string{
		"<< /Type /Catalog /Pages 2 0 R >>",
		"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
		"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
		fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(stream), stream),
		"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
	}
	var b bytes.Buffer
	b.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objs))
	for i, o := range objs {
		offsets[i] = b.Len()
		fmt.Fprintf(&b, "%d 0 obj\n%s\nendobj\n", i+1, o)
	}
	xref := b.Len()
	fmt.Fprintf(&b, "xref\n0 %d\n0000000000 65535 f \n", len(objs)+1)
	for _, off := range offsets {
		fmt.Fprintf(&b, "%010d 00000 n \n", off)
	}
	fmt.Fprintf(&b, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objs)+1, xref)
	return b.Bytes()
}

func TestFileNodes(t *testing.T) {
	h := newHarness(t, nil)
	pdf := base64.StdEncoding.EncodeToString(minimalPDF("Hello PDF"))
	csvData := base64.StdEncoding.EncodeToString([]byte("name,qty\nApple,3\nPear,5\n"))
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Seed", "set", 3.4, map[string]interface{}{"assignments": map[string]interface{}{"assignments": []interface{}{
			assign("pdf", "string", pdf), assign("csv", "string", csvData),
			assign("page", "string", `<div><h1 class="t">Title</h1><ul><li><a href="/a">A</a></li><li><a href="/b">B</a></li></ul></div>`),
			assign("doc", "string", `<order id="7"><item>x</item><item>y</item><total>2</total></order>`),
		}}}),
		nd("PDF bin", "convertToFile", 1.1, map[string]interface{}{"operation": "toBinary", "sourceProperty": "pdf"}),
		nd("PDF text", "extractFromFile", 1, map[string]interface{}{"operation": "pdf"}),
		nd("CSV bin", "convertToFile", 1.1, map[string]interface{}{"operation": "toBinary", "sourceProperty": "csv", "options": map[string]interface{}{"fileName": "a.csv"}}),
		nd("CSV rows", "extractFromFile", 1, map[string]interface{}{}),
		nd("To CSV", "convertToFile", 1.1, map[string]interface{}{}),
		nd("Back", "extractFromFile", 1, map[string]interface{}{"operation": "binaryToPropery"}),
		nd("Scrape", "html", 1.2, map[string]interface{}{"operation": "extractHtmlContent", "dataPropertyName": "page", "extractionValues": map[string]interface{}{"values": []interface{}{
			map[string]interface{}{"key": "title", "cssSelector": "h1.t"},
			map[string]interface{}{"key": "links", "cssSelector": "a", "returnValue": "attribute", "attribute": "href", "returnArray": true},
		}}}),
		nd("MD", "markdown", 1, map[string]interface{}{"html": "={{ $('Seed').item.json.page }}"}),
		nd("XML", "xml", 1, map[string]interface{}{"dataPropertyName": "doc"}),
	}, "Start->Seed", "Seed->PDF bin", "PDF bin->PDF text", "Seed->CSV bin", "CSV bin->CSV rows", "CSV rows->To CSV", "To CSV->Back",
		"Seed->Scrape", "Scrape->MD", "Seed->XML")
	_, steps := h.run(t, data, "")

	if got := itemsOf(t, steps["PDF text"], 0); !strings.Contains(toString(got[0]["text"]), "Hello PDF") || got[0]["numpages"] != 1.0 {
		t.Fatalf("pdf: %v", got)
	}
	rows := itemsOf(t, steps["CSV rows"], 0)
	if len(rows) != 2 || rows[1]["name"] != "Pear" || rows[1]["qty"] != "5" {
		t.Fatalf("csv rows: %v", rows)
	}
	back := itemsOf(t, steps["Back"], 0)
	raw, _ := base64.StdEncoding.DecodeString(toString(back[0]["data"]))
	if string(raw) != "name,qty\nApple,3\nPear,5\n" {
		t.Fatalf("csv round trip: %q", raw)
	}
	sc := itemsOf(t, steps["Scrape"], 0)[0]
	if sc["title"] != "Title" || fmt.Sprint(sc["links"]) != "[/a /b]" {
		t.Fatalf("scrape: %v", sc)
	}
	if mdText := toString(itemsOf(t, steps["MD"], 0)[0]["data"]); !strings.Contains(mdText, "# Title") || !strings.Contains(mdText, "[A](/a)") {
		t.Fatalf("markdown: %q", mdText)
	}
	x := itemsOf(t, steps["XML"], 0)[0]
	order, _ := x["order"].(map[string]interface{})
	attrs, _ := order["$"].(map[string]interface{})
	if attrs["id"] != "7" || fmt.Sprint(order["item"]) != "[x y]" || order["total"] != "2" {
		t.Fatalf("xml: %v", x)
	}
}

// fakeSMTP accepts one message and records it.
func fakeSMTP(t *testing.T) (string, func() string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var mu sync.Mutex
	var msg strings.Builder
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		w := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
		w("220 fake ESMTP")
		inData := false
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if inData {
				if line == ".\r\n" {
					inData = false
					w("250 OK queued")
					continue
				}
				mu.Lock()
				msg.WriteString(line)
				mu.Unlock()
				continue
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				w("250 fake")
			case strings.HasPrefix(cmd, "MAIL"), strings.HasPrefix(cmd, "RCPT"):
				mu.Lock()
				msg.WriteString(line)
				mu.Unlock()
				w("250 OK")
			case cmd == "DATA":
				inData = true
				w("354 go ahead")
			case cmd == "QUIT":
				w("221 bye")
				return
			default:
				w("250 OK")
			}
		}
	}()
	return ln.Addr().String(), func() string { mu.Lock(); defer mu.Unlock(); return msg.String() }
}

func TestEmailSend(t *testing.T) {
	addr, got := fakeSMTP(t)
	host, port, _ := net.SplitHostPort(addr)
	creds := NewStaticCredentials()
	creds.Set("smtp", "*", map[string]interface{}{"host": host, "port": port, "secure": false, "disableStartTls": true})
	h := newHarness(t, &Options{Credentials: creds, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	n := nd("Mail", "emailSend", 2.1, map[string]interface{}{"fromEmail": "Bot <bot@x.com>", "toEmail": "a@x.com, b@x.com", "subject": "Günaydın", "emailFormat": "html", "html": "=<p>{{ $json.msg }}</p>"})
	n["credentials"] = map[string]interface{}{"smtp": map[string]interface{}{"id": "1", "name": "smtp"}}
	data := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), n}, "Start->Mail")
	_, steps := h.run(t, data, `{"msg":"hi"}`)
	out := itemsOf(t, steps["Mail"], 0)[0]
	if fmt.Sprint(out["accepted"]) != "[a@x.com b@x.com]" {
		t.Fatalf("smtp result: %v", out)
	}
	m := got()
	if !strings.Contains(m, "RCPT TO:<b@x.com>") || !strings.Contains(m, "=?utf-8?q?G=C3=BCnayd=C4=B1n?=") || !strings.Contains(m, base64.StdEncoding.EncodeToString([]byte("<p>hi</p>"))) {
		t.Fatalf("smtp message:\n%s", m)
	}
}

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func TestUtilityNodes(t *testing.T) {
	rss := `<?xml version="1.0"?><rss version="2.0"><channel><item><title>Go 2</title><link>https://x/go2</link><pubDate>Mon, 02 Jan 2006 15:04:05 -0700</pubDate><description>&lt;p&gt;News&lt;/p&gt;</description></item></channel></rss>`
	var supaBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/feed":
			_, _ = io.WriteString(w, rss)
		case strings.HasPrefix(r.URL.Path, "/rest/v1/people"):
			b, _ := io.ReadAll(r.Body)
			supaBody = r.Method + " " + r.URL.RawQuery + " " + string(b)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `[{"id":1,"name":"Ann"}]`)
		}
	}))
	defer srv.Close()
	root := t.TempDir()
	_ = os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0o644)
	creds := NewStaticCredentials()
	creds.Set("supabaseApi", "*", map[string]interface{}{"host": srv.URL, "serviceRole": "sr"})
	h := newHarness(t, &Options{Credentials: creds, FileSystemRoot: root, Timezone: "UTC", Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	data := wfJSON(t, []map[string]interface{}{
		nd("Start", "manualTrigger", 1, nil),
		nd("Sales", "code", 2, map[string]interface{}{"jsCode": "return [{json:{r:'eu',v:10,p:'a'}},{json:{r:'eu',v:5,p:'b'}},{json:{r:'us',v:7,p:'a'}}];"}),
		nd("Sum", "summarize", 1.1, map[string]interface{}{"fieldsToSplitBy": "r", "fieldsToSummarize": map[string]interface{}{"values": []interface{}{
			map[string]interface{}{"aggregation": "sum", "field": "v"}, map[string]interface{}{"aggregation": "concatenate", "field": "p", "separateBy": "commaAndSpace"}, map[string]interface{}{"aggregation": "count", "field": "p"}}}}),
		nd("Fmt1", "dateTime", 1, map[string]interface{}{"value": "2024-03-05T10:00:00Z", "toFormat": "DD/MM/YYYY", "custom": true, "dataPropertyName": "d"}),
		nd("Add2", "dateTime", 2, map[string]interface{}{"operation": "addToDate", "magnitude": "2024-01-31T00:00:00Z", "duration": 1, "timeUnit": "months"}),
		nd("Feed", "rssFeedRead", 1.1, map[string]interface{}{"url": srv.URL + "/feed"}),
		credNode(nd("Supa", "supabase", 1, map[string]interface{}{"operation": "getAll", "tableId": "people", "filters": map[string]interface{}{"conditions": []interface{}{map[string]interface{}{"keyName": "name", "condition": "eq", "keyValue": "Ann"}}}}), "supabaseApi"),
		nd("Read", "readWriteFile", 1, map[string]interface{}{"fileSelector": "*.txt"}),
		nd("Write", "readWriteFile", 1, map[string]interface{}{"operation": "write", "fileName": "out/copy.txt"}),
	}, "Start->Sales", "Sales->Sum", "Start->Fmt1", "Start->Add2", "Start->Feed", "Start->Supa", "Start->Read", "Read->Write")
	_, steps := h.run(t, data, "")
	sum := itemsOf(t, steps["Sum"], 0)
	if len(sum) != 2 || sum[0]["r"] != "eu" || sum[0]["sum_v"] != 15.0 || sum[0]["concatenated_p"] != "a, b" || sum[0]["count_p"] != 2.0 {
		t.Fatalf("summarize: %v", sum)
	}
	if d := itemsOf(t, steps["Fmt1"], 0)[0]["d"]; d != "05/03/2024" {
		t.Fatalf("dateTime v1 moment format: %v", d)
	}
	if d := toString(itemsOf(t, steps["Add2"], 0)[0]["newDate"]); !strings.HasPrefix(d, "2024-02-29") {
		t.Fatalf("dateTime v2 add: %v", d)
	}
	if f := itemsOf(t, steps["Feed"], 0)[0]; f["title"] != "Go 2" || f["contentSnippet"] != "News" || f["isoDate"] != "2006-01-02T22:04:05.000Z" {
		t.Fatalf("rss: %v", f)
	}
	if !strings.Contains(supaBody, "GET name=eq.Ann") {
		t.Fatalf("supabase request: %s", supaBody)
	}
	if r := itemsOf(t, steps["Read"], 0); len(r) != 1 || r[0]["fileName"] != "a.txt" {
		t.Fatalf("read: %v", r)
	}
	if b, err := os.ReadFile(filepath.Join(root, "out", "copy.txt")); err != nil || string(b) != "alpha" {
		t.Fatalf("write: %q %v", b, err)
	}

	// Traversal outside the sandbox is impossible; disabled without a root.
	esc := wfJSON(t, []map[string]interface{}{nd("Start", "manualTrigger", 1, nil), nd("Read", "readWriteFile", 1, map[string]interface{}{"fileSelector": "../../../../etc/passwd"})}, "Start->Read")
	_, steps = h.run(t, esc, "")
	if got := itemsOf(t, steps["Read"], 0); len(got) != 0 {
		t.Fatalf("path traversal must not escape the sandbox: %v", got)
	}
	ec := &execCtx{opts: &Options{}}
	if _, err := ec.sandboxPath("a.txt"); err == nil {
		t.Fatal("file access must be disabled without FileSystemRoot")
	}
}
