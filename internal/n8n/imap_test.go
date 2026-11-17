package n8n

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/backend/memory"
	"github.com/emersion/go-imap/server"

	"github.com/dgmos/flow/internal/node"
)

func TestEmailReadImapTrigger(t *testing.T) {
	be := memory.New()
	srv := server.New(be)
	srv.AllowInsecureAuth = true
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	user, _ := be.Login(nil, "username", "password")
	mb, _ := user.GetMailbox("INBOX")
	inbox := mb.(*memory.Mailbox)

	creds := NewStaticCredentials()
	port := ln.Addr().(*net.TCPAddr).Port
	creds.Set("imap", "*", map[string]interface{}{"host": "127.0.0.1", "port": port, "secure": false, "user": "username", "password": "password"})
	h := newHarness(t, &Options{Credentials: creds, PollState: &memPollState{m: map[string]string{}}, Outbound: node.OutboundPolicy{AllowedHosts: []string{"127.0.0.1"}}})
	h.fixedID = "imap-wf"
	trig := credNode(map[string]interface{}{"name": "Mail", "type": "n8n-nodes-base.emailReadImap", "typeVersion": 2, "parameters": map[string]interface{}{"options": map[string]interface{}{}, "downloadAttachments": true}}, "imap")
	data := wfJSON(t, []map[string]interface{}{trig, nd("Out", "noOp", 1, nil)}, "Mail->Out")
	_, rep, _ := Import(data, ImportOptions{})
	if !rep.Runnable || rep.Triggers[0].Kind != "poll" || rep.Triggers[0].Cron[0] != imapPollCron {
		t.Fatalf("imap trigger spec: %+v %+v", rep.Triggers, rep.Blocking)
	}

	// First poll ignores mail that was already there.
	if _, steps := h.run(t, data, `{"__trigger":"Mail"}`); steps["Out"].Status != "SKIPPED" {
		t.Fatalf("first poll should only record the position: %+v", steps["Out"])
	}
	raw := "From: Ann <ann@x.com>\r\nTo: me@x.com\r\nSubject: Invoice\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nplease pay\r\n--B\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=\"inv.txt\"\r\n\r\n42 EUR\r\n--B--\r\n"
	if err := inbox.CreateMessage(nil, time.Now(), bytes.NewBufferString(raw)); err != nil {
		t.Fatal(err)
	}
	_, steps := h.run(t, data, `{"__trigger":"Mail"}`)
	out := decodeOutput(steps["Out"].Output)[0]
	if len(out) != 1 || out[0].JSON["subject"] != "Invoice" || out[0].JSON["textPlain"] != "please pay" || out[0].Binary["attachment_0"] == nil {
		t.Fatalf("new mail: %+v", out)
	}
	last := inbox.Messages[len(inbox.Messages)-1]
	seen := false
	for _, f := range last.Flags {
		seen = seen || f == imap.SeenFlag
	}
	if !seen {
		t.Fatalf("message should be marked as read: %v", last.Flags)
	}
	if _, steps := h.run(t, data, `{"__trigger":"Mail"}`); steps["Out"].Status != "SKIPPED" {
		t.Fatalf("mail must not be emitted twice: %+v", steps["Out"])
	}
}
