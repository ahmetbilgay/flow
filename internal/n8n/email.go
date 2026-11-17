package n8n

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"mime"
	"mime/multipart"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/node"
)

// emailSendNode sends mail over SMTP (credential type "smtp").
func emailSendNode(ec *execCtx) ([][]Item, error) {
	op, _ := ec.str(0, "operation", "send")
	if op != "send" && op != "sendAndWait" {
		return nil, permanentError{fmt.Sprintf("Send Email: operation %q is not supported", op)}
	}
	c, err := ec.credential("smtp")
	if err != nil {
		return nil, err
	}
	if op == "sendAndWait" {
		a, err := ec.sendAndWaitApproval(0)
		if err != nil {
			return nil, err
		}
		if _, err := sendSMTP(ec, 0, ec.items()[0], c, a); err != nil {
			return nil, err
		}
		return nil, errWaitSignal
	}
	var out []Item
	for i, it := range ec.items() {
		res, err := sendSMTP(ec, i, it, c, nil)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		out = append(out, Item{JSON: res, PairedItem: paired(i)})
	}
	return [][]Item{out}, nil
}

func sendSMTP(ec *execCtx, i int, it Item, c map[string]interface{}, a *approval) (map[string]interface{}, error) {
	from, _ := ec.str(i, "fromEmail", "")
	to, _ := ec.str(i, "toEmail", "")
	subject, _ := ec.str(i, "subject", "")
	cc, _ := ec.str(i, "options.ccEmail", "")
	bcc, _ := ec.str(i, "options.bccEmail", "")
	replyTo, _ := ec.str(i, "options.replyTo", "")
	format, _ := ec.str(i, "emailFormat", "")
	text, _ := ec.str(i, "text", "")
	htmlBody, _ := ec.str(i, "html", "")
	if format == "" {
		if htmlBody != "" {
			format = "html"
		} else {
			format = "text"
		}
	}
	if ec.version >= 2 && format == "text" && text == "" {
		text, _ = ec.str(i, "message", "")
	}
	if format == "html" && htmlBody == "" {
		htmlBody, _ = ec.str(i, "message", "")
	}
	if a != nil {
		msg, _ := ec.str(i, "message", "")
		format, htmlBody = "html", a.html(msg)
	}
	if strings.ContainsAny(from, "\r\n") {
		return nil, permanentError{"invalid from address"}
	}
	fromAddr, err := mail.ParseAddress(from)
	if err != nil {
		return nil, permanentError{fmt.Sprintf("invalid from address %q", from)}
	}
	var rcpts []string
	for _, list := range []string{to, cc, bcc} {
		if strings.TrimSpace(list) == "" {
			continue
		}
		addrs, err := mail.ParseAddressList(list)
		if err != nil {
			return nil, permanentError{fmt.Sprintf("invalid recipient list %q", list)}
		}
		for _, a := range addrs {
			rcpts = append(rcpts, a.Address)
		}
	}
	if len(rcpts) == 0 {
		return nil, permanentError{"no recipients"}
	}

	var buf bytes.Buffer
	h := textproto.MIMEHeader{}
	domain := fromAddr.Address
	if at := strings.LastIndex(domain, "@"); at >= 0 {
		domain = domain[at+1:]
	}
	msgID := fmt.Sprintf("<%d@%s>", time.Now().UnixNano(), domain)
	h.Set("From", fromAddr.String())
	for _, f := range []struct{ name, val string }{{"To", to}, {"Cc", cc}, {"Reply-To", replyTo}} {
		if strings.TrimSpace(f.val) == "" {
			continue
		}
		list, err := safeAddressList(f.val)
		if err != nil {
			return nil, permanentError{fmt.Sprintf("invalid %s address list: %v", f.name, err)}
		}
		h.Set(f.name, list)
	}
	h.Set("Subject", mime.QEncoding.Encode("utf-8", subject))
	h.Set("Date", time.Now().Format(time.RFC1123Z))
	h.Set("Message-ID", msgID)
	h.Set("MIME-Version", "1.0")
	var attachments []string
	if a, _ := ec.str(i, "options.attachments", ""); a != "" {
		attachments = splitList(a)
	}
	var body, altBuf bytes.Buffer
	mw := multipart.NewWriter(&body)
	h.Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())
	alt := multipart.NewWriter(&altBuf)
	writePart := func(w *multipart.Writer, ct, content string) {
		p, _ := w.CreatePart(textproto.MIMEHeader{"Content-Type": {ct + "; charset=UTF-8"}, "Content-Transfer-Encoding": {"base64"}})
		_, _ = p.Write([]byte(wrap76(base64.StdEncoding.EncodeToString([]byte(content)))))
	}
	if format == "text" || format == "both" {
		writePart(alt, "text/plain", text)
	}
	if format == "html" || format == "both" {
		writePart(alt, "text/html", htmlBody)
	}
	_ = alt.Close()
	p, _ := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {"multipart/alternative; boundary=" + alt.Boundary()}})
	_, _ = p.Write(altBuf.Bytes())
	for _, prop := range attachments {
		data, mt, name, err := binaryOf(it, prop)
		if err != nil {
			return nil, err
		}
		ap, _ := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type":              {mt},
			"Content-Disposition":       {mime.FormatMediaType("attachment", map[string]string{"filename": name})},
			"Content-Transfer-Encoding": {"base64"},
		})
		_, _ = ap.Write([]byte(wrap76(base64.StdEncoding.EncodeToString(data))))
	}
	_ = mw.Close()
	for k, vs := range h {
		for _, v := range vs {
			fmt.Fprintf(&buf, "%s: %s\r\n", k, v)
		}
	}
	buf.WriteString("\r\n")
	buf.Write(body.Bytes())

	host := credString(c, "host")
	port := int(toFloatDefault(c["port"], 465))
	secure := toBool(c["secure"])
	if _, ok := c["secure"]; !ok {
		secure = port == 465
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dial := node.NewOutboundDialer(ec.opts.Outbound)
	conn, err := dial(ec.ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	tlsCfg := &tls.Config{ServerName: host}
	if secure {
		conn = tls.Client(conn, tlsCfg)
	}
	cl, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	defer func() { _ = cl.Close() }()
	if name := credString(c, "hostName"); name != "" {
		_ = cl.Hello(name)
	}
	if !secure && !toBool(c["disableStartTls"]) {
		if ok, _ := cl.Extension("STARTTLS"); ok {
			if err := cl.StartTLS(tlsCfg); err != nil {
				return nil, err
			}
		}
	}
	if user := credString(c, "user"); user != "" {
		if ok, _ := cl.Extension("AUTH"); ok {
			if err := cl.Auth(plainAuth{user: user, pass: credString(c, "password"), host: host}); err != nil {
				return nil, permanentError{"SMTP authentication failed: " + err.Error()}
			}
		}
	}
	if err := cl.Mail(fromAddr.Address); err != nil {
		return nil, err
	}
	var accepted, rejected []interface{}
	for _, r := range rcpts {
		if err := cl.Rcpt(r); err != nil {
			rejected = append(rejected, r)
		} else {
			accepted = append(accepted, r)
		}
	}
	if len(accepted) == 0 {
		return nil, permanentError{"all recipients were rejected"}
	}
	w, err := cl.Data()
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	_ = cl.Quit()
	if rejected == nil {
		rejected = []interface{}{}
	}
	return map[string]interface{}{"accepted": accepted, "rejected": rejected, "messageId": msgID, "response": "250 Message accepted"}, nil
}

// plainAuth is smtp.PlainAuth without its refusal to authenticate over
// unencrypted connections to non-localhost (the server decides; credentials
// are only sent when TLS is active or the host is explicitly allowed).
type plainAuth struct{ user, pass, host string }

func (a plainAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	if !server.TLS && !isLocal(a.host) {
		return "", nil, fmt.Errorf("refusing to send credentials over an unencrypted connection")
	}
	return "PLAIN", []byte("\x00" + a.user + "\x00" + a.pass), nil
}

func (a plainAuth) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return nil, fmt.Errorf("unexpected server challenge")
	}
	return nil, nil
}

func isLocal(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
