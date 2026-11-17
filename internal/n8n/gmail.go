package n8n

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type gmailAPI struct {
	ec      *execCtx
	base    string
	headers map[string]string
	labels  map[string]string
	me      string
	// approval turns the message into a send-and-wait approval request.
	approval *approval
}

func gmailNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("gmail", "gmailOAuth2")
	if err != nil {
		return nil, err
	}
	g := &gmailAPI{ec: ec, base: ec.apiBase("gmail", "https://gmail.googleapis.com") + "/gmail/v1/users/me", headers: h}
	var out []Item
	for i, it := range ec.items() {
		resource, _ := ec.str(i, "resource", "message")
		op, _ := ec.str(i, "operation", map[string]string{"message": "send", "draft": "create", "thread": "getAll", "label": "getAll"}[resource])
		res, err := g.do(i, it, resource, op)
		if err != nil {
			return nil, fmt.Errorf("gmail %s/%s: %w", resource, op, err)
		}
		for _, r := range res {
			r.PairedItem = paired(i)
			out = append(out, r)
		}
	}
	return [][]Item{out}, nil
}

func (g *gmailAPI) call(method, path string, body interface{}) (map[string]interface{}, error) {
	out, _, err := g.ec.apiCall(method, g.base+path, g.headers, body, "")
	return out, err
}

func one(m map[string]interface{}) []Item { return []Item{{JSON: m}} }

func (g *gmailAPI) do(i int, it Item, resource, op string) ([]Item, error) {
	ec := g.ec
	switch resource + "." + op {
	case "message.sendAndWait":
		a, err := ec.sendAndWaitApproval(i)
		if err != nil {
			return nil, err
		}
		g.approval = a
		raw, err := g.compose(i, it, nil)
		if err != nil {
			return nil, err
		}
		if _, err := g.call("POST", "/messages/send", map[string]interface{}{"raw": raw}); err != nil {
			return nil, err
		}
		return nil, errWaitSignal
	case "message.send":
		raw, err := g.compose(i, it, nil)
		if err != nil {
			return nil, err
		}
		res, err := g.call("POST", "/messages/send", map[string]interface{}{"raw": raw})
		return one(res), err
	case "message.reply", "thread.reply":
		mid, _ := ec.str(i, "messageId", "")
		orig, err := g.call("GET", "/messages/"+url.PathEscape(mid)+"?format=metadata", nil)
		if err != nil {
			return nil, err
		}
		raw, err := g.compose(i, it, orig)
		if err != nil {
			return nil, err
		}
		res, err := g.call("POST", "/messages/send", map[string]interface{}{"raw": raw, "threadId": orig["threadId"]})
		return one(res), err
	case "message.get":
		mid, _ := ec.str(i, "messageId", "")
		m, err := g.message(i, mid)
		return []Item{m}, err
	case "message.getAll":
		return g.list(i)
	case "message.addLabels", "message.removeLabels", "thread.addLabels", "thread.removeLabels":
		kind, idKey := "messages", "messageId"
		if resource == "thread" {
			kind, idKey = "threads", "threadId"
		}
		id, _ := ec.str(i, idKey, "")
		lv, _ := ec.param(i, "labelIds", nil)
		var labels []string
		for _, l := range asList(lv) {
			labels = append(labels, toString(l))
		}
		if s, ok := lv.(string); ok {
			labels = splitList(s)
		}
		body := map[string]interface{}{"addLabelIds": labels}
		if strings.HasPrefix(op, "remove") {
			body = map[string]interface{}{"removeLabelIds": labels}
		}
		res, err := g.call("POST", "/"+kind+"/"+url.PathEscape(id)+"/modify", body)
		return one(res), err
	case "message.markAsRead", "message.markAsUnread":
		id, _ := ec.str(i, "messageId", "")
		body := map[string]interface{}{"removeLabelIds": []string{"UNREAD"}}
		if op == "markAsUnread" {
			body = map[string]interface{}{"addLabelIds": []string{"UNREAD"}}
		}
		res, err := g.call("POST", "/messages/"+url.PathEscape(id)+"/modify", body)
		return one(res), err
	case "message.delete", "thread.delete", "label.delete":
		kind := map[string]string{"message": "messages", "thread": "threads", "label": "labels"}[resource]
		id, _ := ec.str(i, resource+"Id", "")
		if _, err := g.call("DELETE", "/"+kind+"/"+url.PathEscape(id), nil); err != nil {
			return nil, err
		}
		return one(map[string]interface{}{"success": true}), nil
	case "draft.create":
		raw, err := g.compose(i, it, nil)
		if err != nil {
			return nil, err
		}
		msg := map[string]interface{}{"raw": raw}
		if t, _ := ec.str(i, "options.threadId", ""); t != "" {
			msg["threadId"] = t
		}
		res, err := g.call("POST", "/drafts", map[string]interface{}{"message": msg})
		return one(res), err
	case "thread.get":
		id, _ := ec.str(i, "threadId", "")
		res, err := g.call("GET", "/threads/"+url.PathEscape(id), nil)
		return one(res), err
	case "thread.getAll":
		q := g.query(i)
		res, err := g.call("GET", "/threads?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		var out []Item
		for _, t := range asList(res["threads"]) {
			m, _ := t.(map[string]interface{})
			out = append(out, Item{JSON: m})
		}
		return out, nil
	case "label.getAll":
		res, err := g.call("GET", "/labels", nil)
		if err != nil {
			return nil, err
		}
		var out []Item
		for _, l := range asList(res["labels"]) {
			m, _ := l.(map[string]interface{})
			out = append(out, Item{JSON: m})
		}
		return out, nil
	case "label.get":
		id, _ := ec.str(i, "labelId", "")
		res, err := g.call("GET", "/labels/"+url.PathEscape(id), nil)
		return one(res), err
	case "label.create":
		name, _ := ec.str(i, "name", "")
		body := map[string]interface{}{"name": name}
		if v, _ := ec.str(i, "options.labelListVisibility", ""); v != "" {
			body["labelListVisibility"] = v
		}
		if v, _ := ec.str(i, "options.messageListVisibility", ""); v != "" {
			body["messageListVisibility"] = v
		}
		res, err := g.call("POST", "/labels", body)
		return one(res), err
	}
	return nil, permanentError{fmt.Sprintf("operation %s/%s is not supported", resource, op)}
}

func (g *gmailAPI) query(i int) url.Values {
	ec := g.ec
	q := url.Values{}
	var parts []string
	if s, _ := ec.str(i, "filters.q", ""); s != "" {
		parts = append(parts, s)
	}
	if s, _ := ec.str(i, "filters.sender", ""); s != "" {
		parts = append(parts, "from:"+s)
	}
	switch rs, _ := ec.str(i, "filters.readStatus", "both"); rs {
	case "unread":
		parts = append(parts, "is:unread")
	case "read":
		parts = append(parts, "is:read")
	}
	for key, op := range map[string]string{"filters.receivedAfter": "after:", "filters.receivedBefore": "before:"} {
		if s, _ := ec.str(i, key, ""); s != "" {
			if t, ok := timeOf(s); ok {
				parts = append(parts, op+strconv.FormatInt(t.Unix(), 10))
			}
		}
	}
	if len(parts) > 0 {
		q.Set("q", strings.Join(parts, " "))
	}
	if lv, _ := ec.param(i, "filters.labelIds", nil); lv != nil {
		for _, l := range asList(lv) {
			q.Add("labelIds", toString(l))
		}
	}
	if inc, _ := ec.boolean(i, "filters.includeSpamTrash", false); inc {
		q.Set("includeSpamTrash", "true")
	}
	return q
}

func (g *gmailAPI) list(i int) ([]Item, error) {
	ec := g.ec
	returnAll, _ := ec.boolean(i, "returnAll", false)
	limit, _ := ec.num(i, "limit", 50)
	q := g.query(i)
	var ids []string
	token := ""
	for {
		page := url.Values{}
		for k, v := range q {
			page[k] = v
		}
		page.Set("maxResults", "100")
		if token != "" {
			page.Set("pageToken", token)
		}
		res, err := g.call("GET", "/messages?"+page.Encode(), nil)
		if err != nil {
			return nil, err
		}
		for _, m := range asList(res["messages"]) {
			mm, _ := m.(map[string]interface{})
			ids = append(ids, toString(mm["id"]))
		}
		token = toString(res["nextPageToken"])
		if token == "" || !returnAll && len(ids) >= int(limit) {
			break
		}
	}
	if !returnAll && len(ids) > int(limit) {
		ids = ids[:int(limit)]
	}
	out := make([]Item, 0, len(ids))
	for _, id := range ids {
		m, err := g.message(i, id)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// message fetches one message in n8n's simple or parsed format.
func (g *gmailAPI) message(i int, id string) (Item, error) {
	simple, _ := g.ec.boolean(i, "simple", true)
	if simple {
		m, err := g.call("GET", "/messages/"+url.PathEscape(id)+"?format=metadata", nil)
		if err != nil {
			return Item{}, err
		}
		out := map[string]interface{}{}
		for _, k := range []string{"id", "threadId", "snippet", "sizeEstimate", "historyId", "internalDate", "labelIds"} {
			if v, ok := m[k]; ok {
				out[k] = v
			}
		}
		var labels []interface{}
		names := g.labelNames()
		for _, l := range asList(m["labelIds"]) {
			labels = append(labels, map[string]interface{}{"id": l, "name": firstNonEmpty(names[toString(l)], toString(l))})
		}
		out["labels"] = labels
		payload, _ := m["payload"].(map[string]interface{})
		out["payload"] = map[string]interface{}{"mimeType": payload["mimeType"]}
		for _, h := range asList(payload["headers"]) {
			hm, _ := h.(map[string]interface{})
			out[toString(hm["name"])] = hm["value"]
		}
		return Item{JSON: out}, nil
	}
	m, err := g.call("GET", "/messages/"+url.PathEscape(id)+"?format=raw", nil)
	if err != nil {
		return Item{}, err
	}
	raw, err := base64.URLEncoding.DecodeString(padB64(toString(m["raw"])))
	if err != nil {
		return Item{}, err
	}
	download, _ := g.ec.boolean(i, "options.downloadAttachments", false)
	prefix, _ := g.ec.str(i, "options.dataPropertyAttachmentsPrefixName", "attachment_")
	j, bin := parseEmail(raw, download, prefix)
	for _, k := range []string{"id", "threadId", "labelIds", "sizeEstimate"} {
		j[k] = m[k]
	}
	return Item{JSON: j, Binary: bin}, nil
}

func padB64(s string) string {
	if m := len(s) % 4; m != 0 {
		s += strings.Repeat("=", 4-m)
	}
	return s
}

func (g *gmailAPI) labelNames() map[string]string {
	if g.labels != nil {
		return g.labels
	}
	g.labels = map[string]string{}
	if res, err := g.call("GET", "/labels", nil); err == nil {
		for _, l := range asList(res["labels"]) {
			m, _ := l.(map[string]interface{})
			g.labels[toString(m["id"])] = toString(m["name"])
		}
	}
	return g.labels
}

func addrList(s string) map[string]interface{} {
	list, _ := mail.ParseAddressList(s)
	var vals []interface{}
	var texts []string
	for _, a := range list {
		vals = append(vals, map[string]interface{}{"address": a.Address, "name": a.Name})
		texts = append(texts, a.String())
	}
	if len(list) == 0 && s != "" {
		texts = []string{s}
	}
	return map[string]interface{}{"value": vals, "text": strings.Join(texts, ", "), "html": strings.Join(texts, ", ")}
}

// parseEmail converts a raw RFC 822 message into n8n's parsed email shape.
func parseEmail(raw []byte, download bool, prefix string) (map[string]interface{}, map[string]interface{}) {
	msg, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return map[string]interface{}{"text": string(raw)}, nil
	}
	dec := new(mime.WordDecoder)
	hdr := func(k string) string {
		v := msg.Header.Get(k)
		if d, err := dec.DecodeHeader(v); err == nil {
			return d
		}
		return v
	}
	headers := map[string]interface{}{}
	for k := range msg.Header {
		headers[strings.ToLower(k)] = k + ": " + hdr(k)
	}
	out := map[string]interface{}{
		"headers": headers, "subject": hdr("Subject"), "messageId": hdr("Message-ID"),
		"from": addrList(hdr("From")), "to": addrList(hdr("To")),
	}
	if c := hdr("Cc"); c != "" {
		out["cc"] = addrList(c)
	}
	if r := hdr("Reply-To"); r != "" {
		out["replyTo"] = addrList(r)
	}
	if d, err := msg.Header.Date(); err == nil {
		out["date"] = d.UTC().Format(time.RFC3339Nano)
	}
	bin := map[string]interface{}{}
	var text, html string
	var walk func(ct string, h textproto.MIMEHeader, body io.Reader)
	walk = func(ct string, h textproto.MIMEHeader, body io.Reader) {
		mt, params, _ := mime.ParseMediaType(ct)
		if strings.HasPrefix(mt, "multipart/") {
			mr := multipart.NewReader(body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err != nil {
					return
				}
				walk(p.Header.Get("Content-Type"), p.Header, p)
			}
		}
		data, _ := io.ReadAll(decodeTransfer(h.Get("Content-Transfer-Encoding"), body))
		disp, dparams, _ := mime.ParseMediaType(h.Get("Content-Disposition"))
		switch {
		case disp == "attachment" || dparams["filename"] != "":
			if download {
				name := dparams["filename"]
				bin[fmt.Sprintf("%s%d", prefix, len(bin))] = binaryData(data, mt, name)
			}
		case mt == "text/html" && html == "":
			html = string(data)
		case (mt == "text/plain" || mt == "") && text == "":
			text = string(data)
		}
	}
	walk(msg.Header.Get("Content-Type"), textproto.MIMEHeader(msg.Header), msg.Body)
	out["text"] = text
	out["html"] = html
	out["textAsHtml"] = "<p>" + strings.ReplaceAll(text, "\n", "<br/>") + "</p>"
	if len(bin) == 0 {
		bin = nil
	}
	return out, bin
}

func decodeTransfer(enc string, r io.Reader) io.Reader {
	switch strings.ToLower(strings.TrimSpace(enc)) {
	case "base64":
		return base64.NewDecoder(base64.StdEncoding, &stripNewlines{r: r})
	case "quoted-printable":
		return quotedprintable.NewReader(r)
	}
	return r
}

type stripNewlines struct{ r io.Reader }

func (s *stripNewlines) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	j := 0
	for i := 0; i < n; i++ {
		if p[i] != '\r' && p[i] != '\n' {
			p[j] = p[i]
			j++
		}
	}
	return j, err
}

// compose builds a base64url-encoded MIME message for send/reply/draft.
func (g *gmailAPI) compose(i int, it Item, replyTo map[string]interface{}) (string, error) {
	ec := g.ec
	body, err := ec.str(i, "message", "")
	if err != nil {
		return "", err
	}
	emailType, _ := ec.str(i, "emailType", "html")
	if g.approval != nil {
		body, emailType = g.approval.html(body), "html"
	}
	if ec.version < 2.1 && !ec.hasParam("emailType") {
		emailType = "text"
	}
	to, _ := ec.str(i, "sendTo", "")
	subject, _ := ec.str(i, "subject", "")
	cc, _ := ec.str(i, "options.ccList", "")
	bcc, _ := ec.str(i, "options.bccList", "")
	replyAddr, _ := ec.str(i, "options.replyTo", "")
	sender, _ := ec.str(i, "options.senderName", "")
	h := textproto.MIMEHeader{}
	if replyTo != nil {
		payload, _ := replyTo["payload"].(map[string]interface{})
		orig := map[string]string{}
		for _, x := range asList(payload["headers"]) {
			hm, _ := x.(map[string]interface{})
			orig[strings.ToLower(toString(hm["name"]))] = toString(hm["value"])
		}
		subject = orig["subject"]
		if !strings.HasPrefix(strings.ToLower(subject), "re:") {
			subject = "Re: " + subject
		}
		to = orig["reply-to"]
		if to == "" {
			to = orig["from"]
		}
		if only, _ := ec.boolean(i, "options.replyToSenderOnly", false); !only && orig["cc"] != "" && cc == "" {
			cc = orig["cc"]
		}
		if mid := orig["message-id"]; mid != "" {
			h.Set("In-Reply-To", stripCRLF(mid))
			h.Set("References", stripCRLF(strings.TrimSpace(orig["references"]+" "+mid)))
		}
	}
	if sender != "" {
		if g.me == "" {
			if p, err := g.call("GET", "/profile", nil); err == nil {
				g.me = toString(p["emailAddress"])
			}
		}
		h.Set("From", (&mail.Address{Name: sender, Address: g.me}).String())
	}
	for _, f := range []struct{ name, val string }{{"To", to}, {"Cc", cc}, {"Bcc", bcc}, {"Reply-To", replyAddr}} {
		if strings.TrimSpace(f.val) == "" {
			continue
		}
		list, err := safeAddressList(f.val)
		if err != nil {
			return "", permanentError{fmt.Sprintf("invalid %s address list: %v", f.name, err)}
		}
		h.Set(f.name, list)
	}
	h.Set("Subject", mime.QEncoding.Encode("utf-8", subject))
	h.Set("MIME-Version", "1.0")
	ctype := "text/plain; charset=UTF-8"
	if emailType == "html" {
		ctype = "text/html; charset=UTF-8"
	}

	var attachments [][3]string // prop
	if av, _ := ec.param(i, "options.attachmentsUi.attachmentsBinary", nil); av != nil {
		for _, raw := range asList(av) {
			m, _ := raw.(map[string]interface{})
			for _, p := range splitList(toString(m["property"])) {
				attachments = append(attachments, [3]string{p})
			}
		}
	}
	var buf bytes.Buffer
	writeHeaders := func(hh textproto.MIMEHeader) {
		for k, vs := range hh {
			for _, v := range vs {
				fmt.Fprintf(&buf, "%s: %s\r\n", k, v)
			}
		}
	}
	if len(attachments) == 0 {
		h.Set("Content-Type", ctype)
		h.Set("Content-Transfer-Encoding", "base64")
		writeHeaders(h)
		buf.WriteString("\r\n")
		buf.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(body))))
	} else {
		var mbuf bytes.Buffer
		mw := multipart.NewWriter(&mbuf)
		h.Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())
		writeHeaders(h)
		buf.WriteString("\r\n")
		part, _ := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {ctype}, "Content-Transfer-Encoding": {"base64"}})
		_, _ = part.Write([]byte(wrap76(base64.StdEncoding.EncodeToString([]byte(body)))))
		for _, a := range attachments {
			data, mt, name, err := binaryOf(it, a[0])
			if err != nil {
				return "", err
			}
			p, _ := mw.CreatePart(textproto.MIMEHeader{
				"Content-Type":              {mt},
				"Content-Disposition":       {mime.FormatMediaType("attachment", map[string]string{"filename": name})},
				"Content-Transfer-Encoding": {"base64"},
			})
			_, _ = p.Write([]byte(wrap76(base64.StdEncoding.EncodeToString(data))))
		}
		_ = mw.Close()
		buf.Write(mbuf.Bytes())
	}
	return base64.URLEncoding.EncodeToString(buf.Bytes()), nil
}

func wrap76(s string) string {
	var b strings.Builder
	for len(s) > 76 {
		b.WriteString(s[:76])
		b.WriteString("\r\n")
		s = s[76:]
	}
	b.WriteString(s)
	return b.String()
}

// safeAddressList re-serializes an address list so header values can never
// carry CR/LF (header injection) or garbage.
func safeAddressList(s string) (string, error) {
	if strings.ContainsAny(s, "\r\n") {
		return "", fmt.Errorf("line breaks are not allowed")
	}
	list, err := mail.ParseAddressList(s)
	if err != nil {
		return "", err
	}
	parts := make([]string, len(list))
	for i, a := range list {
		if a.Name == "" {
			parts[i] = a.Address
		} else {
			parts[i] = a.String()
		}
	}
	return strings.Join(parts, ", "), nil
}

func stripCRLF(s string) string { return strings.NewReplacer("\r", "", "\n", "").Replace(s) }
