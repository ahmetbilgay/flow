package n8n

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"

	"github.com/dgmos/flow/internal/node"
)

// imapPollCron is how often the Email Trigger (IMAP) checks for new mail;
// n8n keeps an IDLE connection instead.
const imapPollCron = "0 * * * * *"

// imapMaxMessages bounds the messages emitted by one poll.
const imapMaxMessages = 200

type imapCursor struct {
	UIDValidity uint32 `json:"uidValidity"`
	LastUID     uint32 `json:"lastUid"`
}

// emailReadImapNode is the Email Trigger (IMAP) as a polling trigger: each
// poll emits the messages matching the search criteria (default UNSEEN)
// that arrived since the previous poll. The first poll only records the
// mailbox position.
func emailReadImapNode(ec *execCtx) ([][]Item, error) {
	c, err := ec.credential("imap")
	if err != nil {
		return nil, err
	}
	criteria, err := imapCriteria(ec)
	if err != nil {
		return nil, err
	}
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	var cur imapCursor
	if pc.seen {
		_ = json.Unmarshal([]byte(pc.prev), &cur)
	}

	cl, err := imapConnect(ec, c)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cl.Logout() }()
	mailbox, _ := ec.str(0, "mailbox", "INBOX")
	status, err := cl.Select(mailbox, false)
	if err != nil {
		return nil, fmt.Errorf("imap: select %q: %w", mailbox, err)
	}
	if !pc.seen || cur.UIDValidity != status.UidValidity {
		// First poll, or the mailbox was recreated: start from here.
		start := imapCursor{UIDValidity: status.UidValidity}
		if status.UidNext > 0 {
			start.LastUID = status.UidNext - 1
		} else if all, err := cl.UidSearch(imap.NewSearchCriteria()); err == nil {
			for _, u := range all {
				if u > start.LastUID {
					start.LastUID = u
				}
			}
		}
		b, _ := json.Marshal(start)
		return pc.commit(string(b), nil)
	}
	next := cur

	criteria.Uid = new(imap.SeqSet)
	criteria.Uid.AddRange(cur.LastUID+1, 0)
	uids, err := cl.UidSearch(criteria)
	if err != nil {
		return nil, fmt.Errorf("imap: search: %w", err)
	}
	var fresh []uint32
	for _, u := range uids {
		if u > cur.LastUID { // "n:*" always matches the last message
			fresh = append(fresh, u)
		}
	}
	if len(fresh) > imapMaxMessages {
		fresh = fresh[:imapMaxMessages]
	}
	var out []Item
	if len(fresh) > 0 {
		set := new(imap.SeqSet)
		set.AddNum(fresh...)
		section := &imap.BodySectionName{Peek: true}
		ch := make(chan *imap.Message, len(fresh))
		if err := cl.UidFetch(set, []imap.FetchItem{imap.FetchUid, section.FetchItem()}, ch); err != nil {
			return nil, fmt.Errorf("imap: fetch: %w", err)
		}
		download, _ := ec.boolean(0, "downloadAttachments", false)
		prefix, _ := ec.str(0, "dataPropertyAttachmentsPrefixName", "attachment_")
		format, _ := ec.str(0, "format", "simple")
		for m := range ch {
			r := m.GetBody(section)
			if r == nil {
				continue
			}
			raw, err := io.ReadAll(io.LimitReader(r, 50<<20))
			if err != nil {
				return nil, err
			}
			out = append(out, imapItem(raw, m.Uid, format, download, prefix))
			if m.Uid > next.LastUID {
				next.LastUID = m.Uid
			}
		}
		if action, _ := ec.str(0, "postProcessAction", "read"); action == "read" {
			if err := cl.UidStore(set, imap.FormatFlagsOp(imap.AddFlags, true), []interface{}{imap.SeenFlag}, nil); err != nil {
				return nil, fmt.Errorf("imap: mark as read: %w", err)
			}
		}
	}
	b, _ := json.Marshal(next)
	return pc.commit(string(b), out)
}

func imapConnect(ec *execCtx, c map[string]interface{}) (*client.Client, error) {
	host := credString(c, "host")
	port := int(toFloatDefault(c["port"], 993))
	secure := true
	if v, ok := c["secure"]; ok {
		secure = toBool(v)
	}
	tlsCfg := &tls.Config{ServerName: host, InsecureSkipVerify: toBool(c["allowUnauthorizedCerts"])} //nolint:gosec // user opt-in, as in n8n
	conn, err := node.NewOutboundDialer(ec.opts.Outbound)(ec.ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))
	if secure {
		conn = tls.Client(conn, tlsCfg)
	}
	cl, err := client.New(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("imap: %w", sanitizeErr(err))
	}
	if !secure {
		if ok, _ := cl.SupportStartTLS(); ok {
			if err := cl.StartTLS(tlsCfg); err != nil {
				_ = cl.Logout()
				return nil, fmt.Errorf("imap: starttls: %w", err)
			}
		}
	}
	if err := cl.Login(credString(c, "user"), credString(c, "password")); err != nil {
		_ = cl.Logout()
		return nil, permanentError{"imap: login failed: " + err.Error()}
	}
	return cl, nil
}

// imapCriteria converts options.customEmailConfig (n8n's node-imap style,
// default ["UNSEEN"]) to search criteria.
func imapCriteria(ec *execCtx) (*imap.SearchCriteria, error) {
	raw, _ := ec.str(0, "options.customEmailConfig", `["UNSEEN"]`)
	var list []interface{}
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return nil, permanentError{"imap: custom email rules must be a JSON array, e.g. [\"UNSEEN\"]"}
	}
	sc := imap.NewSearchCriteria()
	for _, e := range list {
		key, arg := "", ""
		switch t := e.(type) {
		case string:
			key = t
		case []interface{}:
			if len(t) > 0 {
				key = toString(t[0])
			}
			if len(t) > 1 {
				arg = toString(t[1])
			}
		}
		switch strings.ToUpper(key) {
		case "ALL":
		case "UNSEEN":
			sc.WithoutFlags = append(sc.WithoutFlags, imap.SeenFlag)
		case "SEEN":
			sc.WithFlags = append(sc.WithFlags, imap.SeenFlag)
		case "FLAGGED":
			sc.WithFlags = append(sc.WithFlags, imap.FlaggedFlag)
		case "UNFLAGGED":
			sc.WithoutFlags = append(sc.WithoutFlags, imap.FlaggedFlag)
		case "ANSWERED":
			sc.WithFlags = append(sc.WithFlags, imap.AnsweredFlag)
		case "UNANSWERED":
			sc.WithoutFlags = append(sc.WithoutFlags, imap.AnsweredFlag)
		case "SINCE", "BEFORE":
			t, ok := timeOf(arg)
			if !ok {
				return nil, permanentError{fmt.Sprintf("imap: invalid date %q for %s", arg, key)}
			}
			if strings.EqualFold(key, "SINCE") {
				sc.Since = t
			} else {
				sc.Before = t
			}
		case "FROM", "TO", "CC", "SUBJECT":
			sc.Header.Add(key, arg)
		case "TEXT", "BODY":
			sc.Text = append(sc.Text, arg)
		default:
			return nil, permanentError{fmt.Sprintf("imap: search rule %q is not supported", key)}
		}
	}
	return sc, nil
}

// imapItem renders a message in n8n's simple, resolved or raw format.
func imapItem(raw []byte, uid uint32, format string, download bool, prefix string) Item {
	switch format {
	case "raw":
		return Item{JSON: map[string]interface{}{"raw": string(raw), "attributes": map[string]interface{}{"uid": float64(uid)}}}
	case "resolved":
		j, bin := parseEmail(raw, download, prefix)
		j["attributes"] = map[string]interface{}{"uid": float64(uid)}
		return Item{JSON: j, Binary: bin}
	}
	parsed, bin := parseEmail(raw, download, prefix)
	j := map[string]interface{}{}
	if hs, ok := parsed["headers"].(map[string]interface{}); ok {
		for k, v := range hs {
			// parseEmail renders "Name: value"; simple format keeps the value.
			s := toString(v)
			if i := strings.Index(s, ": "); i >= 0 {
				s = s[i+2:]
			}
			j[k] = s
		}
	}
	j["textPlain"], j["textHtml"] = parsed["text"], parsed["html"]
	j["metadata"] = map[string]interface{}{}
	j["attributes"] = map[string]interface{}{"uid": float64(uid)}
	return Item{JSON: j, Binary: bin}
}
