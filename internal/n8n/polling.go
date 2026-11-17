package n8n

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/dgmos/flow/internal/engine"
)

// PollStateStore keeps the cursor of polling triggers (last seen message,
// row snapshot, …) between scheduled runs. Use a durable implementation in
// production; the default is process-local memory.
type PollStateStore interface {
	GetPollState(ctx context.Context, key string) (string, bool, error)
	SetPollState(ctx context.Context, key, value string) error
}

type memPollState struct {
	mu sync.Mutex
	m  map[string]string
}

var defaultPollState = &memPollState{m: map[string]string{}}

func (s *memPollState) GetPollState(_ context.Context, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	return v, ok, nil
}

func (s *memPollState) SetPollState(_ context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value
	return nil
}

func (ec *execCtx) pollStore() PollStateStore {
	if ec.opts != nil && ec.opts.PollState != nil {
		return ec.opts.PollState
	}
	return defaultPollState
}

func (ec *execCtx) pollKey() string {
	return engine.NamespaceFromContext(ec.ctx) + "/" + ec.data.wf.ID + "/" + ec.node.ID
}

// pollTimesCron converts n8n pollTimes to cron expressions.
func pollTimesCron(p map[string]interface{}) []string {
	pt, _ := p["pollTimes"].(map[string]interface{})
	var out []string
	for _, raw := range asList(pt["item"]) {
		it, _ := raw.(map[string]interface{})
		h, m := int(toFloatDefault(it["hour"], 14)), int(toFloatDefault(it["minute"], 0))
		switch toString(it["mode"]) {
		case "everyHour":
			out = append(out, fmt.Sprintf("0 %d * * * *", m))
		case "everyDay":
			out = append(out, fmt.Sprintf("0 %d %d * * *", m, h))
		case "everyWeek":
			out = append(out, fmt.Sprintf("0 %d %d * * %d", m, h, int(toFloatDefault(it["weekday"], 1))))
		case "everyMonth":
			out = append(out, fmt.Sprintf("0 %d %d %d * *", m, h, int(toFloatDefault(it["dayOfMonth"], 1))))
		case "everyX":
			v := int(toFloatDefault(it["value"], 2))
			switch toString(it["unit"]) {
			case "hours":
				out = append(out, fmt.Sprintf("0 0 */%d * * *", v))
			default:
				out = append(out, fmt.Sprintf("0 */%d * * * *", v))
			}
		case "custom":
			out = append(out, normCron(toString(it["cronExpression"])))
		default: // everyMinute
			out = append(out, "0 * * * * *")
		}
	}
	if len(out) == 0 {
		out = []string{"0 * * * * *"}
	}
	return out
}

// gmailTriggerNode returns messages received since the previous poll. The
// first poll only records the starting point.
func gmailTriggerNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("gmail", "gmailOAuth2")
	if err != nil {
		return nil, err
	}
	g := &gmailAPI{ec: ec, base: ec.apiBase("gmail", "https://gmail.googleapis.com") + "/gmail/v1/users/me", headers: h}
	store, key := ec.pollStore(), ec.pollKey()
	last, seen, err := store.GetPollState(ec.ctx, key)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if !seen {
		return [][]Item{nil}, store.SetPollState(ec.ctx, key, strconv.FormatInt(now.Unix(), 10))
	}
	q := g.query(0)
	extra := "after:" + last
	if cur := q.Get("q"); cur != "" {
		extra = cur + " " + extra
	}
	q.Set("q", extra)
	res, err := g.call("GET", "/messages?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var out []Item
	for _, m := range asList(res["messages"]) {
		mm, _ := m.(map[string]interface{})
		it, err := g.message(0, toString(mm["id"]))
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return [][]Item{out}, store.SetPollState(ec.ctx, key, strconv.FormatInt(now.Unix(), 10))
}

// googleSheetsTriggerNode emits added and/or updated rows since last poll.
func googleSheetsTriggerNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("sheets", "googleSheetsTriggerOAuth2Api")
	if err != nil {
		return nil, err
	}
	api := &sheetsAPI{ec: ec, base: ec.apiBase("sheets", "https://sheets.googleapis.com"), headers: h, titles: map[string]map[string]string{}}
	doc, title, _, err := api.target(0)
	if err != nil {
		return nil, err
	}
	rows, err := api.values(doc, title, 0)
	if err != nil {
		return nil, err
	}
	header := headerOf(rows)
	hashes := map[string]string{}
	for r := 1; r < len(rows); r++ {
		sum := sha1.Sum([]byte(mustJSON(rows[r])))
		hashes[strconv.Itoa(r+1)] = hex.EncodeToString(sum[:])
	}
	store, key := ec.pollStore(), ec.pollKey()
	prevRaw, seen, err := store.GetPollState(ec.ctx, key)
	if err != nil {
		return nil, err
	}
	state, _ := json.Marshal(hashes)
	if !seen {
		return [][]Item{nil}, store.SetPollState(ec.ctx, key, string(state))
	}
	var prev map[string]string
	_ = json.Unmarshal([]byte(prevRaw), &prev)
	event, _ := ec.str(0, "event", "rowAdded")
	var nums []int
	for k, h := range hashes {
		old, existed := prev[k]
		switch {
		case !existed && (event == "rowAdded" || event == "anyUpdate"):
		case existed && old != h && (event == "rowUpdate" || event == "rowUpdated" || event == "anyUpdate"):
		default:
			continue
		}
		n, _ := strconv.Atoi(k)
		nums = append(nums, n)
	}
	sort.Ints(nums)
	var out []Item
	for _, n := range nums {
		out = append(out, Item{JSON: rowObject(header, rows[n-1], n)})
	}
	return [][]Item{out}, store.SetPollState(ec.ctx, key, string(state))
}

// googleDriveTriggerNode emits files created/updated since the last poll.
func googleDriveTriggerNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("drive", "googleDriveOAuth2Api")
	if err != nil {
		return nil, err
	}
	store, key := ec.pollStore(), ec.pollKey()
	last, seen, err := store.GetPollState(ec.ctx, key)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if !seen {
		return [][]Item{nil}, store.SetPollState(ec.ctx, key, now)
	}
	event, _ := ec.str(0, "event", "fileCreated")
	field := "createdTime"
	if event == "fileUpdated" || event == "folderUpdated" || event == "watchFolderUpdated" {
		field = "modifiedTime"
	}
	q := []string{fmt.Sprintf("%s > '%s'", field, last), "trashed = false"}
	switch on, _ := ec.str(0, "triggerOn", "specificFolder"); on {
	case "specificFolder":
		f, _ := ec.rlString(0, "folderToWatch")
		q = append(q, fmt.Sprintf("'%s' in parents", f))
	case "specificFile":
		f, _ := ec.rlString(0, "fileToWatch")
		res, _, err := ec.apiCall("GET", ec.apiBase("drive", "https://www.googleapis.com")+"/drive/v3/files/"+url.PathEscape(f)+"?fields=id,name,mimeType,modifiedTime&supportsAllDrives=true", h, nil, "")
		if err != nil {
			return nil, err
		}
		var out []Item
		if toString(res["modifiedTime"]) > last {
			out = append(out, Item{JSON: res})
		}
		return [][]Item{out}, store.SetPollState(ec.ctx, key, now)
	}
	if event == "folderCreated" || event == "folderUpdated" {
		q = append(q, "mimeType = 'application/vnd.google-apps.folder'")
	} else {
		q = append(q, "mimeType != 'application/vnd.google-apps.folder'")
	}
	v := url.Values{"q": {joinAnd(q)}, "fields": {"files(id,name,mimeType,webViewLink,createdTime,modifiedTime,parents)"}, "supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"}}
	res, _, err := ec.apiCall("GET", ec.apiBase("drive", "https://www.googleapis.com")+"/drive/v3/files?"+v.Encode(), h, nil, "")
	if err != nil {
		return nil, err
	}
	var out []Item
	for _, f := range asList(res["files"]) {
		m, _ := f.(map[string]interface{})
		out = append(out, Item{JSON: m})
	}
	return [][]Item{out}, store.SetPollState(ec.ctx, key, now)
}

func joinAnd(q []string) string {
	s := ""
	for i, x := range q {
		if i > 0 {
			s += " and "
		}
		s += x
	}
	return s
}
