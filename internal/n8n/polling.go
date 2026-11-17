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

// PollStateSwapper is an optional extension of PollStateStore. When the
// store implements it, cursors are advanced with compare-and-swap so two
// overlapping polls (frequent schedules, several instances) never emit the
// same items twice: the poll that loses the swap emits nothing, and its
// window is picked up by the next poll. Without it, polls are only
// serialized within one process.
type PollStateSwapper interface {
	// CompareAndSwapPollState sets key to value only if its current value is
	// old (or, when oldExists is false, only if key is absent).
	CompareAndSwapPollState(ctx context.Context, key, old string, oldExists bool, value string) (bool, error)
}

func (s *memPollState) CompareAndSwapPollState(_ context.Context, key, old string, oldExists bool, value string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.m[key]
	if ok != oldExists || (ok && cur != old) {
		return false, nil
	}
	s.m[key] = value
	return true, nil
}

var pollLocks sync.Map // poll key -> *sync.Mutex

// pollCursor is one poll's view of its trigger's cursor. Polls of the same
// trigger are serialized in-process for the cursor's lifetime.
type pollCursor struct {
	ec     *execCtx
	store  PollStateStore
	key    string
	prev   string
	seen   bool
	unlock func()
}

func (ec *execCtx) openPoll() (*pollCursor, error) {
	key := ec.pollKey()
	m, _ := pollLocks.LoadOrStore(key, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	store := ec.pollStore()
	prev, seen, err := store.GetPollState(ec.ctx, key)
	if err != nil {
		mu.Unlock()
		return nil, err
	}
	return &pollCursor{ec: ec, store: store, key: key, prev: prev, seen: seen, unlock: mu.Unlock}, nil
}

func (p *pollCursor) close() { p.unlock() }

// commit advances the cursor to next and returns out as the trigger's
// output, or no items if a concurrent poll already advanced the cursor.
func (p *pollCursor) commit(next string, out []Item) ([][]Item, error) {
	if sw, ok := p.store.(PollStateSwapper); ok {
		won, err := sw.CompareAndSwapPollState(p.ec.ctx, p.key, p.prev, p.seen, next)
		if err != nil {
			return nil, err
		}
		if !won {
			return [][]Item{nil}, nil
		}
		return [][]Item{out}, nil
	}
	return [][]Item{out}, p.store.SetPollState(p.ec.ctx, p.key, next)
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
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	last, seen := pc.prev, pc.seen
	now := time.Now()
	if !seen {
		return pc.commit(strconv.FormatInt(now.Unix(), 10), nil)
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
	return pc.commit(strconv.FormatInt(now.Unix(), 10), out)
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
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	prevRaw, seen := pc.prev, pc.seen
	state, _ := json.Marshal(hashes)
	if !seen {
		return pc.commit(string(state), nil)
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
	return pc.commit(string(state), out)
}

// googleDriveTriggerNode emits files created/updated since the last poll.
func googleDriveTriggerNode(ec *execCtx) ([][]Item, error) {
	h, err := ec.googleAuth("drive", "googleDriveOAuth2Api")
	if err != nil {
		return nil, err
	}
	pc, err := ec.openPoll()
	if err != nil {
		return nil, err
	}
	defer pc.close()
	last, seen := pc.prev, pc.seen
	now := time.Now().UTC().Format(time.RFC3339)
	if !seen {
		return pc.commit(now, nil)
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
		return pc.commit(now, out)
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
	return pc.commit(now, out)
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
