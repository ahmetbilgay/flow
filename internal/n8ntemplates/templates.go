// Package n8ntemplates fetches public n8n templates and turns them into
// catalog entries checked with flow's importer: whether they run, the
// credentials and accounts they need, their triggers, the parameters a user
// has to fill in (setup fields) and the workflow itself. Paid templates are
// never returned.
//
// Templates are written by their authors on n8n.io; check n8n's terms and
// the authors' rights before offering them in a product.
package n8ntemplates

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgmos/flow"
)

const DefaultAPI = "https://api.n8n.io/api/templates"

// Template is one catalog entry.
type Template struct {
	Source          string       `json:"source"`
	SourceID        string       `json:"source_id"`
	SourceURL       string       `json:"source_url"`
	Name            string       `json:"name"`
	Description     string       `json:"description"`
	Categories      []string     `json:"categories"`
	Author          *Author      `json:"author,omitempty"`
	TotalViews      int          `json:"total_views"`
	CreatedAt       string       `json:"created_at,omitempty"`
	NodeTypes       []string     `json:"node_types"`
	Runnable        bool         `json:"runnable"`
	Nodes           int          `json:"nodes"`
	SupportedNodes  int          `json:"supported_nodes"`
	CredentialTypes []string     `json:"credential_types"`
	Credentials     []Credential `json:"credentials"`
	Triggers        []Trigger    `json:"triggers"`
	SetupFields     []SetupField `json:"setup_fields"`
	// NeedsAccount lists service nodes whose credential the template does
	// not name (authors' credentials are often stripped): the user still has
	// to connect an account for each.
	NeedsAccount []NodeRef              `json:"needs_account"`
	Warnings     []flow.N8NIssue        `json:"warnings,omitempty"`
	Blocking     []flow.N8NIssue        `json:"blocking,omitempty"`
	Workflow     map[string]interface{} `json:"workflow"`
	FetchedAt    string                 `json:"fetched_at"`
}

// Credential is a credential a node needs.
type Credential struct {
	Node string `json:"node"`
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
}

// NodeRef names a node.
type NodeRef struct {
	Node string `json:"node"`
	Type string `json:"type"`
}

// Author is the template's author on n8n.io.
type Author struct {
	Name     string `json:"name,omitempty"`
	Username string `json:"username,omitempty"`
}

// Trigger is how the workflow starts.
type Trigger struct {
	Node    string   `json:"node"`
	Kind    string   `json:"kind"`
	Path    string   `json:"path,omitempty"`
	Method  string   `json:"method,omitempty"`
	Cron    []string `json:"cron,omitempty"`
	Default bool     `json:"default,omitempty"`
}

// SetupField is a node parameter the user has to set before the workflow
// is useful: a resource of the template's author (a sheet, a channel) or a
// placeholder value.
type SetupField struct {
	Node      string      `json:"node"`
	NodeType  string      `json:"node_type"`
	Parameter string      `json:"parameter"` // dotted path in the node's parameters
	Value     interface{} `json:"value"`
	Reason    string      `json:"reason"` // "resource" | "placeholder" | "account-specific"
}

type Client struct {
	// API is the template API base URL (DefaultAPI).
	API string
	// HTTP is the client used; nil means a 30-second-timeout client.
	HTTP *http.Client
	// UserAgent is sent with every request.
	UserAgent string
}

// Stats counts what Export did.
type Stats struct{ Runnable, Blocked, Paid, Failed int }

// Export fetches and checks templates; the result keeps the input order.
// Paid templates are skipped; blocked ones are kept only with includeBlocked.
func Export(ctx context.Context, c *Client, ids []string, parallel int, includeBlocked bool, limit int) ([]Template, Stats) {
	type result struct {
		t    *Template
		paid bool
		err  error
	}
	results := make([]result, len(ids))
	sem := make(chan struct{}, max(1, parallel))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[i] = result{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()
			raw, err := c.Fetch(ctx, id)
			if err != nil {
				results[i] = result{err: err}
				return
			}
			t, paid, err := Build(id, raw, time.Now().UTC())
			results[i] = result{t: t, paid: paid, err: err}
		}(i, id)
	}
	wg.Wait()
	var out []Template
	var st Stats
	for i, r := range results {
		switch {
		case r.err != nil:
			st.Failed++
			log.Printf("template %s: %v", ids[i], r.err)
		case r.paid:
			st.Paid++
		case r.t.Runnable:
			st.Runnable++
			out = append(out, *r.t)
		default:
			st.Blocked++
			if includeBlocked {
				out = append(out, *r.t)
			}
		}
		if limit > 0 && len(out) >= limit {
			out = out[:limit]
			break
		}
	}
	if out == nil {
		out = []Template{}
	}
	return out, st
}

func (c *Client) get(ctx context.Context, u string, v interface{}) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		req.Header.Set("User-Agent", c.agent())
		req.Header.Set("Accept", "application/json")
		resp, err := c.httpClient().Do(req)
		if err == nil {
			body, rerr := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
			_ = resp.Body.Close()
			switch {
			case rerr != nil:
				err = rerr
			case resp.StatusCode == 404:
				return errors.New("not found")
			case resp.StatusCode >= 400:
				err = fmt.Errorf("status %d", resp.StatusCode)
			default:
				return json.Unmarshal(body, v)
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return last
}

// Search lists template ids, most viewed first, with at least minViews
// views, optionally in one category.
func (c *Client) Search(ctx context.Context, minViews int, category string) ([]string, error) {
	type row struct {
		ID         json.Number `json:"id"`
		TotalViews json.Number `json:"totalViews"`
	}
	var rows []row
	for page := 1; page < 1000; page++ {
		var res struct {
			TotalWorkflows int   `json:"totalWorkflows"`
			Workflows      []row `json:"workflows"`
		}
		u := fmt.Sprintf("%s/search?rows=100&page=%d", c.base(), page)
		if category != "" {
			u += "&category=" + urlQuery(category)
		}
		if err := c.get(ctx, u, &res); err != nil {
			return nil, fmt.Errorf("search page %d: %w", page, err)
		}
		rows = append(rows, res.Workflows...)
		if len(res.Workflows) == 0 || len(rows) >= res.TotalWorkflows {
			break
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, _ := rows[i].TotalViews.Int64()
		b, _ := rows[j].TotalViews.Int64()
		return a > b
	})
	var ids []string
	seen := map[string]bool{}
	for _, r := range rows {
		v, _ := r.TotalViews.Int64()
		if int(v) < minViews || seen[r.ID.String()] {
			continue
		}
		seen[r.ID.String()] = true
		ids = append(ids, r.ID.String())
	}
	return ids, nil
}

func urlQuery(s string) string {
	r := strings.NewReplacer(" ", "+", "&", "%26", "#", "%23", "?", "%3F", "=", "%3D")
	return r.Replace(s)
}

func (c *Client) Fetch(ctx context.Context, id string) (map[string]interface{}, error) {
	var res map[string]interface{}
	if err := c.get(ctx, c.base()+"/workflows/"+id, &res); err != nil {
		return nil, err
	}
	return res, nil
}

// Build turns a template API response into a catalog entry; paid reports
// a paid template (no entry).
func Build(id string, raw map[string]interface{}, now time.Time) (*Template, bool, error) {
	meta, _ := raw["workflow"].(map[string]interface{})
	if meta == nil {
		return nil, false, errors.New("unexpected response")
	}
	if p := num(meta["price"]); p > 0 {
		return nil, true, nil
	}
	wf, _ := meta["workflow"].(map[string]interface{})
	if wf == nil || wf["nodes"] == nil {
		return nil, false, errors.New("template has no workflow")
	}
	name := str(meta["name"])
	if _, ok := wf["name"]; !ok {
		wf["name"] = name
	}
	t := &Template{
		Source: "n8n", SourceID: id, SourceURL: "https://n8n.io/workflows/" + id,
		Name: name, Description: str(meta["description"]), TotalViews: int(num(meta["totalViews"])), CreatedAt: str(meta["createdAt"]),
		FetchedAt: now.Format(time.RFC3339), Categories: []string{},
	}
	if err := t.check(wf, "n8n-"+id); err != nil {
		return nil, false, err
	}
	for _, c := range asList(meta["categories"]) {
		if n := str(asMap(c)["name"]); n != "" {
			t.Categories = append(t.Categories, n)
		}
	}
	if u := asMap(meta["user"]); len(u) > 0 {
		t.Author = &Author{Name: str(u["name"]), Username: str(u["username"])}
	}
	return t, false, nil
}

// check runs flow's importer on wf and fills the template's workflow,
// report, accounts and setup fields.
func (t *Template) check(wf map[string]interface{}, id string) error {
	delete(wf, "pinData") // sample data captured by the author
	data, _ := json.Marshal(wf)
	_, rep, err := flow.ImportN8NWorkflow(data, flow.N8NImportOptions{ID: id})
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}
	t.Workflow = wf
	t.Runnable, t.Nodes, t.SupportedNodes = rep.Runnable, rep.Nodes, rep.Supported
	t.Credentials, t.Warnings, t.Blocking = []Credential{}, rep.Warnings, rep.Blocking
	types := map[string]bool{}
	for _, n := range asList(wf["nodes"]) {
		if ty := str(asMap(n)["type"]); ty != "" && ty != "n8n-nodes-base.stickyNote" {
			types[ty] = true
		}
	}
	t.NodeTypes = sortedKeys(types)
	ct := map[string]bool{}
	for _, c := range rep.Credentials {
		ct[c.Type] = true
		t.Credentials = append(t.Credentials, Credential{Node: c.Node, Type: c.Type, Name: c.Name})
	}
	t.CredentialTypes = sortedKeys(ct)
	for _, tr := range rep.Triggers {
		t.Triggers = append(t.Triggers, Trigger{Node: tr.Node, Kind: tr.Kind, Path: tr.Path, Method: tr.Method, Cron: tr.Cron, Default: tr.Default})
	}
	t.SetupFields = setupFields(wf)
	t.NeedsAccount = needsAccount(wf)
	if t.Triggers == nil {
		t.Triggers = []Trigger{}
	}
	return nil
}

// ---------------------------------------------------------------- setup fields

var (
	placeholderRe = regexp.MustCompile(`(?i)^\s*(your[_\- ]|<[^<>]{2,60}>$|\[[a-z0-9_ \-]{2,60}\]$|replace[_\- ]?(me|this|with)|xxx+|todo\b|insert[_\- ]your|enter[_\- ]your|change[_\- ]?me)`)
	// "{{ voice id }}"-style blanks that are not n8n expressions.
	blankBracesRe = regexp.MustCompile(`\{\{\s*[A-Za-z][A-Za-z _\-]{1,40}\s*\}\}`)
	exampleRe     = regexp.MustCompile(`(?i)(@example\.(com|org)|example\.(com|org)/|your-?(domain|company|webhook)|0{6,}|1234567890)`)
)

// codeParams hold code or queries, never a setup value; choiceParams are
// choices with a sensible template default (a model).
var (
	codeParams   = map[string]bool{"jsCode": true, "pythonCode": true, "code": true, "functionCode": true, "functionItemCode": true, "query": true, "sql": true, "html": true}
	choiceParams = map[string]bool{"model": true, "modelId": true, "modelName": true}
)

// accountParams are literal parameters that name the author's own
// accounts or resources.
var accountParams = map[string]bool{
	"chatId": true, "channelId": true, "channel": true, "spreadsheetId": true, "documentId": true, "sheetId": true, "sheetName": true,
	"folderId": true, "driveId": true, "fileId": true, "baseId": true, "base": true, "table": true, "tableId": true, "databaseId": true,
	"pageId": true, "calendar": true, "calendarId": true, "listId": true, "boardId": true, "projectId": true, "workspaceId": true,
	"repository": true, "owner": true, "sendTo": true, "toEmail": true, "fromEmail": true, "phoneNumberId": true, "recipientPhoneNumber": true,
	"guildId": true, "webhookUri": true, "collectionId": true,
}

func setupFields(wf map[string]interface{}) []SetupField {
	out := []SetupField{}
	for _, n := range asList(wf["nodes"]) {
		nm := asMap(n)
		ty := str(nm["type"])
		if ty == "n8n-nodes-base.stickyNote" || nm["disabled"] == true {
			continue
		}
		name := str(nm["name"])
		walk(asMap(nm["parameters"]), "", func(path, key string, v interface{}) {
			add := func(reason string) {
				out = append(out, SetupField{Node: name, NodeType: ty, Parameter: path, Value: v, Reason: reason})
			}
			if codeParams[key] || choiceParams[key] {
				return
			}
			if m, ok := v.(map[string]interface{}); ok && m["__rl"] == true {
				val := str(m["value"])
				if strings.HasPrefix(val, "=") {
					return // an expression
				}
				add("resource")
				return
			}
			s, ok := v.(string)
			if !ok || strings.TrimSpace(s) == "" {
				return
			}
			expr := strings.HasPrefix(s, "=")
			switch {
			case !expr && placeholderRe.MatchString(s), hasBlank(s):
				add("placeholder")
			case !expr && exampleRe.MatchString(s):
				add("placeholder")
			case !expr && accountParams[key]:
				add("account-specific")
			}
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Parameter < out[j].Parameter
	})
	return out
}

// hasBlank reports a {{ … }} holding plain words ("{{ voice id }}"), a
// blank the author left, not an n8n expression ("{{ $json.id }}": the
// pattern admits no $, dots or brackets).
func hasBlank(s string) bool {
	for _, m := range blankBracesRe.FindAllString(s, -1) {
		if inner := strings.TrimSpace(strings.Trim(m, "{}")); strings.ContainsAny(inner, " _-") {
			return true
		}
	}
	return false
}

// walk visits every leaf and resource locator of a parameter tree.
func walk(v interface{}, path string, fn func(path, key string, v interface{})) {
	key := path
	if i := strings.LastIndexAny(path, ".["); i >= 0 {
		key = path[i+1:]
	}
	key = strings.TrimSuffix(key, "]")
	switch x := v.(type) {
	case map[string]interface{}:
		if x["__rl"] == true {
			fn(path, key, x)
			return
		}
		keys := sortedKeysAny(x)
		for _, k := range keys {
			p := k
			if path != "" {
				p = path + "." + k
			}
			walk(x[k], p, fn)
		}
	case []interface{}:
		for i, e := range x {
			walk(e, path+"["+strconv.Itoa(i)+"]", fn)
		}
	default:
		if path != "" {
			fn(path, key, x)
		}
	}
}

// ---------------------------------------------------------------- helpers

func asMap(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}

func asList(v interface{}) []interface{} {
	l, _ := v.([]interface{})
	return l
}

func str(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

func num(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeysAny(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// noAccountNodes need no credential (flow control, data, triggers, AI
// plumbing whose credentials sit on their model sub-nodes).
var noAccountNodes = map[string]bool{}

func init() {
	for _, n := range strings.Fields(`set if switch merge code function functionItem noOp wait splitInBatches splitOut aggregate summarize filter
		limit sort removeDuplicates itemLists dateTime crypto html htmlExtract xml markdown extractFromFile convertToFile compression
		stopAndError executeWorkflow executeWorkflowTrigger respondToWebhook webhook manualTrigger scheduleTrigger cron interval
		formTrigger form errorTrigger stickyNote readWriteFile readBinaryFile readBinaryFiles writeBinaryFile editImage renameKeys
		executionData n8nTrigger evaluation evaluationTrigger quickChart rssFeedRead rssFeedReadTrigger moveBinaryData spreadsheetFile
		readPDF jwt timeSaved debugHelper localFileTrigger simulate simulateTrigger dataTable`) {
		noAccountNodes["n8n-nodes-base."+n] = true
	}
	for _, n := range strings.Fields(`agent agentTool chainLlm chainRetrievalQa chainSummarization chatTrigger manualChatTrigger chat
		textClassifier informationExtractor sentimentAnalysis outputParserStructured outputParserAutofixing outputParserItemList
		memoryBufferWindow toolCode toolWorkflow toolThink toolCalculator toolHttpRequest documentDefaultDataLoader
		documentBinaryInputLoader textSplitterRecursiveCharacterTextSplitter textSplitterCharacterTextSplitter
		textSplitterTokenSplitter vectorStoreInMemory vectorStoreInMemoryInsert vectorStoreInMemoryLoad retrieverVectorStore
		code mcpTrigger guardrails`) {
		noAccountNodes["@n8n/n8n-nodes-langchain."+n] = true
	}
}

// needsAccount lists enabled service nodes that name no credential.
func needsAccount(wf map[string]interface{}) []NodeRef {
	out := []NodeRef{}
	for _, n := range asList(wf["nodes"]) {
		nm := asMap(n)
		ty := str(nm["type"])
		if ty == "" || nm["disabled"] == true || noAccountNodes[ty] || len(asMap(nm["credentials"])) > 0 {
			continue
		}
		if ty == "n8n-nodes-base.httpRequest" {
			if a := str(asMap(nm["parameters"])["authentication"]); a == "" || a == "none" {
				continue
			}
		}
		out = append(out, NodeRef{Node: str(nm["name"]), Type: ty})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Node < out[j].Node })
	return out
}

func (c *Client) base() string {
	if c.API == "" {
		return DefaultAPI
	}
	return strings.TrimRight(c.API, "/")
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP == nil {
		return &http.Client{Timeout: 30 * time.Second}
	}
	return c.HTTP
}

func (c *Client) agent() string {
	if c.UserAgent == "" {
		return "flow-n8n-templates"
	}
	return c.UserAgent
}

// ---------------------------------------------------------------- browsing

// Summary is a template as the gallery lists it, before it is checked.
type Summary struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	TotalViews  int      `json:"total_views"`
	CreatedAt   string   `json:"created_at,omitempty"`
	Author      *Author  `json:"author,omitempty"`
	Services    []string `json:"services"` // node display names
}

// CategoryCount is a gallery category with its number of templates.
type CategoryCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// Page is one page of the gallery.
type Page struct {
	Total      int             `json:"total"`
	Templates  []Summary       `json:"templates"`
	Categories []CategoryCount `json:"categories"`
}

// ListOptions selects a gallery page.
type ListOptions struct {
	Page     int // from 1
	Rows     int // per page, at most 100
	Search   string
	Category string
}

// List returns one page of the gallery; paid templates are left out.
func (c *Client) List(ctx context.Context, o ListOptions) (*Page, error) {
	if o.Page < 1 {
		o.Page = 1
	}
	if o.Rows < 1 || o.Rows > 100 {
		o.Rows = 24
	}
	q := fmt.Sprintf("%s/search?rows=%d&page=%d", c.base(), o.Rows, o.Page)
	if o.Search != "" {
		q += "&search=" + urlQuery(o.Search)
	}
	if o.Category != "" {
		q += "&category=" + urlQuery(o.Category)
	}
	var res struct {
		TotalWorkflows int `json:"totalWorkflows"`
		Workflows      []struct {
			ID          json.Number `json:"id"`
			Name        string      `json:"name"`
			Description string      `json:"description"`
			TotalViews  json.Number `json:"totalViews"`
			Price       json.Number `json:"price"`
			CreatedAt   string      `json:"createdAt"`
			User        *Author     `json:"user"`
			Nodes       []struct {
				Name        string `json:"name"`
				DisplayName string `json:"displayName"`
			} `json:"nodes"`
		} `json:"workflows"`
		Filters []struct {
			FieldName string `json:"field_name"`
			Counts    []struct {
				Value string `json:"value"`
				Count int    `json:"count"`
			} `json:"counts"`
		} `json:"filters"`
	}
	if err := c.get(ctx, q, &res); err != nil {
		return nil, err
	}
	p := &Page{Total: res.TotalWorkflows, Templates: []Summary{}, Categories: []CategoryCount{}}
	for _, w := range res.Workflows {
		if price, _ := w.Price.Float64(); price > 0 {
			continue
		}
		views, _ := w.TotalViews.Int64()
		s := Summary{ID: w.ID.String(), Name: w.Name, Description: w.Description, TotalViews: int(views), CreatedAt: w.CreatedAt, Author: w.User, Services: []string{}}
		seen := map[string]bool{}
		for _, n := range w.Nodes {
			if n.Name == "n8n-nodes-base.stickyNote" || n.DisplayName == "" || seen[n.DisplayName] {
				continue
			}
			seen[n.DisplayName] = true
			s.Services = append(s.Services, n.DisplayName)
		}
		p.Templates = append(p.Templates, s)
	}
	for _, f := range res.Filters {
		if f.FieldName != "" && f.FieldName != "categories" {
			continue
		}
		for _, c := range f.Counts {
			p.Categories = append(p.Categories, CategoryCount{Name: c.Value, Count: c.Count})
		}
		break
	}
	return p, nil
}

// Template fetches and checks one template; paid templates are refused.
func (c *Client) Template(ctx context.Context, id string) (*Template, error) {
	raw, err := c.Fetch(ctx, id)
	if err != nil {
		return nil, err
	}
	t, paid, err := Build(id, raw, time.Now().UTC())
	if paid {
		return nil, errors.New("paid template")
	}
	return t, err
}

// ---------------------------------------------------------------- local folders

// LoadDir reads the n8n workflow exports in a folder tree (.json, and .txt
// files holding JSON) and checks each like a gallery template. Files that
// are not workflows are skipped; workflows with the same nodes are kept
// once. The first folder level becomes the category, the file's path the
// id.
func LoadDir(dir string, now time.Time) ([]Template, error) {
	var out []Template
	seen := map[string]bool{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); p != dir && (strings.HasPrefix(n, ".") || n == "__MACOSX") {
				return filepath.SkipDir
			}
			return nil
		}
		if ext := strings.ToLower(filepath.Ext(p)); ext != ".json" && ext != ".txt" {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.Size() > 10<<20 {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		var wf map[string]interface{}
		if json.Unmarshal(bytes.TrimPrefix(b, []byte("\xef\xbb\xbf")), &wf) != nil || asList(wf["nodes"]) == nil {
			return nil
		}
		sig := nodeSignature(wf)
		if seen[sig] {
			return nil
		}
		seen[sig] = true
		rel, _ := filepath.Rel(dir, p)
		rel = filepath.ToSlash(rel)
		name := str(wf["name"])
		if name == "" {
			name = strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))
			wf["name"] = name
		}
		t := Template{Source: "local", SourceID: sig[:16], SourceURL: rel, Name: name, Categories: []string{}, FetchedAt: now.Format(time.RFC3339)}
		if i := strings.Index(rel, "/"); i > 0 {
			t.Categories = append(t.Categories, strings.ReplaceAll(rel[:i], "_", " "))
		}
		if err := t.check(wf, "local-"+sig[:16]); err != nil {
			return nil
		}
		out = append(out, t)
		return nil
	})
	sort.SliceStable(out, func(i, j int) bool { return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name) })
	return out, err
}

// nodeSignature identifies a workflow by its nodes' types and names.
func nodeSignature(wf map[string]interface{}) string {
	var parts []string
	for _, n := range asList(wf["nodes"]) {
		m := asMap(n)
		if ty := str(m["type"]); ty != "n8n-nodes-base.stickyNote" {
			parts = append(parts, ty+"\x00"+str(m["name"]))
		}
	}
	sort.Strings(parts)
	h := sha1.Sum([]byte(strings.Join(parts, "\x01")))
	return hex.EncodeToString(h[:])
}
