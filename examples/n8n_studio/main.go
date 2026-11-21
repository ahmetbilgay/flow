// Command n8n_studio is a small web UI for trying n8n workflows on the flow
// engine: paste or upload a workflow (or load a sample or a template from
// the n8n gallery), see its compatibility report drawn as a graph, provide
// credentials, install and run it, and inspect every step's output.
//
//	go run ./examples/n8n_studio            # http://localhost:8080
//	go run ./examples/n8n_studio -addr :9000
//
// Everything is kept in memory. The server listens on localhost by default;
// do not expose it publicly: it stores credentials and runs workflows.
package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgmos/flow"
	"github.com/dgmos/flow/internal/n8ntemplates"
)

//go:embed index.html samples/*.json
var assets embed.FS

const namespace = "studio"

type server struct {
	eng   *flow.Engine
	creds *flow.N8NStaticCredentials
	base  string // public URL of the n8n handler, for webhook URLs

	market *n8ntemplates.Client

	mu        sync.Mutex
	workflows map[string]*installed
	credTypes map[string]bool
}

type installed struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	InstalledAt time.Time       `json:"installedAt"`
	Report      *flow.N8NReport `json:"report"`
	Graph       *graph          `json:"graph"`
	nodeNames   map[string]string
}

// graph is the workflow as drawn by the UI.
type graph struct {
	Nodes []graphNode `json:"nodes"`
	Links []graphLink `json:"links"`
}

type graphNode struct {
	Name      string     `json:"name"`
	Type      string     `json:"type"`
	X         float64    `json:"x"`
	Y         float64    `json:"y"`
	Supported bool       `json:"supported"`
	Disabled  bool       `json:"disabled,omitempty"`
	Trigger   bool       `json:"trigger,omitempty"`
	Notes     string     `json:"notes,omitempty"`
	Blocking  []string   `json:"blocking,omitempty"`
	Warnings  []string   `json:"warnings,omitempty"`
	Params    any        `json:"parameters,omitempty"`
	Creds     []string   `json:"credentials,omitempty"`
	Sticky    *stickyBox `json:"sticky,omitempty"`
}

type stickyBox struct {
	W float64 `json:"w"`
	H float64 `json:"h"`
}

type graphLink struct {
	From string `json:"from"`
	To   string `json:"to"`
	Kind string `json:"kind"` // main, ai_tool, ai_languageModel, …
	Out  int    `json:"out"`
}

func main() {
	addr := flag.String("addr", "localhost:8080", "listen address")
	public := flag.String("public-url", "", "public base URL (default http://<addr>)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	base := *public
	if base == "" {
		host := *addr
		if strings.HasPrefix(host, ":") {
			host = "localhost" + host
		}
		base = "http://" + host
	}
	base = strings.TrimRight(base, "/")

	s := &server{
		eng:       flow.NewEngine(flow.NewMemoryStorage(), flow.NewRegistry()),
		creds:     flow.NewN8NStaticCredentials(),
		base:      base + "/n8n",
		workflows: map[string]*installed{},
		credTypes: map[string]bool{},
		market:    &n8ntemplates.Client{HTTP: &http.Client{Timeout: 20 * time.Second}},
	}
	s.eng.EnableN8N(flow.N8NOptions{
		Credentials:   s.creds,
		ResumeBaseURL: s.base,
		ResumeSecret:  randomHex(16),
	})
	go func() {
		if err := s.eng.CronManager.Start(ctx); err != nil {
			log.Printf("schedules: %v", err)
		}
	}()
	go s.eng.StartWorkerWithOptions(ctx, namespace, "studio-worker", flow.WorkerOptions{Concurrency: 4, PollInterval: 20 * time.Millisecond})

	mux := http.NewServeMux()
	static, _ := fs.Sub(assets, ".")
	mux.Handle("/", http.FileServerFS(static))
	mux.HandleFunc("GET /api/samples", s.samples)
	mux.HandleFunc("GET /api/gallery/{id}", s.gallery)
	mux.HandleFunc("GET /api/market", s.marketList)
	mux.HandleFunc("GET /api/market/{id}", s.marketTemplate)
	mux.HandleFunc("POST /api/check", s.check)
	mux.HandleFunc("POST /api/install", s.install)
	mux.HandleFunc("GET /api/workflows", s.list)
	mux.HandleFunc("GET /api/workflows/{id}", s.get)
	mux.HandleFunc("POST /api/workflows/{id}/run", s.run)
	mux.HandleFunc("GET /api/executions", s.executions)
	mux.HandleFunc("GET /api/executions/{id}", s.execution)
	mux.HandleFunc("GET /api/credentials", s.listCreds)
	mux.HandleFunc("POST /api/credentials", s.setCred)
	mux.Handle("/n8n/", http.StripPrefix("/n8n", s.eng.N8NHandler(flow.N8NHandlerOptions{Namespace: namespace, ResponseTimeout: 60 * time.Second})))

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("n8n Studio on %s", base)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, format string, a ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, a...)})
}

// ---------------------------------------------------------------- samples & gallery

func (s *server) samples(w http.ResponseWriter, _ *http.Request) {
	entries, _ := fs.ReadDir(assets, "samples")
	type sample struct {
		File     string          `json:"file"`
		Name     string          `json:"name"`
		Workflow json.RawMessage `json:"workflow"`
	}
	var out []sample
	for _, e := range entries {
		b, err := assets.ReadFile("samples/" + e.Name())
		if err != nil {
			continue
		}
		var meta struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(b, &meta)
		out = append(out, sample{File: e.Name(), Name: meta.Name, Workflow: b})
	}
	writeJSON(w, 200, out)
}

var digits = regexp.MustCompile(`^\d{1,7}$`)

// gallery fetches a public template from n8n's template API.
func (s *server) gallery(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !digits.MatchString(id) {
		fail(w, 400, "template id must be a number")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.n8n.io/api/templates/workflows/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fail(w, 502, "gallery: %v", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		fail(w, 404, "template %s not found (status %d)", id, resp.StatusCode)
		return
	}
	var body struct {
		Workflow struct {
			Name     string          `json:"name"`
			Workflow json.RawMessage `json:"workflow"`
		} `json:"workflow"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 10<<20)).Decode(&body); err != nil || len(body.Workflow.Workflow) == 0 {
		fail(w, 502, "gallery: unexpected response")
		return
	}
	// Templates carry the name outside the workflow JSON.
	var wf map[string]any
	if json.Unmarshal(body.Workflow.Workflow, &wf) == nil {
		if _, ok := wf["name"]; !ok {
			wf["name"] = body.Workflow.Name
		}
	}
	writeJSON(w, 200, map[string]any{"name": body.Workflow.Name, "workflow": wf})
}

// ---------------------------------------------------------------- market

// marketList returns a page of the public n8n gallery (paid templates left out).
func (s *server) marketList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	p, err := s.market.List(r.Context(), n8ntemplates.ListOptions{Page: page, Rows: 20, Search: q.Get("q"), Category: q.Get("category")})
	if err != nil {
		fail(w, 502, "gallery: %v", err)
		return
	}
	writeJSON(w, 200, p)
}

// marketTemplate fetches one template and checks it: whether it runs, the
// accounts it needs, its setup fields and its workflow.
func (s *server) marketTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !digits.MatchString(id) {
		fail(w, 400, "template id must be a number")
		return
	}
	t, err := s.market.Template(r.Context(), id)
	if err != nil {
		fail(w, 502, "template %s: %v", id, err)
		return
	}
	writeJSON(w, 200, t)
}

// ---------------------------------------------------------------- check & install

func readWorkflow(r *http.Request) ([]byte, string, error) {
	var req struct {
		Workflow json.RawMessage `json:"workflow"`
		ID       string          `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 20<<20)).Decode(&req); err != nil {
		return nil, "", fmt.Errorf("invalid request: %v", err)
	}
	if len(req.Workflow) == 0 {
		return nil, "", errors.New("no workflow given")
	}
	// Accept a template API response or {"workflow": …} wrapper too.
	var probe map[string]json.RawMessage
	if json.Unmarshal(req.Workflow, &probe) == nil {
		if _, ok := probe["nodes"]; !ok {
			if inner, ok := probe["workflow"]; ok {
				req.Workflow = inner
			}
		}
	}
	return req.Workflow, req.ID, nil
}

func (s *server) check(w http.ResponseWriter, r *http.Request) {
	data, id, err := readWorkflow(r)
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	wf, rep, err := flow.ImportN8NWorkflow(data, flow.N8NImportOptions{ID: id})
	if err != nil {
		fail(w, 422, "import: %v", err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": wf.ID, "name": wf.Name, "report": rep, "graph": buildGraph(data, rep)})
}

func (s *server) install(w http.ResponseWriter, r *http.Request) {
	data, id, err := readWorkflow(r)
	if err != nil {
		fail(w, 400, "%v", err)
		return
	}
	res, err := s.eng.InstallN8NWorkflow(r.Context(), namespace, data, flow.N8NInstallOptions{
		N8NImportOptions: flow.N8NImportOptions{ID: id},
		WebhookBaseURL:   s.base,
	})
	if err != nil && (res == nil || res.Workflow == nil || errors.Is(err, flow.ErrN8NNotRunnable)) {
		out := map[string]any{"error": err.Error()}
		if res != nil {
			out["report"] = res.Report
		}
		writeJSON(w, 422, out)
		return
	}
	inst := &installed{ID: res.Workflow.ID, Name: res.Workflow.Name, InstalledAt: time.Now(), Report: res.Report,
		Graph: buildGraph(data, res.Report), nodeNames: map[string]string{}}
	for _, n := range res.Workflow.Nodes {
		inst.nodeNames[n.ID] = n.Name
	}
	s.mu.Lock()
	s.workflows[inst.ID] = inst
	s.mu.Unlock()
	out := map[string]any{"workflow": inst, "scheduleIds": res.ScheduleIDs, "webhooks": res.Webhooks, "activationExecutions": res.ActivationExecutions}
	if err != nil {
		out["warning"] = err.Error()
	}
	writeJSON(w, 200, out)
}

func (s *server) list(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]*installed, 0, len(s.workflows))
	for _, wf := range s.workflows {
		out = append(out, wf)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].InstalledAt.After(out[j].InstalledAt) })
	writeJSON(w, 200, out)
}

func (s *server) lookup(id string) *installed {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workflows[id]
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	inst := s.lookup(r.PathValue("id"))
	if inst == nil {
		fail(w, 404, "workflow not installed")
		return
	}
	writeJSON(w, 200, map[string]any{"workflow": inst, "endpoints": s.endpoints(inst)})
}

// endpoints lists the URLs the workflow's triggers listen on.
func (s *server) endpoints(inst *installed) []map[string]string {
	var out []map[string]string
	for _, t := range inst.Report.Triggers {
		switch t.Kind {
		case "webhook":
			m := t.Method
			if m == "" {
				m = "POST"
			}
			out = append(out, map[string]string{"kind": "webhook", "node": t.Node, "method": m, "url": s.base + "/webhook/" + t.Path})
		case "chat":
			out = append(out, map[string]string{"kind": "chat", "node": t.Node, "method": "POST", "url": s.base + "/chat/" + inst.ID})
		case "form":
			out = append(out, map[string]string{"kind": "form", "node": t.Node, "method": "POST", "url": s.base + "/form/" + inst.ID})
		}
	}
	return out
}

// ---------------------------------------------------------------- runs

func (s *server) run(w http.ResponseWriter, r *http.Request) {
	inst := s.lookup(r.PathValue("id"))
	if inst == nil {
		fail(w, 404, "workflow not installed")
		return
	}
	var req struct {
		Trigger string `json:"trigger"`
		Items   any    `json:"items"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 5<<20)).Decode(&req)
	input := map[string]any{}
	if req.Trigger != "" {
		input[flow.N8NTriggerKey] = req.Trigger
	}
	if req.Items != nil {
		input["items"] = req.Items
	}
	b, _ := json.Marshal(input)
	id, err := s.eng.Execute(r.Context(), namespace, inst.ID, b)
	if err != nil {
		fail(w, 500, "start: %v", err)
		return
	}
	writeJSON(w, 200, map[string]string{"executionId": id})
}

type execSummary struct {
	ID         string     `json:"id"`
	WorkflowID string     `json:"workflowId"`
	Status     string     `json:"status"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Trigger    string     `json:"trigger,omitempty"`
}

func summarize(e *flow.Execution) execSummary {
	var in map[string]any
	_ = json.Unmarshal([]byte(e.Input), &in)
	t, _ := in[flow.N8NTriggerKey].(string)
	return execSummary{ID: e.ID, WorkflowID: e.WorkflowID, Status: string(e.Status), Error: e.ErrorMessage, StartedAt: e.StartedAt, FinishedAt: e.FinishedAt, Trigger: t}
}

func (s *server) executions(w http.ResponseWriter, r *http.Request) {
	all, err := s.eng.ListExecutions(r.Context(), namespace)
	if err != nil {
		fail(w, 500, "%v", err)
		return
	}
	wf := r.URL.Query().Get("workflow")
	out := []execSummary{}
	for _, e := range all {
		if wf == "" || e.WorkflowID == wf {
			out = append(out, summarize(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	if len(out) > 50 {
		out = out[:50]
	}
	writeJSON(w, 200, out)
}

type stepView struct {
	Node       string           `json:"node"`
	Status     string           `json:"status"`
	Error      string           `json:"error,omitempty"`
	Attempt    int              `json:"attempt"`
	StartedAt  time.Time        `json:"startedAt"`
	DurationMs *int64           `json:"durationMs,omitempty"`
	Outputs    [][]flow.N8NItem `json:"outputs"`
}

func (s *server) execution(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	e, err := s.eng.GetExecution(ctx, namespace, r.PathValue("id"))
	if err != nil {
		fail(w, 404, "%v", err)
		return
	}
	steps, _ := s.eng.GetExecutionSteps(ctx, namespace, e.ID)
	names := map[string]string{}
	if inst := s.lookup(e.WorkflowID); inst != nil {
		names = inst.nodeNames
	}
	views := make([]stepView, 0, len(steps))
	for _, st := range steps {
		name := names[st.NodeID]
		if name == "" {
			name = st.NodeID
		}
		v := stepView{Node: name, Status: string(st.Status), Error: st.Error, Attempt: st.AttemptNumber, StartedAt: st.StartedAt, Outputs: outputPorts(st.Output)}
		if st.FinishedAt != nil && !st.StartedAt.IsZero() {
			d := st.FinishedAt.Sub(st.StartedAt).Milliseconds()
			v.DurationMs = &d
		}
		views = append(views, v)
	}
	sort.SliceStable(views, func(i, j int) bool { return views[i].StartedAt.Before(views[j].StartedAt) })
	writeJSON(w, 200, map[string]any{"execution": summarize(e), "output": flow.N8NOutputItems(e.Output), "steps": views})
}

// outputPorts decodes a step output ({"__ports": {"0": [...], ...}}) into
// its ports' items, keeping them small enough for the browser.
func outputPorts(raw string) [][]flow.N8NItem {
	var env struct {
		Ports map[string][]flow.N8NItem `json:"__ports"`
	}
	ports := [][]flow.N8NItem{}
	if json.Unmarshal([]byte(raw), &env) == nil {
		for k, items := range env.Ports {
			var i int
			if _, err := fmt.Sscanf(k, "%d", &i); err != nil || i < 0 || i > 64 {
				continue
			}
			for len(ports) <= i {
				ports = append(ports, []flow.N8NItem{})
			}
			ports[i] = items
		}
	}
	for p := range ports {
		if len(ports[p]) > 200 {
			ports[p] = ports[p][:200]
		}
		for i := range ports[p] {
			for k, b := range ports[p][i].Binary {
				if m, ok := b.(map[string]any); ok {
					c := map[string]any{}
					for mk, mv := range m {
						if mk != "data" {
							c[mk] = mv
						}
					}
					ports[p][i].Binary[k] = c
				}
			}
		}
	}
	return ports
}

// ---------------------------------------------------------------- credentials

func (s *server) listCreds(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]string, 0, len(s.credTypes))
	for t := range s.credTypes {
		out = append(out, t)
	}
	s.mu.Unlock()
	sort.Strings(out)
	writeJSON(w, 200, out)
}

func (s *server) setCred(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil || req.Type == "" || req.Data == nil {
		fail(w, 400, "need {type, data}")
		return
	}
	s.creds.Set(req.Type, "*", req.Data)
	s.mu.Lock()
	s.credTypes[req.Type] = true
	s.mu.Unlock()
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// ---------------------------------------------------------------- graph

func buildGraph(data []byte, rep *flow.N8NReport) *graph {
	var raw struct {
		Nodes []struct {
			Name        string         `json:"name"`
			Type        string         `json:"type"`
			Position    []float64      `json:"position"`
			Disabled    bool           `json:"disabled"`
			Notes       any            `json:"notes"`
			Parameters  map[string]any `json:"parameters"`
			Credentials map[string]any `json:"credentials"`
		} `json:"nodes"`
		Connections map[string]map[string][][]struct {
			Node string `json:"node"`
		} `json:"connections"`
	}
	_ = json.Unmarshal(data, &raw)
	issues := func(list []flow.N8NIssue) map[string][]string {
		m := map[string][]string{}
		for _, is := range list {
			m[is.Node] = append(m[is.Node], is.Message)
		}
		return m
	}
	block, warn := issues(rep.Blocking), issues(rep.Warnings)
	g := &graph{Nodes: []graphNode{}, Links: []graphLink{}}
	for _, n := range raw.Nodes {
		gn := graphNode{Name: n.Name, Type: n.Type, Disabled: n.Disabled, Blocking: block[n.Name], Warnings: warn[n.Name], Params: n.Parameters}
		if len(n.Position) == 2 {
			gn.X, gn.Y = n.Position[0], n.Position[1]
		}
		if s, ok := n.Notes.(string); ok {
			gn.Notes = s
		}
		for k := range n.Credentials {
			gn.Creds = append(gn.Creds, k)
		}
		if n.Type == "n8n-nodes-base.stickyNote" {
			wd, _ := n.Parameters["width"].(float64)
			ht, _ := n.Parameters["height"].(float64)
			if wd == 0 {
				wd = 240
			}
			if ht == 0 {
				ht = 160
			}
			gn.Sticky = &stickyBox{W: wd, H: ht}
			gn.Supported = true
		} else {
			gn.Supported = flow.N8NNodeSupported(n.Type) && len(gn.Blocking) == 0
		}
		gn.Trigger = strings.HasSuffix(strings.ToLower(n.Type), "trigger") || strings.HasSuffix(n.Type, ".webhook") || strings.HasSuffix(n.Type, ".formTrigger")
		g.Nodes = append(g.Nodes, gn)
	}
	for from, kinds := range raw.Connections {
		for kind, outs := range kinds {
			for port, targets := range outs {
				for _, t := range targets {
					g.Links = append(g.Links, graphLink{From: from, To: t.Node, Kind: kind, Out: port})
				}
			}
		}
	}
	sort.Slice(g.Links, func(i, j int) bool {
		a, b := g.Links[i], g.Links[j]
		return a.From+a.Kind+a.To < b.From+b.Kind+b.To
	})
	return g
}
