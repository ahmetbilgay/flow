package n8n

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/dop251/goja"

	"github.com/dgmos/flow/internal/models"
)

//go:embed prelude.js
var preludeSrc string

const contextSrc = `(function (g) {
  var outCache = {}, inputCache = null;
  function items(name, port) {
    var k = name + '\u0000' + port;
    if (!(k in outCache)) outCache[k] = JSON.parse(__nodeOutput(name, port));
    return outCache[k];
  }
  function inputItems() { if (inputCache === null) inputCache = JSON.parse(__inputItems()); return inputCache; }
  g.__resetCaches = function () { outCache = {}; inputCache = null; };
  function pairedOf(name, idx) {
    var r = __paired(name, idx === undefined ? g.$itemIndex : idx);
    if (!r) {
      var all = items(name, 0);
      return all.length === 1 ? all[0] : undefined;
    }
    var p = JSON.parse(r);
    return items(name, p.port)[p.index];
  }
  g.$ = function (name) {
    __assertNode(name);
    return {
      all: function (b) { return items(name, b || 0); },
      first: function (b) { return items(name, b || 0)[0]; },
      last: function (b) { var a = items(name, b || 0); return a[a.length - 1]; },
      get item() { return pairedOf(name); },
      itemMatching: function (i) { return pairedOf(name, i); },
      pairedItem: function (i) { return pairedOf(name, i); },
      get isExecuted() { return __hasRun(name); },
      get params() { return JSON.parse(__params(name)); },
      get context() { return JSON.parse(__nodeContext(name)); },
      get json() { var it = pairedOf(name); return it && it.json; }
    };
  };
  g.$node = new Proxy({}, {
    get: function (t, name) {
      if (typeof name !== 'string') return undefined;
      var x = g.$(name);
      return {
        get json() { var it = pairedOf(name) || items(name, 0)[0]; return it && it.json; },
        get binary() { var it = pairedOf(name) || items(name, 0)[0]; return it && it.binary; },
        get parameter() { return x.params; },
        get context() { return x.context; }, runIndex: 0
      };
    }
  });
  g.$items = function (name, out) { return name ? items(name, out || 0) : inputItems(); };
  g.$input = {
    all: function () { return inputItems(); },
    first: function () { return inputItems()[0]; },
    last: function () { var a = inputItems(); return a[a.length - 1]; },
    get item() { return inputItems()[g.$itemIndex]; },
    get params() { return JSON.parse(__params('')); },
    context: {}
  };
  Object.defineProperty(g, '$json', { get: function () { var it = inputItems()[g.$itemIndex]; return it ? it.json : {}; }, configurable: true });
  Object.defineProperty(g, '$binary', { get: function () { var it = inputItems()[g.$itemIndex]; return (it && it.binary) || {}; }, configurable: true });
  Object.defineProperty(g, '$parameter', { get: function () { return JSON.parse(__params('')); }, configurable: true });
  g.$item = function (i) { return { $json: (inputItems()[i] || {}).json, $node: g.$node }; };
  g.$itemIndex = 0; g.$runIndex = 0;
  g.$workflow = JSON.parse(__workflowInfo());
  g.$execution = JSON.parse(__executionInfo());
  g.$execution.customData = { set: function () { }, get: function () { }, setAll: function () { }, getAll: function () { return {}; } };
  g.$env = JSON.parse(__env());
  g.$vars = JSON.parse(__vars());
  g.$prevNode = JSON.parse(__prevNode());
  g.$now = DateTime.now();
  g.$today = g.$now.startOf('day');
  var staticData = {};
  g.$getWorkflowStaticData = function (t) { return staticData[t] || (staticData[t] = {}); };
  g.$jmespath = function () { throw new Error('$jmespath is not supported'); };
  g.$fromAI = g.$fromai = g.$fromAi = function (key, desc, type, def) {
    var args = JSON.parse(__fromAI());
    if (args === null) throw new Error('$fromAI() can only be used in AI tool parameters');
    var v = args[key];
    return v === undefined ? def : v;
  };
})(this);`

func init() {
	// Bound JS regular expressions that fall back to the backtracking engine
	// (lookarounds, backreferences) so they cannot hang a worker.
	regexp2.DefaultMatchTimeout = 2 * time.Second
}

var (
	compileOnce sync.Once
	preludeProg *goja.Program
	contextProg *goja.Program
	compileErr  error
)

func programs() (*goja.Program, *goja.Program, error) {
	compileOnce.Do(func() {
		preludeProg, compileErr = goja.Compile("prelude.js", preludeSrc, true)
		if compileErr != nil {
			return
		}
		contextProg, compileErr = goja.Compile("context.js", contextSrc, true)
	})
	return preludeProg, contextProg, compileErr
}

// maxJSCallDepth bounds JavaScript recursion in expressions and Code nodes.
const maxJSCallDepth = 5000

// Runtime evaluates n8n expressions for one node execution.
type Runtime struct {
	vm      *goja.Runtime
	data    *execData
	node    *models.Node
	input   NodeInput
	items   []Item
	progs   map[string]*goja.Program
	timeout time.Duration
	logs    []string
	// ctx cancels running JavaScript (worker shutdown, lease loss).
	ctx context.Context
	// broken is set when a run had to be abandoned (stuck in native code);
	// the VM is then never touched again.
	broken bool
	// fromAI holds the model-provided arguments for $fromAI() in AI tools.
	fromAI map[string]interface{}
}

// newRuntime prepares a JS runtime bound to a node execution. items is the
// node's main input (or trigger items).
func newRuntime(data *execData, node *models.Node, input NodeInput, items []Item) (*Runtime, error) {
	pre, ctxp, err := programs()
	if err != nil {
		return nil, fmt.Errorf("n8n runtime: %w", err)
	}
	r := &Runtime{
		vm:      goja.New(),
		data:    data,
		node:    node,
		input:   input,
		items:   items,
		progs:   map[string]*goja.Program{},
		timeout: data.opts.exprTimeout(),
	}
	r.vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	// Fail fast on runaway recursion instead of growing until the timeout.
	r.vm.SetMaxCallStackSize(maxJSCallDepth)
	if err := r.installHost(); err != nil {
		return nil, err
	}
	if _, err := r.run(pre); err != nil {
		return nil, fmt.Errorf("n8n prelude: %w", err)
	}
	if _, err := r.run(ctxp); err != nil {
		return nil, fmt.Errorf("n8n context: %w", err)
	}
	return r, nil
}

func (r *Runtime) throw(format string, a ...interface{}) {
	panic(r.vm.NewGoError(fmt.Errorf(format, a...)))
}

func (r *Runtime) installHost() error {
	set := func(name string, fn interface{}) error { return r.vm.Set(name, fn) }
	zone := r.data.opts.timezone(r.data.wf)
	host := map[string]interface{}{
		"__defaultZone": func() string { return zone },
		"__localZone":   func() string { return time.Local.String() },
		"__tzOffset": func(name string, ms float64) interface{} {
			loc, err := loadZone(name)
			if err != nil {
				return nil
			}
			_, off := time.UnixMilli(int64(ms)).In(loc).Zone()
			return off / 60
		},
		"__tzLocal": func(name string, y, mo, d, h, mi, s, ms int) float64 {
			loc, err := loadZone(name)
			if err != nil {
				loc = time.UTC
			}
			return float64(time.Date(y, time.Month(mo), d, h, mi, s, ms*int(time.Millisecond), loc).UnixMilli())
		},
		"__hash": func(algo, s string) string {
			var h hash.Hash
			switch strings.ToLower(algo) {
			case "sha1":
				h = sha1.New()
			case "sha256":
				h = sha256.New()
			case "sha512":
				h = sha512.New()
			default:
				h = md5.New()
			}
			h.Write([]byte(s))
			return hex.EncodeToString(h.Sum(nil))
		},
		"__b64enc": func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) },
		"__b64dec": func(s string) string {
			b, err := decodeB64(s)
			if err != nil {
				r.throw("invalid base64: %v", err)
			}
			return string(b)
		},
		"__b64norm": func(s string) string {
			b, err := decodeB64(s)
			if err != nil {
				r.throw("invalid base64: %v", err)
			}
			return base64.StdEncoding.EncodeToString(b)
		},
		"__b64len": func(s string) int { b, _ := decodeB64(s); return len(b) },
		"__b64hex": func(s string) string { b, _ := decodeB64(s); return hex.EncodeToString(b) },
		"__hexb64": func(s string) string {
			b, err := hex.DecodeString(s)
			if err != nil {
				r.throw("invalid hex: %v", err)
			}
			return base64.StdEncoding.EncodeToString(b)
		},
		"__log": func(call goja.FunctionCall) goja.Value {
			parts := make([]string, 0, len(call.Arguments))
			for _, a := range call.Arguments {
				parts = append(parts, a.String())
			}
			if len(r.logs) < 100 {
				r.logs = append(r.logs, strings.Join(parts, " "))
			}
			return goja.Undefined()
		},
		"__assertNode": func(name string) {
			if _, ok := r.data.nodeByName(name); !ok {
				r.throw("referenced node doesn't exist: %q", name)
			}
		},
		"__nodeOutput": func(name string, port int) string {
			items, err := r.data.output(name, port)
			if err != nil {
				r.throw("%v", err)
			}
			return mustJSON(nonNilItems(items))
		},
		"__paired": func(name string, idx int) interface{} {
			port, index, ok := r.data.pairedItem(r.node, r.input, name, idx)
			if !ok {
				return nil
			}
			return fmt.Sprintf(`{"port":%d,"index":%d}`, port, index)
		},
		"__nodeContext": func(name string) string {
			n, ok := r.data.nodeByName(name)
			if !ok {
				return "{}"
			}
			if c := r.data.nodeCtx[n.ID]; c != nil {
				return mustJSON(c)
			}
			return "{}"
		},
		"__hasRun": func(name string) bool {
			n, ok := r.data.nodeByName(name)
			return ok && r.data.view(n.ID).executed
		},
		"__params": func(name string) string {
			n := r.node
			if name != "" {
				var ok bool
				if n, ok = r.data.nodeByName(name); !ok {
					r.throw("referenced node doesn't exist: %q", name)
				}
			}
			if n == nil {
				return "{}"
			}
			return mustJSON(nodeParams(n))
		},
		"__inputItems": func() string { return mustJSON(nonNilItems(r.items)) },
		"__workflowInfo": func() string {
			return mustJSON(map[string]interface{}{"id": r.data.wf.ID, "name": r.data.wf.Name, "active": true})
		},
		"__executionInfo": func() string {
			ru := r.data.opts.resumeURL(r.data.exec.Namespace, r.data.exec.ID, "")
			return mustJSON(map[string]interface{}{"id": r.data.exec.ID, "mode": "production", "resumeUrl": ru, "resumeFormUrl": ru})
		},
		"__env": func() string { return mustJSON(r.data.opts.env()) },
		"__fromAI": func() string {
			if r.fromAI == nil {
				return "null"
			}
			return mustJSON(r.fromAI)
		},
		"__vars": func() string { return mustJSON(r.data.exec.Variables) },
		"__prevNode": func() string {
			prev := map[string]interface{}{"name": "", "outputIndex": 0, "runIndex": 0}
			if len(r.input.Sources) > 0 && len(r.input.Sources[0]) > 0 {
				src := r.input.Sources[0][0]
				if n, ok := r.data.byID[src.Node]; ok {
					prev["name"] = n.Name
					prev["outputIndex"] = atoiDefault(src.Port, 0)
				}
			}
			return mustJSON(prev)
		},
	}
	for k, v := range host {
		if err := set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// run executes a program with the expression timeout.
func (r *Runtime) run(p *goja.Program) (goja.Value, error) {
	t := time.AfterFunc(r.timeout, func() { r.vm.Interrupt("execution timed out") })
	defer t.Stop()
	v, err := r.vm.RunProgram(p)
	r.vm.ClearInterrupt()
	return v, cleanJSError(err)
}

func cleanJSError(err error) error {
	if err == nil {
		return nil
	}
	var ex *goja.Exception
	if errors.As(err, &ex) {
		return errors.New(ex.Value().String())
	}
	var in *goja.InterruptedError
	if errors.As(err, &in) {
		return fmt.Errorf("expression %v", in.Value())
	}
	return err
}

// setItem selects the current item ($json, $itemIndex).
// setGlobalJSON defines a JavaScript global from a JSON-compatible value
// (as native JS objects and arrays, so n8n's prototype helpers apply), or
// removes it when v is nil.
func (r *Runtime) setGlobalJSON(name string, v interface{}) error {
	if v == nil {
		return r.vm.Set(name, goja.Undefined())
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	parse, ok := goja.AssertFunction(r.vm.Get("JSON").ToObject(r.vm).Get("parse"))
	if !ok {
		return errors.New("JSON.parse unavailable")
	}
	val, err := parse(goja.Undefined(), r.vm.ToValue(string(b)))
	if err != nil {
		return err
	}
	return r.vm.Set(name, val)
}

func (r *Runtime) setItem(i int) {
	_ = r.vm.Set("$itemIndex", i)
}

func (r *Runtime) program(code string) (*goja.Program, error) {
	if p, ok := r.progs[code]; ok {
		return p, nil
	}
	p, err := goja.Compile("expr", "(function(){ return (\n"+code+"\n); })()", false)
	if err != nil {
		var err2 error
		p, err2 = goja.Compile("expr", "(function(){\n"+code+"\n})()", false)
		if err2 != nil {
			return nil, fmt.Errorf("invalid expression syntax: %v", err)
		}
	}
	r.progs[code] = p
	return p, nil
}

// evalExpr evaluates a JS expression and returns a JSON-compatible value.
func (r *Runtime) evalExpr(code string) (interface{}, error) {
	p, err := r.program(code)
	if err != nil {
		return nil, err
	}
	return r.guard(r.timeout, func() (interface{}, error) {
		v, err := r.vm.RunProgram(p)
		if err != nil {
			return nil, cleanJSError(err)
		}
		return r.export(v)
	})
}

// guard runs fn with an interrupt timer and context cancellation. If fn is
// stuck in native code that ignores interrupts (e.g. a pathological
// JSON.stringify), the run is abandoned after a grace period so the worker
// is freed; the VM is marked broken and never used again.
func (r *Runtime) guard(timeout time.Duration, fn func() (interface{}, error)) (interface{}, error) {
	if r.broken {
		return nil, errors.New("javascript runtime is unusable after an earlier timeout")
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	type result struct {
		v   interface{}
		err error
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- result{nil, fmt.Errorf("javascript panic: %v", p)}
			}
		}()
		v, err := fn()
		done <- result{v, err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	interrupted := false
	for {
		select {
		case res := <-done:
			r.vm.ClearInterrupt()
			return res.v, res.err
		case <-timer.C:
			if interrupted {
				r.broken = true
				return nil, errors.New("execution timed out")
			}
			r.vm.Interrupt("execution timed out")
			interrupted = true
			timer.Reset(2 * time.Second) // grace period for native code
		case <-ctx.Done():
			if !interrupted {
				r.vm.Interrupt("execution cancelled")
				interrupted = true
				timer.Reset(2 * time.Second)
			}
			ctx = context.Background() // only once
		}
	}
}

func (r *Runtime) export(v goja.Value) (interface{}, error) {
	fn, _ := goja.AssertFunction(r.vm.Get("__export"))
	s, err := fn(goja.Undefined(), v)
	if err != nil {
		return nil, cleanJSError(err)
	}
	var wrapped struct {
		V interface{} `json:"v"`
		U int         `json:"u"`
	}
	if err := json.Unmarshal([]byte(s.String()), &wrapped); err != nil {
		return nil, err
	}
	if wrapped.U == 1 {
		return nil, nil
	}
	return wrapped.V, nil
}

func (r *Runtime) evalString(code string) (string, error) {
	p, err := r.program(code)
	if err != nil {
		return "", err
	}
	v, err := r.guard(r.timeout, func() (interface{}, error) {
		v, err := r.vm.RunProgram(p)
		if err != nil {
			return nil, cleanJSError(err)
		}
		fn, _ := goja.AssertFunction(r.vm.Get("__str"))
		s, err := fn(goja.Undefined(), v)
		if err != nil {
			return nil, cleanJSError(err)
		}
		return s.String(), nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// resolve deep-resolves n8n parameter values: strings starting with "=" are
// expressions/templates, everything else is returned as-is.
func (r *Runtime) resolve(v interface{}) (interface{}, error) {
	switch t := v.(type) {
	case string:
		if !strings.HasPrefix(t, "=") {
			return t, nil
		}
		return r.resolveTemplate(t[1:])
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, x := range t {
			rv, err := r.resolve(x)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = rv
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, x := range t {
			rv, err := r.resolve(x)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return v, nil
	}
}

type segment struct {
	expr bool
	text string
}

// splitTemplate splits "a {{ x }} b" into literal and expression segments,
// respecting braces and string literals inside expressions.
func splitTemplate(s string) ([]segment, error) {
	var segs []segment
	for {
		i := strings.Index(s, "{{")
		if i < 0 {
			if s != "" {
				segs = append(segs, segment{text: s})
			}
			return segs, nil
		}
		if i > 0 {
			segs = append(segs, segment{text: s[:i]})
		}
		end, err := exprEnd(s, i+2)
		if err != nil {
			return nil, err
		}
		segs = append(segs, segment{expr: true, text: s[i+2 : end]})
		s = s[end+2:]
	}
}

func exprEnd(s string, from int) (int, error) {
	depth := 0
	var quote byte
	for i := from; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '/':
			if j, ok := regexLiteralEnd(s, from, i); ok {
				i = j
			}
		case '{':
			depth++
		case '}':
			if depth == 0 && i+1 < len(s) && s[i+1] == '}' {
				return i, nil
			}
			if depth > 0 {
				depth--
			}
		}
	}
	// Unbalanced (e.g. a quote we misread): fall back to the next "}}", which
	// is how n8n itself splits templates.
	if j := strings.Index(s[from:], "}}"); j >= 0 {
		return from + j, nil
	}
	return 0, errors.New("unterminated expression: missing }}")
}

// regexLiteralEnd reports whether the '/' at s[i] starts a JS regex literal
// (judged by the previous significant character, as JS tokenizers do) and,
// if so, returns the index of its closing '/'. Quotes and braces inside a
// regex such as /['"{]/g must not be treated as code.
func regexLiteralEnd(s string, from, i int) (int, bool) {
	p := i - 1
	for p >= from && (s[p] == ' ' || s[p] == '\t' || s[p] == '\n' || s[p] == '\r') {
		p--
	}
	if p >= from {
		prev := s[p]
		isIdent := prev == '_' || prev == '$' || prev == ')' || prev == ']' ||
			(prev >= '0' && prev <= '9') || (prev >= 'a' && prev <= 'z') || (prev >= 'A' && prev <= 'Z')
		if isIdent {
			// Division, unless the identifier is a keyword that precedes an expression.
			k := p
			for k >= from && (s[k] == '_' || s[k] == '$' || (s[k] >= 'a' && s[k] <= 'z') || (s[k] >= 'A' && s[k] <= 'Z')) {
				k--
			}
			switch s[k+1 : p+1] {
			case "return", "typeof", "in", "of", "case", "void", "delete", "instanceof", "new":
			default:
				return 0, false
			}
		}
	}
	inClass := false
	for j := i + 1; j < len(s); j++ {
		switch s[j] {
		case '\\':
			j++
		case '\n':
			return 0, false
		case '[':
			inClass = true
		case ']':
			inClass = false
		case '/':
			if !inClass {
				return j, true
			}
		}
	}
	return 0, false
}

func (r *Runtime) resolveTemplate(body string) (interface{}, error) {
	segs, err := splitTemplate(body)
	if err != nil {
		return nil, err
	}
	exprCount := 0
	for _, s := range segs {
		if s.expr {
			exprCount++
		}
	}
	if exprCount == 0 {
		return body, nil
	}
	if exprCount == 1 && len(segs) == 1 {
		for _, s := range segs {
			if s.expr {
				return r.evalExpr(s.text)
			}
		}
	}
	var b strings.Builder
	for _, s := range segs {
		if !s.expr {
			b.WriteString(s.text)
			continue
		}
		v, err := r.evalString(s.text)
		if err != nil {
			return nil, err
		}
		b.WriteString(v)
	}
	return b.String(), nil
}

// ---------------------------------------------------------------- helpers

var zoneCache sync.Map

func loadZone(name string) (*time.Location, error) {
	if name == "" || name == "UTC" || name == "utc" {
		return time.UTC, nil
	}
	if strings.EqualFold(name, "local") {
		return time.Local, nil
	}
	if l, ok := zoneCache.Load(name); ok {
		return l.(*time.Location), nil
	}
	l, err := time.LoadLocation(name)
	if err != nil {
		return nil, err
	}
	zoneCache.Store(name, l)
	return l, nil
}

func decodeB64(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	if b, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(s, "=")); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}

func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}

func nonNilItems(items []Item) []Item {
	if items == nil {
		return []Item{}
	}
	return items
}

func atoiDefault(s string, d int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return d
}
