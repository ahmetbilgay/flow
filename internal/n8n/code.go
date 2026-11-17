package n8n

import (
	"errors"
	"fmt"
	"time"

	"github.com/dop251/goja"
)

// runJS runs code inside an async function and returns its exported result.
// Only synchronous work completes: promises that depend on real I/O are
// rejected because the sandbox has no event loop sources.
func (r *Runtime) runJS(code string, timeout time.Duration) (interface{}, error) {
	return r.runJSWith("", code, timeout)
}

// runJSWith runs code with outer variables declared by preamble, so user
// code may redeclare them (e.g. "const items = ...") without a SyntaxError.
func (r *Runtime) runJSWith(preamble, code string, timeout time.Duration) (interface{}, error) {
	p, err := goja.Compile("code", "(function(){\n"+preamble+"\nreturn (async function(){\n"+code+"\n})();\n})()", false)
	if err != nil {
		return nil, fmt.Errorf("syntax error: %v", cleanJSError(err))
	}
	return r.guard(timeout, func() (interface{}, error) {
		v, err := r.vm.RunProgram(p)
		if err != nil {
			return nil, cleanJSError(err)
		}
		return r.settle(v)
	})
}

// settle resolves a returned promise and exports the value.
func (r *Runtime) settle(v goja.Value) (interface{}, error) {
	if prom, ok := v.Export().(*goja.Promise); ok {
		switch prom.State() {
		case goja.PromiseStateFulfilled:
			v = prom.Result()
		case goja.PromiseStateRejected:
			res := prom.Result()
			if obj, ok := res.(*goja.Object); ok {
				if m := obj.Get("message"); m != nil {
					return nil, errors.New(m.String())
				}
			}
			return nil, errors.New(res.String())
		default:
			return nil, errors.New("code awaited an operation that is not available in this runtime (e.g. HTTP requests from Code nodes)")
		}
	}
	return r.export(v)
}

// toItems normalizes a Code node result into items.
func toItemsResult(v interface{}, inputCount int) ([]Item, error) {
	var list []interface{}
	switch t := v.(type) {
	case nil:
		return nil, errors.New("code doesn't return items properly: return an array of objects, e.g. [{json: {...}}]")
	case []interface{}:
		list = t
	case map[string]interface{}:
		list = []interface{}{t}
	default:
		return nil, fmt.Errorf("code doesn't return items properly: got %T", v)
	}
	out := make([]Item, 0, len(list))
	for i, el := range list {
		m, ok := el.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("code doesn't return items properly: item %d is not an object", i)
		}
		it := Item{}
		if j, ok := m["json"].(map[string]interface{}); ok {
			it.JSON = j
			if b, ok := m["binary"].(map[string]interface{}); ok {
				it.Binary = b
			}
			it.PairedItem = m["pairedItem"]
		} else {
			it.JSON = m
		}
		if it.PairedItem == nil && len(list) == inputCount {
			it.PairedItem = paired(i)
		}
		out = append(out, it)
	}
	return out, nil
}

func codeNode(ec *execCtx) ([][]Item, error) {
	if lang, _ := ec.str(0, "language", "javaScript"); lang == "python" || lang == "pythonNative" {
		code, _ := ec.str(0, "pythonCode", "")
		mode, _ := ec.str(0, "mode", "runOnceForAllItems")
		return runPython(ec, code, mode == "runOnceForEachItem")
	}
	code, _ := ec.str(0, "jsCode", "")
	if code == "" {
		code, _ = ec.str(0, "code", "")
	}
	mode, _ := ec.str(0, "mode", "runOnceForAllItems")
	if mode == "runOnceForEachItem" {
		return runCode(ec, code, true, "var item = $input.item;")
	}
	return runCode(ec, code, false, "var items = $input.all();")
}

// functionNode supports the legacy Function / Function Item nodes.
func functionNode(ec *execCtx) ([][]Item, error) {
	code, _ := ec.str(0, "functionCode", "")
	return runCode(ec, code, false, "var items = $input.all();\n")
}

func functionItemNode(ec *execCtx) ([][]Item, error) {
	code, _ := ec.str(0, "functionCode", "")
	return runCode(ec, "var __r = (function(){\n"+code+"\n})();\nreturn __r === undefined ? item : __r;", true, "var item = $json;")
}

func runCode(ec *execCtx, code string, perItem bool, preamble string) ([][]Item, error) {
	rt, err := ec.runtime()
	if err != nil {
		return nil, err
	}
	items := ec.items()
	timeout := ec.opts.codeTimeout()
	if !perItem {
		rt.setItem(0)
		v, err := rt.runJSWith(preamble, code, timeout)
		if err != nil {
			return nil, fmt.Errorf("code: %w", err)
		}
		out, err := toItemsResult(v, len(items))
		if err != nil {
			return nil, err
		}
		return [][]Item{out}, nil
	}
	var out []Item
	for i := range items {
		rt.setItem(i)
		v, err := rt.runJSWith(preamble, code, timeout)
		if err != nil {
			return nil, fmt.Errorf("code (item %d): %w", i, err)
		}
		if v == nil {
			continue
		}
		res, err := toItemsResult(v, 1)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		for _, it := range res {
			if it.PairedItem == nil || len(res) == 1 {
				it.PairedItem = paired(i)
			}
			out = append(out, it)
		}
	}
	return [][]Item{out}, nil
}
