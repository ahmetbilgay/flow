package n8n

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dgmos/flow/internal/engine"
	"github.com/dgmos/flow/internal/models"
)

// stepView is the decoded state of one node within an execution.
type stepView struct {
	executed bool
	input    NodeInput
	outputs  map[int][]Item
}

// execData gives node executors read access to the rest of the execution:
// other nodes' outputs ($('Node')), their inputs (for paired-item tracing)
// and workflow metadata. In-process runs (loop bodies) add an overlay.
type execData struct {
	exec    *models.Execution
	wf      *models.Workflow
	byName  map[string]*models.Node
	byID    map[string]*models.Node
	steps   map[string]*models.ExecutionStep
	overlay map[string]*stepView
	cache   map[string]*stepView
	opts    *Options
}

func newExecData(exec *models.Execution, wf *models.Workflow, steps []*models.ExecutionStep, opts *Options) *execData {
	d := &execData{
		exec:    exec,
		wf:      wf,
		byName:  map[string]*models.Node{},
		byID:    map[string]*models.Node{},
		steps:   map[string]*models.ExecutionStep{},
		overlay: map[string]*stepView{},
		cache:   map[string]*stepView{},
		opts:    opts,
	}
	if d.exec == nil {
		d.exec = &models.Execution{}
	}
	if d.wf == nil {
		d.wf = &models.Workflow{}
	}
	var index func(nodes []models.Node)
	index = func(nodes []models.Node) {
		for i := range nodes {
			n := &nodes[i]
			d.byID[n.ID] = n
			name := n.Name
			if name == "" {
				name = n.ID
			}
			d.byName[name] = n
			// Loop bodies live inside their loop node's config.
			for _, key := range []string{cfgLoop, cfgCycle} {
				if raw, ok := n.Config[key]; ok {
					var spec struct {
						Nodes []models.Node `json:"nodes"`
					}
					b, _ := json.Marshal(raw)
					if json.Unmarshal(b, &spec) == nil {
						index(spec.Nodes)
					}
				}
			}
		}
	}
	index(d.wf.Nodes)
	for _, s := range steps {
		cur, ok := d.steps[s.NodeID]
		if !ok || s.StartedAt.After(cur.StartedAt) || s.Status == models.TaskCompleted && cur.Status != models.TaskCompleted {
			d.steps[s.NodeID] = s
		}
		// Loop nodes record the last run of their body nodes.
		if s.Status == models.TaskCompleted && strings.Contains(s.Output, `"__nodeRuns"`) {
			var env struct {
				Runs map[string]struct {
					Outputs map[int][]Item `json:"outputs"`
				} `json:"__nodeRuns"`
			}
			if json.Unmarshal([]byte(s.Output), &env) == nil {
				for id, r := range env.Runs {
					d.cache[id] = &stepView{executed: true, outputs: r.Outputs}
				}
			}
		}
	}
	return d
}

// loadExecData reads the current execution through the engine in ctx. When
// the node runs outside an engine (unit tests), an empty view is returned.
func loadExecData(ctx context.Context, opts *Options) (*execData, error) {
	// In-process runs (loop bodies, sub-workflows) share one view.
	if ov := overlayFromContext(ctx); ov != nil {
		return ov, nil
	}
	eng := engine.EngineFromContext(ctx)
	ns, execID := engine.NamespaceFromContext(ctx), engine.ExecutionIDFromContext(ctx)
	if eng == nil || execID == "" {
		return newExecData(nil, nil, nil, opts), nil
	}
	exec, wf, steps, err := eng.ExecutionSnapshot(ctx, ns, execID)
	if err != nil {
		return nil, err
	}
	return newExecData(exec, wf, steps, opts), nil
}

type overlayKey struct{}

func withOverlay(ctx context.Context, d *execData) context.Context {
	return context.WithValue(ctx, overlayKey{}, d)
}

func overlayFromContext(ctx context.Context) *execData {
	d, _ := ctx.Value(overlayKey{}).(*execData)
	return d
}

func (d *execData) nodeByName(name string) (*models.Node, bool) {
	n, ok := d.byName[name]
	return n, ok
}

func (d *execData) view(nodeID string) *stepView {
	if v, ok := d.overlay[nodeID]; ok {
		return v
	}
	if v, ok := d.cache[nodeID]; ok {
		return v
	}
	v := &stepView{}
	if s, ok := d.steps[nodeID]; ok && s.Status == models.TaskCompleted {
		v.executed = true
		v.input = parseNodeInput(s.Input)
		v.outputs = decodeOutput(s.Output)
	}
	d.cache[nodeID] = v
	return v
}

// output returns the items a named node produced on a port.
func (d *execData) output(name string, port int) ([]Item, error) {
	n, ok := d.nodeByName(name)
	if !ok {
		return nil, fmt.Errorf("referenced node doesn't exist: %q", name)
	}
	v := d.view(n.ID)
	if !v.executed {
		return nil, fmt.Errorf("referenced node is unexecuted: %q", name)
	}
	return v.outputs[port], nil
}

// pairedItem traces input item idx of the current node back to the output
// item of the named node it derives from, following pairedItem links.
func (d *execData) pairedItem(current *models.Node, in NodeInput, target string, idx int) (port, index int, ok bool) {
	t, exists := d.nodeByName(target)
	if !exists {
		return 0, 0, false
	}
	inp, inPort, k := in, 0, idx
	for depth := 0; depth < 200; depth++ {
		src, off, found := locate(inp, inPort, k)
		if !found {
			return 0, 0, false
		}
		srcNode, ok := d.byID[src.Node]
		if !ok {
			return 0, 0, false
		}
		sp := atoiDefault(src.Port, 0)
		if srcNode.ID == t.ID {
			return sp, off, true
		}
		v := d.view(srcNode.ID)
		if !v.executed {
			return 0, 0, false
		}
		items := v.outputs[sp]
		if off >= len(items) {
			return 0, 0, false
		}
		next, nextPort := pairedIndex(items[off].PairedItem, off)
		inp, inPort, k = v.input, nextPort, next
		if len(inp.Inputs) == 0 {
			// Reached a trigger/start node without finding the target.
			return 0, 0, false
		}
	}
	return 0, 0, false
}

// locate finds which source segment input item k on port p came from.
func locate(in NodeInput, p, k int) (Source, int, bool) {
	if p >= len(in.Sources) {
		return Source{}, 0, false
	}
	start := 0
	for _, s := range in.Sources[p] {
		if k < start+s.Count {
			return s, s.Offset + k - start, true
		}
		start += s.Count
	}
	return Source{}, 0, false
}

// pairedIndex extracts {item, input} from an item's pairedItem, defaulting to
// the item's own index (n8n's behavior when lineage is missing).
func pairedIndex(p interface{}, fallback int) (int, int) {
	switch v := p.(type) {
	case float64:
		return int(v), 0
	case map[string]interface{}:
		item, input := fallback, 0
		if f, ok := v["item"].(float64); ok {
			item = int(f)
		}
		if f, ok := v["input"].(float64); ok {
			input = int(f)
		}
		return item, input
	case []interface{}:
		if len(v) > 0 {
			return pairedIndex(v[0], fallback)
		}
	}
	return fallback, 0
}
