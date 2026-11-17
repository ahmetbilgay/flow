package n8n

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/dgmos/flow/internal/engine"
	"github.com/dgmos/flow/internal/models"
)

// CycleNodeType is the synthetic node that runs a cyclic region (e.g. a
// polling loop: IF -> Wait -> HTTP -> IF) of an imported n8n workflow.
const CycleNodeType = "n8n:cycle"

const cfgCycle = "__cycle"

// maxCycleRuns bounds node executions inside one cyclic region.
const maxCycleRuns = 10000

type cycleSpec struct {
	Nodes   []models.Node `json:"nodes"`
	Edges   []models.Edge `json:"edges"`   // internal edges
	Entries []cyclePort   `json:"entries"` // entry port -> internal target
	Exits   []cyclePort   `json:"exits"`   // internal source -> exit port
}

type cyclePort struct {
	Port      int    `json:"port"`
	Node      string `json:"node"`
	NodePort  string `json:"nodePort,omitempty"`  // exits: source output port
	NodeInput int    `json:"nodeInput,omitempty"` // entries: target input port
}

// sccs returns the strongly connected components with a cycle (Tarjan).
func sccs(nodes []models.Node, edges []models.Edge) [][]string {
	adj := map[string][]string{}
	self := map[string]bool{}
	for _, e := range edges {
		adj[e.SourceID] = append(adj[e.SourceID], e.TargetID)
		if e.SourceID == e.TargetID {
			self[e.SourceID] = true
		}
	}
	index, low := map[string]int{}, map[string]int{}
	onStack := map[string]bool{}
	var stack []string
	var out [][]string
	next := 0
	var strong func(v string)
	strong = func(v string) {
		index[v], low[v] = next, next
		next++
		stack = append(stack, v)
		onStack[v] = true
		for _, w := range adj[v] {
			if _, seen := index[w]; !seen {
				strong(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] && index[w] < low[v] {
				low[v] = index[w]
			}
		}
		if low[v] == index[v] {
			var comp []string
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				comp = append(comp, w)
				if w == v {
					break
				}
			}
			if len(comp) > 1 || self[comp[0]] {
				sort.Strings(comp)
				out = append(out, comp)
			}
		}
	}
	for _, n := range nodes {
		if _, seen := index[n.ID]; !seen {
			strong(n.ID)
		}
	}
	return out
}

// collapseCycles replaces every cyclic region with a CycleNodeType node.
func collapseCycles(nodes []models.Node, edges []models.Edge, rep *Report) ([]models.Node, []models.Edge) {
	comps := sccs(nodes, edges)
	for ci, comp := range comps {
		in := map[string]bool{}
		for _, id := range comp {
			in[id] = true
		}
		var names []string
		spec := cycleSpec{}
		var keptNodes []models.Node
		for _, n := range nodes {
			if in[n.ID] {
				spec.Nodes = append(spec.Nodes, n)
				names = append(names, n.Name)
			} else {
				keptNodes = append(keptNodes, n)
			}
		}
		id := fmt.Sprintf("cycle-%d-%s", ci, comp[0])
		var keptEdges []models.Edge
		for _, e := range edges {
			switch {
			case in[e.SourceID] && in[e.TargetID]:
				spec.Edges = append(spec.Edges, e)
			case in[e.TargetID]:
				p := len(spec.Entries)
				spec.Entries = append(spec.Entries, cyclePort{Port: p, Node: e.TargetID, NodeInput: e.TargetInput})
				keptEdges = append(keptEdges, models.Edge{ID: e.ID, SourceID: e.SourceID, TargetID: id, Condition: e.Condition, TargetInput: p})
			case in[e.SourceID]:
				p := len(spec.Exits)
				port := e.Condition
				if port == "" {
					port = "0"
				}
				spec.Exits = append(spec.Exits, cyclePort{Port: p, Node: e.SourceID, NodePort: port})
				keptEdges = append(keptEdges, models.Edge{ID: e.ID, SourceID: id, TargetID: e.TargetID, Condition: strconv.Itoa(p), TargetInput: e.TargetInput})
			default:
				keptEdges = append(keptEdges, e)
			}
		}
		sort.Strings(names)
		b, _ := json.Marshal(spec)
		var asMap map[string]interface{}
		_ = json.Unmarshal(b, &asMap)
		keptNodes = append(keptNodes, models.Node{
			ID: id, Name: "Loop (" + strings.Join(names, ", ") + ")", Type: CycleNodeType,
			Config:      map[string]interface{}{cfgCycle: asMap, cfgID: id, cfgName: "Loop (" + strings.Join(names, ", ") + ")"},
			RetryPolicy: &models.RetryPolicy{MaxAttempts: 0, BackoffCoefficient: 1},
			Meta:        map[string]interface{}{"n8n": true, "cycle": names},
		})
		rep.Warnings = append(rep.Warnings, Issue{Message: fmt.Sprintf("loop %v runs as one step (inner nodes are not recorded as separate steps)", names)})
		nodes, edges = keptNodes, keptEdges
	}
	return nodes, edges
}

type pending struct {
	node   string
	inputs []engine.PortInput
}

// cycleNode runs a cyclic region event-driven: a node runs each time data
// arrives, like n8n. Nodes with several inputs (Merge) wait until every
// connected input has data or nothing else can run.
func cycleNode(ec *execCtx) ([][]Item, error) {
	var spec cycleSpec
	b, _ := json.Marshal(ec.node.Config[cfgCycle])
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, err
	}
	reg := registryFrom(engine.EngineFromContext(ec.ctx))
	if reg == nil {
		return nil, errors.New("loop: no engine registry in context")
	}
	byID := map[string]*models.Node{}
	inputPorts := map[string]map[int]bool{}
	for i := range spec.Nodes {
		n := &spec.Nodes[i]
		byID[n.ID] = n
		ec.data.byID[n.ID] = n
		ec.data.byName[n.Name] = n
	}
	for _, e := range spec.Edges {
		if inputPorts[e.TargetID] == nil {
			inputPorts[e.TargetID] = map[int]bool{}
		}
		inputPorts[e.TargetID][e.TargetInput] = true
	}
	for _, en := range spec.Entries {
		if inputPorts[en.Node] == nil {
			inputPorts[en.Node] = map[int]bool{}
		}
		inputPorts[en.Node][en.NodeInput] = true
	}

	var queue []pending
	buffers := map[string][]engine.PortInput{}
	push := func(target string, in engine.PortInput) {
		if len(inputPorts[target]) > 1 {
			buffers[target] = append(buffers[target], in)
			have := map[int]bool{}
			for _, x := range buffers[target] {
				have[x.ToPort] = true
			}
			if len(have) < len(inputPorts[target]) {
				return
			}
			queue = append(queue, pending{target, buffers[target]})
			delete(buffers, target)
			return
		}
		queue = append(queue, pending{target, []engine.PortInput{in}})
	}
	for _, en := range spec.Entries {
		if en.Port >= len(ec.input.Inputs) || len(ec.input.Inputs[en.Port]) == 0 {
			continue
		}
		raw, _ := json.Marshal(ec.input.Inputs[en.Port])
		from := ec.node.ID
		if en.Port < len(ec.input.Sources) && len(ec.input.Sources[en.Port]) > 0 {
			from = ec.input.Sources[en.Port][0].Node
		}
		push(en.Node, engine.PortInput{FromNode: from, FromPort: "0", ToPort: en.NodeInput, Data: raw})
	}

	exits := make([][]Item, len(spec.Exits))
	ctx := withOverlay(ec.ctx, ec.data)
	runs := 0
	for len(queue) > 0 || len(buffers) > 0 {
		if len(queue) == 0 {
			// Flush partially filled multi-input nodes.
			ids := make([]string, 0, len(buffers))
			for id := range buffers {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			queue = append(queue, pending{ids[0], buffers[ids[0]]})
			delete(buffers, ids[0])
		}
		p := queue[0]
		queue = queue[1:]
		if runs++; runs > maxCycleRuns {
			return nil, fmt.Errorf("loop exceeded %d node runs", maxCycleRuns)
		}
		if err := ec.ctx.Err(); err != nil {
			return nil, err
		}
		n := byID[p.node]
		ex, err := reg.Get(n.Type)
		if err != nil {
			return nil, err
		}
		input := ""
		if bld, ok := ex.(engine.InputBuilder); ok {
			if input, err = bld.BuildInput(p.inputs); err != nil {
				return nil, err
			}
		}
		out, err := ex.Execute(ctx, n.Config, input)
		if err != nil {
			if err.Error() == engine.WaitSignal || err.Error() == engine.WaitHuman {
				return nil, fmt.Errorf("node %q waits for an external event, which is not supported inside loops", n.Name)
			}
			return nil, fmt.Errorf("node %q: %w", n.Name, err)
		}
		ports := parsePorts(out)
		ec.data.overlay[n.ID] = &stepView{executed: true, input: parseNodeInput(input), outputs: decodeOutput(out)}
		for _, e := range spec.Edges {
			if e.SourceID != n.ID {
				continue
			}
			key := e.Condition
			if key == "" {
				key = "0"
			}
			if data, ok := ports[key]; ok {
				push(e.TargetID, engine.PortInput{FromNode: n.ID, FromPort: key, ToPort: e.TargetInput, Data: data})
			}
		}
		for _, x := range spec.Exits {
			if x.Node != n.ID {
				continue
			}
			if data, ok := ports[x.NodePort]; ok {
				items, _ := decodeItems(data)
				exits[x.Port] = append(exits[x.Port], items...)
			}
		}
	}
	runsOut := map[string]interface{}{}
	for _, n := range spec.Nodes {
		if v, ok := ec.data.overlay[n.ID]; ok {
			run := map[string]interface{}{"name": n.Name, "outputs": v.outputs}
			if c := persistedContext(ec.data.nodeCtx[n.ID]); c != nil {
				run["context"] = c
			}
			runsOut[n.ID] = run
		}
	}
	ec.extra = map[string]interface{}{"__nodeRuns": runsOut}
	return exits, nil
}
