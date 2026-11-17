// Package n8n runs n8n workflow definitions on the flow engine.
//
// Every n8n node becomes one engine step. Data moves between steps in n8n's
// item model ([{json, binary, pairedItem}]) wrapped in port envelopes so that
// nodes with several outputs (IF, Switch, …) and several inputs (Merge) keep
// their n8n semantics.
package n8n

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/dgmos/flow/internal/engine"
)

// Item is one n8n data item.
type Item struct {
	JSON       map[string]interface{} `json:"json"`
	Binary     map[string]interface{} `json:"binary,omitempty"`
	PairedItem interface{}            `json:"pairedItem,omitempty"`
}

// Source describes a contiguous run of input items that came from one
// upstream node output.
type Source struct {
	Node  string `json:"node"`
	Port  string `json:"port"`
	Count int    `json:"count"`
	// Offset is the index of the first item within the source node's
	// output (non-zero when a node runs on a single item of a batch).
	Offset int `json:"offset,omitempty"`
}

// inputEnvelope is the step input of an n8n node (built by BuildInput).
type inputEnvelope struct {
	N8N     int        `json:"__n8n"`
	Inputs  [][]Item   `json:"inputs"`
	Sources [][]Source `json:"sources"`
}

// NodeInput is the decoded input of one node execution.
type NodeInput struct {
	Inputs  [][]Item
	Sources [][]Source
	// Raw is set when the node received a non-n8n input (trigger nodes
	// receive the execution input as-is).
	Raw string
}

// Main returns the items on input 0.
func (in *NodeInput) Main() []Item {
	if len(in.Inputs) == 0 {
		return nil
	}
	return in.Inputs[0]
}

// buildInput implements engine.InputBuilder for every n8n executor.
func buildInput(inputs []engine.PortInput) (string, error) {
	env := inputEnvelope{N8N: 1}
	for _, in := range inputs {
		for len(env.Inputs) <= in.ToPort {
			env.Inputs = append(env.Inputs, nil)
			env.Sources = append(env.Sources, nil)
		}
		items, err := decodeItems(in.Data)
		if err != nil {
			return "", fmt.Errorf("input from %s: %w", in.FromNode, err)
		}
		env.Inputs[in.ToPort] = append(env.Inputs[in.ToPort], items...)
		env.Sources[in.ToPort] = append(env.Sources[in.ToPort], Source{Node: in.FromNode, Port: in.FromPort, Count: len(items)})
	}
	b, err := json.Marshal(env)
	return string(b), err
}

// decodeItems accepts an items array, a single item, or a plain JSON value
// (wrapped into one item) so non-n8n upstream nodes interoperate.
func decodeItems(raw json.RawMessage) ([]Item, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) == nil {
		out := make([]Item, 0, len(arr))
		for _, r := range arr {
			out = append(out, toItem(r))
		}
		return out, nil
	}
	return []Item{toItem(raw)}, nil
}

func toItem(raw json.RawMessage) Item {
	var probe map[string]json.RawMessage
	if json.Unmarshal(raw, &probe) == nil {
		if j, ok := probe["json"]; ok {
			var it Item
			if json.Unmarshal(raw, &it) == nil && it.JSON != nil {
				return it
			}
			var m map[string]interface{}
			if json.Unmarshal(j, &m) == nil {
				return Item{JSON: m}
			}
		}
		var m map[string]interface{}
		_ = json.Unmarshal(raw, &m)
		if m == nil {
			m = map[string]interface{}{}
		}
		return Item{JSON: m}
	}
	var v interface{}
	_ = json.Unmarshal(raw, &v)
	return Item{JSON: map[string]interface{}{"data": v}}
}

// parseNodeInput decodes a step input.
func parseNodeInput(input string) NodeInput {
	var env inputEnvelope
	if json.Unmarshal([]byte(input), &env) == nil && env.N8N == 1 {
		return NodeInput{Inputs: env.Inputs, Sources: env.Sources}
	}
	return NodeInput{Raw: input}
}

// encodeOutput wraps per-output item lists into a port envelope. Empty
// outputs are omitted so the engine treats their edges as dead.
func encodeOutput(outputs [][]Item, extra map[string]interface{}) (string, error) {
	ports := map[string][]Item{}
	for i, items := range outputs {
		if len(items) > 0 {
			ports[strconv.Itoa(i)] = items
		}
	}
	env := map[string]interface{}{engine.PortsKey: ports}
	for k, v := range extra {
		env[k] = v
	}
	b, err := json.Marshal(env)
	return string(b), err
}

// decodeOutput reads a step output back into items per port.
func decodeOutput(output string) map[int][]Item {
	var env struct {
		Ports map[string]json.RawMessage `json:"__ports"`
	}
	res := map[int][]Item{}
	if json.Unmarshal([]byte(output), &env) != nil || env.Ports == nil {
		// Non-n8n upstream node: expose its output as items on port 0.
		if output != "" {
			items, _ := decodeItems(json.RawMessage(output))
			res[0] = items
		}
		return res
	}
	for k, raw := range env.Ports {
		idx, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		items, _ := decodeItems(raw)
		res[idx] = items
	}
	return res
}

// OutputItems returns the items of an n8n step output, all ports in order.
// Hosts use it to read the result of an n8n execution.
func OutputItems(output string) []Item {
	ports := decodeOutput(output)
	keys := make([]int, 0, len(ports))
	for k := range ports {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	var out []Item
	for _, k := range keys {
		out = append(out, ports[k]...)
	}
	return out
}

func paired(i int) map[string]interface{} {
	return map[string]interface{}{"item": i}
}

// cloneJSON deep-copies a JSON object.
func cloneJSON(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return map[string]interface{}{}
	}
	b, _ := json.Marshal(m)
	var out map[string]interface{}
	_ = json.Unmarshal(b, &out)
	if out == nil {
		out = map[string]interface{}{}
	}
	return out
}

// asMap returns v as an object, or nil when it is anything else (so ranging
// over parameters with unexpected types never panics).
func asMap(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}
