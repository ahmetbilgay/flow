package engine

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/models"
)

const (
	NodeTypeHumanTask = "humantask"
	WaitHuman         = "HUMAN_WAIT"
	WaitSignal        = "SIGNAL_WAIT"
)

// IsInteractiveNode returns true when the node should not be auto-claimed by workers
// (humantask or execution_mode=interactive).
func IsInteractiveNode(n models.Node) bool {
	if n.Type == NodeTypeHumanTask {
		return true
	}
	if n.Config == nil {
		return false
	}
	mode, _ := n.Config["execution_mode"].(string)
	return mode == "interactive"
}

// FindNode returns the node definition by ID.
func FindNode(wf *models.Workflow, nodeID string) *models.Node {
	for i := range wf.Nodes {
		if wf.Nodes[i].ID == nodeID {
			return &wf.Nodes[i]
		}
	}
	return nil
}

// ApplyHumanTaskConfig copies assignee/claim fields from node config onto a step.
func ApplyHumanTaskConfig(step *models.ExecutionStep, n *models.Node) {
	if n == nil || n.Config == nil {
		return
	}
	step.Assignees = stringSliceFromConfig(n.Config["assignees"])
	step.CandidateGroups = stringSliceFromConfig(n.Config["candidate_groups"])
	if mode, ok := n.Config["claim_mode"].(string); ok && mode != "" {
		step.ClaimMode = models.ClaimMode(mode)
	} else {
		step.ClaimMode = models.ClaimAny
	}
	if p, ok := n.Config["priority"].(float64); ok {
		step.Priority = int(p)
	}
	if due, ok := n.Config["due_at"].(string); ok && due != "" {
		if t, err := time.Parse(time.RFC3339, due); err == nil {
			step.DueAt = &t
		}
	}
}

func stringSliceFromConfig(v interface{}) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []interface{}:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// NewStepForNode creates a PENDING or WAITING step based on node type/mode.
func NewStepForNode(ns, execID, nodeID, input string, n *models.Node) *models.ExecutionStep {
	step := &models.ExecutionStep{
		Namespace:   ns,
		ExecutionID: execID,
		NodeID:      nodeID,
		Status:      models.TaskPending,
		Input:       input,
		StartedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if n != nil && IsInteractiveNode(*n) {
		step.Status = models.TaskWaiting
		ApplyHumanTaskConfig(step, n)
	}
	return step
}

// MergeActionPayload merges action + payload JSON into an output map.
func MergeActionPayload(existingInput, action, payload string) string {
	out := map[string]interface{}{}
	if existingInput != "" {
		_ = json.Unmarshal([]byte(existingInput), &out)
	}
	if payload != "" {
		var p map[string]interface{}
		if err := json.Unmarshal([]byte(payload), &p); err == nil {
			for k, v := range p {
				out[k] = v
			}
		} else {
			out["payload"] = payload
		}
	}
	if action != "" {
		out["action"] = action
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want || strings.EqualFold(s, want) {
			return true
		}
	}
	return false
}
