package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/google/uuid"
)

// stepIDNamespace seeds deterministic (UUIDv5) step IDs.
var stepIDNamespace = uuid.MustParse("3b0f6c5e-8f8a-4c1e-9a49-0f6c1d2e7a10")

// forwardStepID is the deterministic ID of the step that runs nodeID on the
// normal (forward) path of an execution. Because every scheduler computes the
// same ID, two workers finishing parallel branches at the same time cannot
// create a join node twice: the second insert fails with ErrStepExists.
func forwardStepID(execID, nodeID string) string {
	return uuid.NewSHA1(stepIDNamespace, []byte(execID+"\x00"+nodeID)).String()
}

// derivedID builds a deterministic, UUID-shaped ID from a prefix and a parent
// ID (e.g. the compensation step for a failed step). It stays within the
// VARCHAR(36) id columns of the SQL schema.
func derivedID(prefix, parentID string) string {
	return uuid.NewSHA1(stepIDNamespace, []byte(prefix+"\x00"+parentID)).String()
}

// createStep inserts a step, treating "already exists" as a lost race.
// It returns true only when this call created the step.
func (e *Engine) createStep(ctx context.Context, namespace string, step *models.ExecutionStep) (bool, error) {
	err := e.storage.CreateExecutionStep(ctx, namespace, step)
	if errors.Is(err, storage.ErrStepExists) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// nodeSteps indexes the forward step of every node. The deterministic forward
// ID wins; otherwise (executions created before deterministic IDs existed) the
// most recently started step for the node is used.
func nodeSteps(execID string, steps []*models.ExecutionStep) map[string]*models.ExecutionStep {
	byNode := make(map[string]*models.ExecutionStep, len(steps))
	for _, s := range steps {
		cur, ok := byNode[s.NodeID]
		switch {
		case !ok:
			byNode[s.NodeID] = s
		case cur.ID == forwardStepID(execID, cur.NodeID):
			// keep the forward step
		case s.ID == forwardStepID(execID, s.NodeID) || s.StartedAt.After(cur.StartedAt):
			byNode[s.NodeID] = s
		}
	}
	return byNode
}

// uniquePredecessors returns the distinct parents of nodeID in edge order.
func uniquePredecessors(g *Graph, nodeID string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range g.Predecessors[nodeID] {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func edgeTaken(g *Graph, from, to, output string) bool {
	for _, id := range g.GetNextNodesWithBranch(from, output) {
		if id == to {
			return true
		}
	}
	return false
}

// advanceFrom resolves every direct child of nodeID after nodeID settled
// (completed or skipped).
func (e *Engine) advanceFrom(ctx context.Context, exec *models.Execution, wf *models.Workflow, g *Graph, nodeID string) error {
	seen := map[string]bool{}
	for _, edge := range g.Edges[nodeID] {
		if seen[edge.TargetID] {
			continue
		}
		seen[edge.TargetID] = true
		if err := e.resolveNode(ctx, exec, wf, g, edge.TargetID); err != nil {
			return err
		}
	}
	return nil
}

// resolveNode decides what happens to nodeID once all of its predecessors
// have settled:
//   - any predecessor still running/waiting/failed → nothing yet
//   - at least one completed predecessor routed to nodeID → schedule it
//   - every incoming edge is dead (skipped parent or branch not taken) → skip
//     it and propagate to its children.
func (e *Engine) resolveNode(ctx context.Context, exec *models.Execution, wf *models.Workflow, g *Graph, nodeID string) error {
	steps, err := e.storage.GetExecutionSteps(ctx, exec.Namespace, exec.ID)
	if err != nil {
		return err
	}
	byNode := nodeSteps(exec.ID, steps)
	if _, exists := byNode[nodeID]; exists {
		return nil // already scheduled or skipped
	}

	preds := uniquePredecessors(g, nodeID)
	var active []string
	for _, p := range preds {
		ps := byNode[p]
		if ps == nil {
			return nil
		}
		switch ps.Status {
		case models.TaskCompleted:
			if edgeTaken(g, p, nodeID, ps.Output) {
				active = append(active, p)
			}
		case models.TaskSkipped:
			// dead edge
		default:
			return nil // not settled (or failed: downstream never runs)
		}
	}

	if len(active) == 0 {
		now := time.Now()
		skipped := &models.ExecutionStep{
			ID:          forwardStepID(exec.ID, nodeID),
			Namespace:   exec.Namespace,
			ExecutionID: exec.ID,
			NodeID:      nodeID,
			Status:      models.TaskSkipped,
			StartedAt:   now,
			FinishedAt:  &now,
		}
		created, err := e.createStep(ctx, exec.Namespace, skipped)
		if err != nil || !created {
			return err
		}
		e.logger.Debug("Skipping node", "node", nodeID, "execution", exec.ID)
		return e.advanceFrom(ctx, exec, wf, g, nodeID)
	}

	var base string
	if len(preds) > 1 {
		agg := make(map[string]interface{}, len(active))
		for _, p := range active {
			var out interface{}
			_ = json.Unmarshal([]byte(byNode[p].Output), &out)
			agg[p] = out
		}
		b, _ := json.Marshal(agg)
		base = string(b)
	} else {
		base = byNode[active[0]].Output
	}
	input := e.mergeVarsIntoInput(ctx, exec.Namespace, exec.ID, base)

	newStep := NewStepForNode(exec.Namespace, exec.ID, nodeID, input, FindNode(wf, nodeID))
	newStep.ID = forwardStepID(exec.ID, nodeID)
	created, err := e.createStep(ctx, exec.Namespace, newStep)
	if err != nil || !created {
		return err
	}
	if newStep.Status == models.TaskWaiting {
		e.emit(ctx, Event{
			Type: EventTypeStepWaitingHuman, Namespace: exec.Namespace, Execution: exec, Step: newStep, Workflow: wf,
		})
	}
	return nil
}
