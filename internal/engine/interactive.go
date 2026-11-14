package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/google/uuid"
)

// TaskFilter is re-exported conceptually via storage; engine uses storage.TaskFilter.
type TaskFilter = storage.TaskFilter

// Advance completes a WAITING (or PENDING interactive) step synchronously.
// action is a free-form string (approve/reject/submit); payload is JSON.
func (e *Engine) Advance(ctx context.Context, namespace, execID, nodeID, action, payload string) (string, []*models.ExecutionStep, error) {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "advance"); err != nil {
		return "", nil, fmt.Errorf("access denied: %w", err)
	}
	if err := e.checkTaskAccess(ctx, namespace, nodeID, "advance"); err != nil {
		return "", nil, fmt.Errorf("access denied: %w", err)
	}
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return "", nil, err
	}
	if exec.Status == models.TaskCancelled || exec.Status == models.TaskCompleted || exec.Status == models.TaskFailed {
		return "", nil, fmt.Errorf("execution %s is not advanceable (status=%s)", execID, exec.Status)
	}

	wf, err := e.storage.GetWorkflow(ctx, namespace, exec.WorkflowID)
	if err != nil {
		return "", nil, err
	}
	n := FindNode(wf, nodeID)
	if n == nil {
		return "", nil, fmt.Errorf("node %s not found", nodeID)
	}

	step, err := e.storage.GetExecutionStepByNode(ctx, namespace, execID, nodeID)
	if err != nil {
		return "", nil, err
	}

	var output string
	switch step.Status {
	case models.TaskWaiting:
		merged := MergeActionPayload(step.Input, action, payload)
		step.Action = action
		// Human/signal waits complete with merged payload; other interactive nodes run executor first.
		if n.Type == NodeTypeHumanTask || n.Type == "signal" {
			output = merged
			if err := e.CompleteStep(ctx, step, output); err != nil {
				return "", nil, err
			}
		} else {
			step.Input = merged
			step.Status = models.TaskRunning
			_ = e.storage.UpdateExecutionStep(ctx, namespace, step)
			executor, err := e.registry.Get(n.Type)
			if err != nil {
				return "", nil, err
			}
			ctx = WithExecutionContext(ctx, namespace, execID)
			ctx = WithEngine(ctx, e)
			out, execErr := executor.Execute(ctx, n.Config, merged)
			if execErr != nil {
				if execErr.Error() == WaitHuman || execErr.Error() == WaitSignal {
					step.Status = models.TaskWaiting
					ApplyHumanTaskConfig(step, n)
					_ = e.storage.UpdateExecutionStep(ctx, namespace, step)
					waiting, _ := e.storage.ListWaitingSteps(ctx, namespace, TaskFilter{ExecutionID: execID})
					return merged, waiting, nil
				}
				step.Status = models.TaskFailed
				step.Error = execErr.Error()
				now := time.Now()
				step.FinishedAt = &now
				_ = e.storage.UpdateExecutionStep(ctx, namespace, step)
				return "", nil, execErr
			}
			output = out
			if err := e.CompleteStep(ctx, step, output); err != nil {
				return "", nil, err
			}
		}
	case models.TaskPending:
		if !IsInteractiveNode(*n) {
			mode, _ := n.Config["execution_mode"].(string)
			if mode != "hybrid" {
				return "", nil, fmt.Errorf("step for node %s is PENDING and not interactive/hybrid — use a worker or set execution_mode", nodeID)
			}
		}
		// Sync execute then complete
		mergedInput := MergeActionPayload(step.Input, action, payload)
		step.Input = mergedInput
		step.Status = models.TaskRunning
		_ = e.storage.UpdateExecutionStep(ctx, namespace, step)

		executor, err := e.registry.Get(n.Type)
		if err != nil {
			return "", nil, err
		}
		ctx = WithExecutionContext(ctx, namespace, execID)
		ctx = WithEngine(ctx, e)
		out, execErr := executor.Execute(ctx, n.Config, mergedInput)
		if execErr != nil {
			if execErr.Error() == WaitHuman || execErr.Error() == WaitSignal {
				step.Status = models.TaskWaiting
				ApplyHumanTaskConfig(step, n)
				step.Action = action
				if err := e.storage.UpdateExecutionStep(ctx, namespace, step); err != nil {
					return "", nil, err
				}
				e.emit(ctx, Event{
					Type: EventTypeStepWaitingHuman, Namespace: namespace, Execution: exec, Step: step, Workflow: wf,
					Metadata: map[string]interface{}{"nodeId": nodeID},
				})
				waiting, _ := e.storage.ListWaitingSteps(ctx, namespace, TaskFilter{ExecutionID: execID})
				return step.Input, waiting, nil
			}
			step.Status = models.TaskFailed
			step.Error = execErr.Error()
			now := time.Now()
			step.FinishedAt = &now
			_ = e.storage.UpdateExecutionStep(ctx, namespace, step)
			return "", nil, execErr
		}
		output = out
		step.Action = action
		if err := e.CompleteStep(ctx, step, output); err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("step for node %s has status %s; cannot advance", nodeID, step.Status)
	}

	waiting, err := e.storage.ListWaitingSteps(ctx, namespace, TaskFilter{ExecutionID: execID})
	if err != nil {
		return output, nil, err
	}
	return output, waiting, nil
}

// ListTasks returns WAITING human/interactive steps.
func (e *Engine) ListTasks(ctx context.Context, namespace string, filter TaskFilter) ([]*models.ExecutionStep, error) {
	if err := e.CheckAccess(ctx, fmt.Sprintf("task:%s:*", namespace), "view"); err != nil {
		return nil, fmt.Errorf("access denied: %w", err)
	}
	return e.storage.ListWaitingSteps(ctx, namespace, filter)
}

// ClaimTask assigns a waiting human task to an actor.
func (e *Engine) ClaimTask(ctx context.Context, namespace, execID, nodeID, actor string) error {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "claim"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	if err := e.checkTaskAccess(ctx, namespace, nodeID, "claim"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	if actor == "" {
		return fmt.Errorf("actor is required")
	}
	step, err := e.storage.GetExecutionStepByNode(ctx, namespace, execID, nodeID)
	if err != nil {
		return err
	}
	if step.Status != models.TaskWaiting {
		return fmt.Errorf("step is not WAITING")
	}
	if step.ClaimedBy != "" && step.ClaimedBy != actor {
		if step.ClaimMode == models.ClaimFirst || step.ClaimMode == models.ClaimAny {
			return fmt.Errorf("task already claimed by %s", step.ClaimedBy)
		}
	}
	if len(step.Assignees) > 0 && !containsString(step.Assignees, actor) {
		return fmt.Errorf("actor %s is not in assignees", actor)
	}
	now := time.Now()
	step.ClaimedBy = actor
	step.ClaimedAt = &now
	if err := e.storage.UpdateExecutionStep(ctx, namespace, step); err != nil {
		return err
	}
	exec, _ := e.storage.GetExecution(ctx, namespace, execID)
	e.emit(ctx, Event{
		Type: EventTypeTaskClaimed, Namespace: namespace, Execution: exec, Step: step,
		Metadata: map[string]interface{}{"actor": actor},
	})
	return nil
}

// ReleaseTask clears the claim on a waiting task.
func (e *Engine) ReleaseTask(ctx context.Context, namespace, execID, nodeID, actor string) error {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "release"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	if err := e.checkTaskAccess(ctx, namespace, nodeID, "release"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	step, err := e.storage.GetExecutionStepByNode(ctx, namespace, execID, nodeID)
	if err != nil {
		return err
	}
	if step.ClaimedBy != "" && step.ClaimedBy != actor {
		return fmt.Errorf("task claimed by %s", step.ClaimedBy)
	}
	step.ClaimedBy = ""
	step.ClaimedAt = nil
	return e.storage.UpdateExecutionStep(ctx, namespace, step)
}

// CompleteTask claims (if needed) and advances a human task.
func (e *Engine) CompleteTask(ctx context.Context, namespace, execID, nodeID, actor, action, payload string) (string, []*models.ExecutionStep, error) {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "complete"); err != nil {
		return "", nil, fmt.Errorf("access denied: %w", err)
	}
	if err := e.checkTaskAccess(ctx, namespace, nodeID, "complete"); err != nil {
		return "", nil, fmt.Errorf("access denied: %w", err)
	}
	step, err := e.storage.GetExecutionStepByNode(ctx, namespace, execID, nodeID)
	if err != nil {
		return "", nil, err
	}
	if step.Status != models.TaskWaiting {
		return "", nil, fmt.Errorf("step is not WAITING")
	}
	if step.ClaimedBy == "" {
		if err := e.ClaimTask(ctx, namespace, execID, nodeID, actor); err != nil {
			return "", nil, err
		}
	} else if step.ClaimedBy != actor {
		return "", nil, fmt.Errorf("task claimed by %s", step.ClaimedBy)
	}

	out, waiting, err := e.Advance(ctx, namespace, execID, nodeID, action, payload)
	if err != nil {
		return "", nil, err
	}
	exec, _ := e.storage.GetExecution(ctx, namespace, execID)
	step, _ = e.storage.GetExecutionStepByNode(ctx, namespace, execID, nodeID)
	e.emit(ctx, Event{
		Type: EventTypeTaskCompleted, Namespace: namespace, Execution: exec, Step: step,
		Metadata: map[string]interface{}{"actor": actor, "action": action},
	})
	return out, waiting, nil
}

// GetVar reads a durable execution variable.
func (e *Engine) GetVar(ctx context.Context, namespace, execID, key string) (interface{}, error) {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return nil, err
	}
	if exec.Variables == nil {
		return nil, nil
	}
	return exec.Variables[key], nil
}

// SetVar sets a durable execution variable.
func (e *Engine) SetVar(ctx context.Context, namespace, execID, key string, value interface{}) error {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return err
	}
	if exec.Variables == nil {
		exec.Variables = make(map[string]interface{})
	}
	exec.Variables[key] = value
	return e.storage.UpdateExecution(ctx, namespace, exec)
}

// MergeVars merges a map into durable execution variables.
func (e *Engine) MergeVars(ctx context.Context, namespace, execID string, vars map[string]interface{}) error {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return err
	}
	if exec.Variables == nil {
		exec.Variables = make(map[string]interface{})
	}
	for k, v := range vars {
		exec.Variables[k] = v
	}
	return e.storage.UpdateExecution(ctx, namespace, exec)
}

// GetVars returns all durable variables.
func (e *Engine) GetVars(ctx context.Context, namespace, execID string) (map[string]interface{}, error) {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return nil, err
	}
	if exec.Variables == nil {
		return map[string]interface{}{}, nil
	}
	return exec.Variables, nil
}

// RollbackTo records a journal entry and enqueues compensation for the target node's CompensateNodeID (or the node itself if it is a compensate node).
func (e *Engine) RollbackTo(ctx context.Context, namespace, execID, nodeID, reason, actor string) error {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "rollback"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return err
	}
	wf, err := e.storage.GetWorkflow(ctx, namespace, exec.WorkflowID)
	if err != nil {
		return err
	}
	n := FindNode(wf, nodeID)
	if n == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	step, _ := e.storage.GetExecutionStepByNode(ctx, namespace, execID, nodeID)

	compensateID := n.CompensateNodeID
	if compensateID == "" {
		compensateID = nodeID // host may point at compensate node directly
	}

	rec := &models.CompensationRecord{
		ID:               uuid.New().String(),
		Namespace:        namespace,
		ExecutionID:      execID,
		FailedNodeID:     nodeID,
		CompensateNodeID: compensateID,
		Reason:           reason,
		Actor:            actor,
		CreatedAt:        time.Now(),
	}
	if step != nil {
		rec.InputSnapshot = step.Input
		rec.OutputSnapshot = step.Output
	}

	e.emit(ctx, Event{
		Type: EventTypeRollbackStarted, Namespace: namespace, Execution: exec, Step: step, Workflow: wf,
		Metadata: map[string]interface{}{"reason": reason, "actor": actor, "nodeId": nodeID},
	})

	if err := e.storage.AppendCompensationRecord(ctx, namespace, rec); err != nil {
		return err
	}

	compStep := &models.ExecutionStep{
		ID:          uuid.New().String(),
		Namespace:   namespace,
		ExecutionID: execID,
		NodeID:      compensateID,
		Status:      models.TaskPending,
		Input:       fmt.Sprintf(`{"reason":%q,"actor":%q,"failedNode":%q}`, reason, actor, nodeID),
		StartedAt:   time.Now(),
	}
	if err := e.storage.CreateExecutionStep(ctx, namespace, compStep); err != nil {
		return err
	}
	e.emit(ctx, Event{
		Type: EventTypeCompensationTriggered, Namespace: namespace, Execution: exec, Step: compStep, Workflow: wf,
	})
	return nil
}

// ListCompensationJournal returns compensation/rollback records for an execution.
func (e *Engine) ListCompensationJournal(ctx context.Context, namespace, execID string) ([]*models.CompensationRecord, error) {
	return e.storage.ListCompensationRecords(ctx, namespace, execID)
}

// mergeVarsIntoInput overlays durable variables onto step output JSON for next inputs.
func (e *Engine) mergeVarsIntoInput(ctx context.Context, namespace, execID, base string) string {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil || exec.Variables == nil || len(exec.Variables) == 0 {
		return base
	}
	out := map[string]interface{}{}
	if base != "" {
		_ = json.Unmarshal([]byte(base), &out)
	}
	for k, v := range exec.Variables {
		if _, exists := out[k]; !exists {
			out[k] = v
		}
	}
	b, _ := json.Marshal(out)
	return string(b)
}
