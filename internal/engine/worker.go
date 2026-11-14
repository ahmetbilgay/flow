package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
)

// PermanentError can be implemented by executor errors that must not be
// retried (e.g. a blocked destination or invalid input). The step fails
// immediately instead of going through the retry policy.
type PermanentError interface {
	error
	Permanent() bool
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p PermanentError
	return errors.As(err, &p) && p.Permanent()
}

// WorkerOptions tunes a worker.
type WorkerOptions struct {
	// Concurrency is the number of steps processed in parallel (default 1).
	Concurrency int
	// PollInterval is how long an idle worker waits before polling storage
	// again (default 100ms). A busy worker claims the next step immediately.
	PollInterval time.Duration
}

type Worker struct {
	id          string
	namespace   string
	engine      *Engine
	registry    *Registry
	interval    time.Duration
	concurrency int
}

func NewWorker(id string, engine *Engine, registry *Registry, interval time.Duration) *Worker {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	return &Worker{
		id:          id,
		engine:      engine,
		registry:    registry,
		interval:    interval,
		concurrency: 1,
	}
}

// SetNamespace pins the worker to a specific namespace
func (w *Worker) SetNamespace(namespace string) {
	w.namespace = namespace
}

// SetConcurrency sets how many steps the worker runs in parallel (min 1).
func (w *Worker) SetConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	w.concurrency = n
}

// Start runs the worker until ctx is cancelled. On shutdown it stops claiming
// new steps, cancels in-flight executors, releases their steps back to the
// queue (without consuming a retry attempt) and returns once they are done.
func (w *Worker) Start(ctx context.Context) {
	w.engine.logger.Info("Worker started", "id", w.id, "namespace", w.namespace, "concurrency", w.concurrency)
	GetTelemetry().WorkerStarted(w.id)
	defer GetTelemetry().WorkerStopped(w.id)

	var wg sync.WaitGroup
	defer wg.Wait()

	// Automatically fail signal-waiting tasks that exceed timeout in the background
	wg.Add(1)
	go func() {
		defer wg.Done()
		reapTicker := time.NewTicker(30 * time.Second)
		defer reapTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reapTicker.C:
				if err := w.engine.ReapTimeouts(ctx, w.namespace); err != nil && ctx.Err() == nil {
					w.engine.logger.Error("Timeout reaper failed", "worker", w.id, "error", err)
				}
			}
		}
	}()

	slots := make(chan struct{}, w.concurrency)
	idle := time.NewTimer(0)
	defer idle.Stop()
	for {
		// Wait for a free slot.
		select {
		case <-ctx.Done():
			w.engine.logger.Info("Worker received shutdown signal", "id", w.id)
			return
		case slots <- struct{}{}:
		}

		step, err := w.engine.storage.ClaimReadyStep(ctx, w.namespace, w.id)
		if err != nil && ctx.Err() == nil {
			w.engine.logger.Error("Failed to claim step", "id", w.id, "error", err)
		}
		if step == nil {
			<-slots
			// Nothing to do: back off before polling again.
			idle.Reset(w.interval)
			select {
			case <-ctx.Done():
				w.engine.logger.Info("Worker received shutdown signal", "id", w.id)
				return
			case <-idle.C:
			}
			continue
		}

		wg.Add(1)
		go func(step *models.ExecutionStep) {
			defer wg.Done()
			defer func() { <-slots }()
			w.process(ctx, step)
		}(step)
	}
}

// processOne claims and processes at most one step synchronously.
func (w *Worker) processOne(ctx context.Context) {
	step, err := w.engine.storage.ClaimReadyStep(ctx, w.namespace, w.id)
	if err != nil {
		w.engine.logger.Error("Failed to claim step", "id", w.id, "error", err)
		return
	}
	if step != nil {
		w.process(ctx, step)
	}
}

// process executes a claimed step and records the outcome. Bookkeeping uses a
// context detached from shutdown so results are persisted even while the
// worker is stopping.
func (w *Worker) process(runCtx context.Context, step *models.ExecutionStep) {
	ctx := context.WithoutCancel(runCtx)

	defer func() {
		if r := recover(); r != nil {
			w.engine.logger.Error("Worker panic recovered", "worker", w.id, "panic", r)
			step.Status = models.TaskFailed
			step.Error = fmt.Sprintf("Panic: %v", r)
			now := time.Now()
			step.FinishedAt = &now
			_ = w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step)
			_ = w.engine.failExecution(ctx, step.Namespace, step.ExecutionID, step.Error)
		}
	}()

	exec, err := w.engine.storage.GetExecution(ctx, step.Namespace, step.ExecutionID)
	if err != nil {
		w.failStepHard(ctx, step, fmt.Errorf("load execution: %w", err))
		return
	}
	if exec.Status == models.TaskCancelled {
		w.engine.logger.Warn("Step skipped due to cancellation", "worker", w.id, "step", step.ID, "execution", exec.ID)
		step.Status = models.TaskCancelled
		now := time.Now()
		step.FinishedAt = &now
		if err := w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step); err != nil {
			w.engine.logger.Error("Failed to update execution step for cancellation", "worker", w.id, "step", step.ID, "error", err)
		}
		return
	}
	wf, err := w.engine.storage.GetWorkflow(ctx, step.Namespace, exec.WorkflowID)
	if err != nil {
		w.failStepHard(ctx, step, fmt.Errorf("load workflow: %w", err))
		return
	}

	w.engine.logger.Info("Processing step", "worker", w.id, "step", step.ID, "node", step.NodeID, "namespace", step.Namespace)

	stopHeartbeat := w.startHeartbeat(ctx, step)
	start := time.Now()
	output, err := w.executeStep(runCtx, step, exec, wf)
	duration := time.Since(start)
	stopHeartbeat()

	nodeType := "unknown"
	if n := FindNode(wf, step.NodeID); n != nil {
		nodeType = n.Type
	}

	if err != nil && runCtx.Err() != nil {
		// Worker is shutting down: hand the step back without using a retry.
		w.engine.logger.Info("Releasing step on shutdown", "worker", w.id, "step", step.ID)
		step.Status = models.TaskPending
		step.WorkerID = ""
		step.ScheduledAt = nil
		if uerr := w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step); uerr != nil {
			w.engine.logger.Error("Failed to release step", "worker", w.id, "step", step.ID, "error", uerr)
		}
		return
	}

	if err != nil {
		// Native Signal / Human wait
		if err.Error() == WaitSignal || err.Error() == WaitHuman {
			waitReason := err.Error()
			w.engine.logger.Info("Step entering WAITING state", "worker", w.id, "step", step.ID, "node", step.NodeID, "reason", waitReason)
			step.Status = models.TaskWaiting
			ApplyHumanTaskConfig(step, FindNode(wf, step.NodeID))
			if updErr := w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step); updErr != nil {
				w.engine.logger.Error("Failed to update execution step to WAITING state", "worker", w.id, "step", step.ID, "error", updErr)
			}
			evType := EventTypeStepWaitingSignal
			if waitReason == WaitHuman {
				evType = EventTypeStepWaitingHuman
			}
			w.engine.emit(ctx, Event{Type: evType, Namespace: step.Namespace, Execution: exec, Step: step, Workflow: wf})
			GetTelemetry().IncProcessed(step.Namespace, nodeType, "waiting")
			return
		}

		w.engine.logger.Error("Step execution failed", "worker", w.id, "step", step.ID, "attempt", step.AttemptNumber, "error", err)

		policy := w.getRetryPolicy(wf, step.NodeID)
		permanent := IsPermanent(err)

		if !permanent && step.AttemptNumber < policy.MaxAttempts {
			step.AttemptNumber++
			delay := w.calculateBackoff(policy, step.AttemptNumber)

			nextScheduled := time.Now().Add(delay)
			step.ScheduledAt = &nextScheduled
			step.Status = models.TaskPending
			step.Error = err.Error()

			w.engine.logger.Info("Rescheduling step with dynamic backoff", "worker", w.id, "step", step.ID, "next_run", nextScheduled.Format(time.RFC3339))
			GetTelemetry().IncProcessed(step.Namespace, nodeType, "retried")
			if err := w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step); err != nil {
				w.engine.logger.Error("Failed to update execution step for retry", "worker", w.id, "step", step.ID, "error", err)
			}
			return
		}

		// Hard Fail
		step.Status = models.TaskFailed
		if permanent {
			step.Error = err.Error()
		} else {
			step.Error = fmt.Sprintf("Max retries reached: %v", err)
		}
		now := time.Now()
		step.FinishedAt = &now
		GetTelemetry().IncProcessed(step.Namespace, nodeType, "failed")
		GetTelemetry().ObserveDuration(step.Namespace, nodeType, duration)
		if err := w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step); err != nil {
			w.engine.logger.Error("Failed to update execution step after max retries", "worker", w.id, "step", step.ID, "error", err)
		}

		// Check if compensation exists - if yes, don't fail execution yet
		hasCompensation := w.handleCompensation(ctx, wf, step, exec, err)
		if !hasCompensation {
			if err := w.engine.checkAndFinishExecution(ctx, exec); err != nil {
				w.engine.logger.Error("Failed to check and finish execution", "worker", w.id, "execution", exec.ID, "error", err)
			}
		}
		return
	}

	GetTelemetry().IncProcessed(step.Namespace, nodeType, "completed")
	GetTelemetry().ObserveDuration(step.Namespace, nodeType, duration)
	if err := w.engine.CompleteStep(ctx, step, output); err != nil {
		w.engine.logger.Error("Failed to complete step", "worker", w.id, "step", step.ID, "error", err)
	}
}

func (w *Worker) getRetryPolicy(wf *models.Workflow, nodeID string) *models.RetryPolicy {
	for _, n := range wf.Nodes {
		if n.ID == nodeID {
			if n.RetryPolicy != nil {
				return n.RetryPolicy
			}
			break
		}
	}
	return &models.RetryPolicy{
		MaxAttempts:        3,
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    5 * time.Minute,
	}
}

// calculateBackoff returns the delay before retry number `attempt` (1-based):
// InitialInterval * BackoffCoefficient^(attempt-1), capped at MaximumInterval.
func (w *Worker) calculateBackoff(policy *models.RetryPolicy, attempt int) time.Duration {
	coef := policy.BackoffCoefficient
	if coef < 1 {
		coef = 1
	}
	delay := float64(policy.InitialInterval)
	for i := 1; i < attempt; i++ {
		delay *= coef
		if policy.MaximumInterval > 0 && delay >= float64(policy.MaximumInterval) {
			break
		}
	}
	d := time.Duration(delay)
	if policy.MaximumInterval > 0 && d > policy.MaximumInterval {
		d = policy.MaximumInterval
	}
	return d
}

// heartbeatInterval is how often a running step's lease is renewed.
var heartbeatInterval = 30 * time.Second

// startHeartbeat renews the step lease until the returned stop func is called,
// so long-running steps are not reclaimed and executed twice by another worker.
func (w *Worker) startHeartbeat(ctx context.Context, step *models.ExecutionStep) func() {
	hb, ok := w.engine.storage.(storage.StepHeartbeater)
	if !ok {
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(heartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				owned, err := hb.HeartbeatStep(ctx, step.Namespace, step.ID, w.id)
				if err != nil {
					w.engine.logger.Warn("Step heartbeat failed", "worker", w.id, "step", step.ID, "error", err)
				} else if !owned {
					w.engine.logger.Warn("Step lease lost", "worker", w.id, "step", step.ID)
				}
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// failStepHard marks a claimed step FAILED when it cannot even be executed.
func (w *Worker) failStepHard(ctx context.Context, step *models.ExecutionStep, err error) {
	w.engine.logger.Error("Step cannot be executed", "worker", w.id, "step", step.ID, "error", err)
	step.Status = models.TaskFailed
	step.Error = err.Error()
	now := time.Now()
	step.FinishedAt = &now
	if uerr := w.engine.storage.UpdateExecutionStep(ctx, step.Namespace, step); uerr != nil {
		w.engine.logger.Error("Failed to update step", "worker", w.id, "step", step.ID, "error", uerr)
	}
	_ = w.engine.failExecution(ctx, step.Namespace, step.ExecutionID, step.Error)
}

func (w *Worker) handleCompensation(ctx context.Context, wf *models.Workflow, step *models.ExecutionStep, exec *models.Execution, err error) bool {
	for _, n := range wf.Nodes {
		if n.ID == step.NodeID && n.CompensateNodeID != "" {
			w.engine.logger.Info("Triggering Saga Compensation", "execution", exec.ID, "failed_node", n.ID, "compensation_node", n.CompensateNodeID)

			rec := &models.CompensationRecord{
				ID:               derivedID("journal", step.ID),
				Namespace:        step.Namespace,
				ExecutionID:      step.ExecutionID,
				FailedNodeID:     n.ID,
				CompensateNodeID: n.CompensateNodeID,
				Reason:           err.Error(),
				Actor:            "system",
				InputSnapshot:    step.Input,
				OutputSnapshot:   step.Output,
				CreatedAt:        time.Now(),
			}
			_ = w.engine.storage.AppendCompensationRecord(ctx, step.Namespace, rec)
			w.engine.emit(ctx, Event{
				Type: EventTypeCompensationTriggered, Namespace: step.Namespace, Execution: exec, Step: step, Workflow: wf,
				Metadata: map[string]interface{}{"compensateNodeId": n.CompensateNodeID},
			})

			compStep := &models.ExecutionStep{
				ID:          derivedID("compensation", step.ID),
				Namespace:   step.Namespace,
				ExecutionID: step.ExecutionID,
				NodeID:      n.CompensateNodeID,
				Status:      models.TaskPending,
				Input:       compensationInput(n.ID, err),
				StartedAt:   time.Now(),
			}
			if _, err := w.engine.createStep(ctx, step.Namespace, compStep); err != nil {
				w.engine.logger.Error("Failed to create compensation step", "execution", exec.ID, "step", compStep.ID, "error", err)
			}
			return true
		}
	}
	return false
}

func compensationInput(failedNode string, err error) string {
	b, _ := json.Marshal(map[string]interface{}{
		"reason":     "compensation",
		"failedNode": failedNode,
		"error":      err.Error(),
	})
	return string(b)
}

func (w *Worker) executeStep(ctx context.Context, step *models.ExecutionStep, exec *models.Execution, wf *models.Workflow) (string, error) {
	n := FindNode(wf, step.NodeID)
	if n == nil {
		return "", fmt.Errorf("node %s not found in workflow", step.NodeID)
	}
	executor, err := w.registry.Get(n.Type)
	if err != nil {
		return "", err
	}

	ctx = WithExecutionContext(ctx, step.Namespace, step.ExecutionID)
	ctx = WithEngine(ctx, w.engine)
	w.engine.emit(ctx, Event{Type: EventTypeStepStarted, Namespace: step.Namespace, Execution: exec, Step: step, Workflow: wf})

	timeout, err := stepTimeout(n)
	if err != nil {
		return "", err
	}
	if timeout <= 0 {
		return executor.Execute(ctx, n.Config, step.Input)
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := executor.Execute(execCtx, n.Config, step.Input)
	if err != nil && errors.Is(execCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		return "", fmt.Errorf("step timed out after %s: %w", timeout, err)
	}
	return out, err
}

// stepTimeout reads the optional "execution_timeout" node config (Go
// duration string, e.g. "30s"). Zero means no timeout.
func stepTimeout(n *models.Node) (time.Duration, error) {
	raw, ok := n.Config["execution_timeout"]
	if !ok {
		return 0, nil
	}
	s, ok := raw.(string)
	if !ok {
		return 0, invalidConfigError(fmt.Sprintf("node %s: execution_timeout must be a duration string", n.ID))
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, invalidConfigError(fmt.Sprintf("node %s: invalid execution_timeout %q", n.ID, s))
	}
	return d, nil
}

type invalidConfigError string

func (e invalidConfigError) Error() string   { return string(e) }
func (e invalidConfigError) Permanent() bool { return true }
