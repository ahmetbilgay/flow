package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dgmos/flow/internal/auth"
	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/dgmos/flow/internal/telemetry"
	"github.com/google/uuid"
)

type Engine struct {
	storage      storage.Storage
	registry     *Registry
	logger       *slog.Logger
	authProvider auth.AuthProvider // Optional - if nil, no auth checks
	eventBus     *EventBus
	hostHooks    HostHooks // sync OnEvent via EmitSync when set
}

func NewEngine(s storage.Storage, r *Registry) *Engine {
	return &Engine{
		storage:  s,
		registry: r,
		logger:   slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		eventBus: NewEventBus(),
	}
}

func (e *Engine) WithLogger(l *slog.Logger) *Engine {
	e.logger = l
	return e
}

// SetAuthProvider sets the auth provider for authorization checks
// If not set, all operations are allowed (no auth)
func (e *Engine) SetAuthProvider(provider auth.AuthProvider) *Engine {
	e.authProvider = provider
	return e
}

// CheckAccess runs the configured AuthProvider (no-op when unset).
func (e *Engine) CheckAccess(ctx context.Context, resource, action string) error {
	if e.authProvider == nil {
		return nil
	}
	return e.authProvider.CheckAccess(ctx, resource, action)
}

func (e *Engine) checkExecutionAccess(ctx context.Context, namespace, execID, action string) error {
	return e.CheckAccess(ctx, fmt.Sprintf("execution:%s:%s", namespace, execID), action)
}

func (e *Engine) checkTaskAccess(ctx context.Context, namespace, nodeID, action string) error {
	return e.CheckAccess(ctx, fmt.Sprintf("task:%s:%s", namespace, nodeID), action)
}

// SetEventBus replaces the engine event bus (async handlers).
func (e *Engine) SetEventBus(bus *EventBus) *Engine {
	if bus != nil {
		e.eventBus = bus
	}
	return e
}

// EventBus returns the engine's event bus.
func (e *Engine) EventBus() *EventBus {
	return e.eventBus
}

// SetHostHooks sets a synchronous host integration handler (WS, notifications).
func (e *Engine) SetHostHooks(h HostHooks) *Engine {
	e.hostHooks = h
	return e
}

func (e *Engine) emit(ctx context.Context, event Event) {
	// Sync emit avoids races on shared Execution/Step pointers during worker updates.
	if e.eventBus != nil {
		_ = e.eventBus.EmitSync(ctx, event)
	}
	if e.hostHooks != nil {
		_ = e.hostHooks.HandleEvent(ctx, event)
	}
}

// StartWorkflow creates a new execution for a workflow
func (e *Engine) StartWorkflow(ctx context.Context, namespace, workflowID string, input string) (string, error) {
	// Check auth if provider is set
	if e.authProvider != nil {
		resource := fmt.Sprintf("workflow:%s:%s", namespace, workflowID)
		if err := e.authProvider.CheckAccess(ctx, resource, "execute"); err != nil {
			return "", fmt.Errorf("access denied: %w", err)
		}
	}

	wf, err := e.storage.GetWorkflow(ctx, namespace, workflowID)
	if err != nil {
		return "", fmt.Errorf("failed to get workflow: %w", err)
	}

	// Validate everything before persisting, so a bad definition does not
	// leave an orphan RUNNING execution behind.
	g, err := NewGraph(wf)
	if err != nil {
		return "", err
	}
	if err := g.Validate(); err != nil {
		return "", err
	}
	for _, n := range wf.Nodes {
		executor, err := e.registry.Get(n.Type)
		if err != nil {
			return "", fmt.Errorf("node %s: %w", n.ID, err)
		}
		if err := executor.Validate(n.Config); err != nil {
			return "", fmt.Errorf("node %s configuration error: %w", n.ID, err)
		}
		if _, err := stepTimeout(&n); err != nil {
			return "", err
		}
	}

	execID := uuid.New().String()
	exec := &models.Execution{
		ID:         execID,
		Namespace:  namespace,
		WorkflowID: workflowID,
		Version:    wf.Version,
		Status:     models.TaskRunning,
		Input:      input,
		Variables:  make(map[string]interface{}),
		StartedAt:  time.Now(),
	}

	if err := e.storage.CreateExecution(ctx, namespace, exec); err != nil {
		return "", fmt.Errorf("failed to create execution: %w", err)
	}

	for _, nodeID := range g.GetInitialNodes() {
		step := NewStepForNode(namespace, execID, nodeID, input, FindNode(wf, nodeID))
		step.ID = forwardStepID(execID, nodeID)
		if _, err := e.createStep(ctx, namespace, step); err != nil {
			return "", err
		}
		if step.Status == models.TaskWaiting {
			e.emit(ctx, Event{
				Type: EventTypeStepWaitingHuman, Namespace: namespace, Execution: exec, Step: step, Workflow: wf,
			})
		}
	}

	telemetry.WorkflowsStarted.WithLabelValues(namespace, workflowID).Inc()
	e.emit(ctx, Event{
		Type: EventTypeWorkflowStarted, Namespace: namespace, Execution: exec, Workflow: wf,
	})

	return execID, nil
}

// ErrStepConflict is returned when a step changed concurrently (e.g. a
// waiting step was already resumed, or a reclaimed step is now owned by
// another worker) and the transition was not applied.
var ErrStepConflict = errors.New("step was changed concurrently")

// updateStepIf applies a compare-and-set step write when the storage
// supports it (all built-in backends do).
func (e *Engine) updateStepIf(ctx context.Context, step *models.ExecutionStep, from models.TaskStatus, worker string) (bool, error) {
	if cu, ok := e.storage.(storage.ConditionalUpdater); ok {
		return cu.UpdateExecutionStepIf(ctx, step.Namespace, step, from, worker)
	}
	return true, e.storage.UpdateExecutionStep(ctx, step.Namespace, step)
}

// updateExecIf applies a compare-and-set execution write.
func (e *Engine) updateExecIf(ctx context.Context, exec *models.Execution, from models.TaskStatus) (bool, error) {
	if cu, ok := e.storage.(storage.ConditionalUpdater); ok {
		return cu.UpdateExecutionIf(ctx, exec.Namespace, exec, from)
	}
	return true, e.storage.UpdateExecution(ctx, exec.Namespace, exec)
}

// CompleteStep handles the transition after a node finishes. The step must
// still be in the state the caller read (WAITING, or RUNNING and owned by
// the same worker); otherwise ErrStepConflict is returned and nothing
// changes. Scheduling continues even if ctx is cancelled after the step was
// persisted, so children are never orphaned by a disconnected caller.
func (e *Engine) CompleteStep(ctx context.Context, step *models.ExecutionStep, output string) error {
	ctx = context.WithoutCancel(ctx)
	from, worker := step.Status, ""
	if from == models.TaskRunning {
		worker = step.WorkerID
	}
	step.Status = models.TaskCompleted
	step.Output = output
	step.Error = ""
	now := time.Now()
	step.FinishedAt = &now

	ok, err := e.updateStepIf(ctx, step, from, worker)
	if err != nil {
		return err
	}
	if !ok {
		return ErrStepConflict
	}

	exec, err := e.storage.GetExecution(ctx, step.Namespace, step.ExecutionID)
	if err != nil {
		return err
	}

	wf, err := e.storage.GetWorkflow(ctx, step.Namespace, exec.WorkflowID)
	if err != nil {
		return err
	}

	e.emit(ctx, Event{
		Type: EventTypeStepCompleted, Namespace: step.Namespace, Execution: exec, Step: step, Workflow: wf,
	})

	g, err := NewGraph(wf)
	if err != nil {
		return err
	}

	if err := e.advanceFrom(ctx, exec, wf, g, step.NodeID); err != nil {
		return err
	}
	return e.checkAndFinishExecution(ctx, exec)
}

func (e *Engine) CancelExecution(ctx context.Context, namespace string, execID string) error {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "cancel"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return err
	}
	if exec.Status != models.TaskRunning {
		return fmt.Errorf("cannot cancel finished execution")
	}
	exec.Status = models.TaskCancelled
	now := time.Now()
	exec.FinishedAt = &now
	ok, err := e.updateExecIf(ctx, exec, models.TaskRunning)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("cannot cancel finished execution")
	}
	e.emit(ctx, Event{Type: EventTypeWorkflowCancelled, Namespace: namespace, Execution: exec})
	return nil
}

func (e *Engine) FailExecution(ctx context.Context, namespace string, execID string, message string) error {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "fail"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	return e.failExecution(ctx, namespace, execID, message)
}

// failExecution marks an execution FAILED without auth checks (engine-internal).
func (e *Engine) failExecution(ctx context.Context, namespace string, execID string, message string) error {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return err
	}
	if exec.Status != models.TaskRunning {
		return nil
	}
	exec.Status = models.TaskFailed
	exec.ErrorMessage = message
	now := time.Now()
	exec.FinishedAt = &now
	ok, err := e.updateExecIf(ctx, exec, models.TaskRunning)
	if err != nil || !ok {
		return err
	}
	e.emit(ctx, Event{Type: EventTypeWorkflowFailed, Namespace: namespace, Execution: exec})
	return nil
}

func (e *Engine) SignalExecution(ctx context.Context, namespace string, execID string, nodeID string, output string) error {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "signal"); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	steps, err := e.storage.GetExecutionSteps(ctx, namespace, execID)
	if err != nil {
		return err
	}

	var targetStep *models.ExecutionStep
	for _, s := range steps {
		if s.NodeID == nodeID && s.Status == models.TaskWaiting {
			targetStep = s
			break
		}
	}

	if targetStep == nil {
		return fmt.Errorf("no WAITING step found for node %s in execution %s", nodeID, execID)
	}

	// Merge signal data with existing input so downstream nodes (like condition) can access it
	mergedOutput := e.mergeSignalData(targetStep.Input, output)

	ctx = context.WithoutCancel(ctx)
	e.logger.Info("Signaling execution resume", "execution", execID, "node", nodeID)
	e.emit(ctx, Event{
		Type: EventTypeSignalReceived, Namespace: namespace,
		Metadata: map[string]interface{}{"nodeId": nodeID, "executionId": execID},
	})
	return e.CompleteStep(ctx, targetStep, mergedOutput)
}

// mergeSignalData combines existing input with signal data for downstream nodes
func (e *Engine) mergeSignalData(existingInput string, signalData string) string {
	// Try to parse signal data as JSON
	var signalMap map[string]interface{}
	signalErr := json.Unmarshal([]byte(signalData), &signalMap)

	// If signal data is not valid JSON or is empty, return it as-is for backwards compatibility
	if signalErr != nil || len(signalMap) == 0 {
		return signalData
	}

	// Parse existing input
	var inputMap map[string]interface{}
	if existingInput != "" {
		_ = json.Unmarshal([]byte(existingInput), &inputMap)
	}
	if inputMap == nil {
		inputMap = make(map[string]interface{})
	}

	// Merge: signal data overwrites existing keys
	for k, v := range signalMap {
		inputMap[k] = v
	}

	// Return merged JSON
	merged, _ := json.Marshal(inputMap)
	return string(merged)
}

// ReapTimeouts checks for WAITING steps that have passed their timeout period.
func (e *Engine) ReapTimeouts(ctx context.Context, namespace string) error {
	waiting, err := e.storage.ListWaitingSteps(ctx, namespace, storage.TaskFilter{})
	if err != nil {
		return err
	}

	execCache := make(map[string]*models.Execution)
	wfCache := make(map[string]*models.Workflow)

	for _, step := range waiting {
		exec, ok := execCache[step.ExecutionID]
		if !ok {
			exec, err = e.storage.GetExecution(ctx, namespace, step.ExecutionID)
			if err != nil {
				continue
			}
			execCache[step.ExecutionID] = exec
		}
		if exec.Status != models.TaskRunning {
			continue
		}
		if step.Action == timerAction {
			if step.DueAt != nil && !time.Now().Before(*step.DueAt) {
				if err := e.CompleteStep(ctx, step, step.Output); err != nil && !errors.Is(err, ErrStepConflict) {
					e.logger.Error("Failed to resume timer step", "step", step.ID, "error", err)
				}
			}
			continue
		}

		wf, ok := wfCache[exec.WorkflowID]
		if !ok {
			wf, err = e.storage.GetWorkflow(ctx, namespace, exec.WorkflowID)
			if err != nil {
				e.logger.Error("Failed to get workflow for reaper", "workflow", exec.WorkflowID, "error", err)
				continue
			}
			wfCache[exec.WorkflowID] = wf
		}

		n := FindNode(wf, step.NodeID)
		if n == nil {
			continue
		}
		timeoutStr, ok := n.Config["timeout"].(string)
		if !ok {
			continue
		}
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil || time.Since(step.UpdatedAt) <= timeout {
			continue
		}

		step.Status = models.TaskFailed
		step.Error = "SIGNAL_TIMEOUT"
		now := time.Now()
		step.FinishedAt = &now
		applied, err := e.updateStepIf(ctx, step, models.TaskWaiting, "")
		if err != nil {
			e.logger.Error("Failed to update timed out step", "step", step.ID, "error", err)
			continue
		}
		if !applied {
			continue // resumed or reaped concurrently
		}
		e.logger.Warn("Step timed out waiting for signal", "step", step.ID, "node", step.NodeID)
		if err := e.checkAndFinishExecution(ctx, exec); err != nil {
			e.logger.Error("Failed to finish execution after timeout", "execution", exec.ID, "error", err)
		}
	}
	return nil
}

// ListExecutions returns all executions in a namespace.
func (e *Engine) ListExecutions(ctx context.Context, namespace string) ([]*models.Execution, error) {
	if err := e.CheckAccess(ctx, fmt.Sprintf("execution:%s:*", namespace), "view"); err != nil {
		return nil, fmt.Errorf("access denied: %w", err)
	}
	return e.storage.ListExecutions(ctx, namespace)
}

func (e *Engine) GetExecutionStatus(ctx context.Context, namespace string, execID string) (*models.Execution, error) {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "view"); err != nil {
		return nil, fmt.Errorf("access denied: %w", err)
	}
	return e.storage.GetExecution(ctx, namespace, execID)
}

func (e *Engine) GetLogger() *slog.Logger {
	return e.logger
}

// RegisterWorkflow saves a workflow definition to storage
func (e *Engine) RegisterWorkflow(ctx context.Context, namespace string, wf *models.Workflow) error {
	// Check auth if provider is set
	if e.authProvider != nil {
		resource := fmt.Sprintf("workflow:%s:%s", namespace, wf.ID)
		if err := e.authProvider.CheckAccess(ctx, resource, "create"); err != nil {
			return fmt.Errorf("access denied: %w", err)
		}
	}

	g, err := NewGraph(wf)
	if err != nil {
		return fmt.Errorf("invalid workflow %s: %w", wf.ID, err)
	}
	if err := g.Validate(); err != nil {
		return fmt.Errorf("invalid workflow %s: %w", wf.ID, err)
	}

	wf.Namespace = namespace
	wf.Status = models.WorkflowActive
	wf.CreatedAt = time.Now()
	wf.UpdatedAt = time.Now()
	return e.storage.SaveWorkflow(ctx, namespace, wf)
}

// Execute is an alias for StartWorkflow (accepts []byte input)
func (e *Engine) Execute(ctx context.Context, namespace, workflowID string, input []byte) (string, error) {
	return e.StartWorkflow(ctx, namespace, workflowID, string(input))
}

// StartWorker starts a worker loop that processes tasks (one at a time).
// It blocks until ctx is cancelled and in-flight work has been released.
func (e *Engine) StartWorker(ctx context.Context, namespace, workerID string) {
	e.StartWorkerWithOptions(ctx, namespace, workerID, WorkerOptions{})
}

// StartWorkerWithOptions starts a worker with custom concurrency/polling.
func (e *Engine) StartWorkerWithOptions(ctx context.Context, namespace, workerID string, opts WorkerOptions) {
	worker := NewWorker(workerID, e, e.registry, opts.PollInterval)
	worker.SetNamespace(namespace)
	worker.SetConcurrency(opts.Concurrency)
	worker.Start(ctx)
}

func (e *Engine) GetExecutionSteps(ctx context.Context, namespace string, execID string) ([]*models.ExecutionStep, error) {
	if err := e.checkExecutionAccess(ctx, namespace, execID, "view"); err != nil {
		return nil, fmt.Errorf("access denied: %w", err)
	}
	return e.storage.GetExecutionSteps(ctx, namespace, execID)
}

// checkAndFinishExecution finalises a RUNNING execution once every step has
// settled. It re-reads the execution so a concurrent cancel (or an earlier
// finish) is never overwritten, and so durable variables are not clobbered.
func (e *Engine) checkAndFinishExecution(ctx context.Context, exec *models.Execution) error {
	current, err := e.storage.GetExecution(ctx, exec.Namespace, exec.ID)
	if err != nil {
		return err
	}
	if current.Status != models.TaskRunning {
		return nil
	}

	steps, err := e.storage.GetExecutionSteps(ctx, exec.Namespace, exec.ID)
	if err != nil {
		return err
	}
	for _, s := range steps {
		if !isTerminal(s.Status) {
			return nil
		}
	}

	// Another worker may have just completed a predecessor and not yet
	// created the downstream step. Resolve every node without a step
	// (idempotent: step IDs are deterministic) so nothing is left behind.
	if wf, err := e.storage.GetWorkflow(ctx, current.Namespace, current.WorkflowID); err == nil {
		if g, err := NewGraph(wf); err == nil {
			byNode := nodeSteps(current.ID, steps)
			created := false
			for _, n := range wf.Nodes {
				if byNode[n.ID] == nil && len(g.Predecessors[n.ID]) > 0 {
					if err := e.resolveNode(ctx, current, wf, g, n.ID); err != nil {
						return err
					}
					created = true
				}
			}
			if created {
				if steps, err = e.storage.GetExecutionSteps(ctx, exec.Namespace, exec.ID); err != nil {
					return err
				}
			}
		}
	}

	var lastCompleted, firstFailed *models.ExecutionStep
	for _, s := range steps {
		switch s.Status {
		case models.TaskCompleted:
			if lastCompleted == nil || finishedAfter(s, lastCompleted) {
				lastCompleted = s
			}
		case models.TaskFailed:
			if firstFailed == nil || finishedAfter(firstFailed, s) {
				firstFailed = s
			}
		case models.TaskSkipped:
		default:
			return nil // still pending/running/waiting
		}
	}

	now := time.Now()
	current.FinishedAt = &now
	if firstFailed != nil {
		current.Status = models.TaskFailed
		current.ErrorMessage = firstFailed.Error
		e.logger.Info("Execution failed", "execution", current.ID, "error", firstFailed.Error)
		telemetry.WorkflowsFailed.WithLabelValues(current.Namespace, current.WorkflowID, firstFailed.NodeID).Inc()
	} else {
		current.Status = models.TaskCompleted
		if lastCompleted != nil {
			current.Output = lastCompleted.Output
		}
		e.logger.Info("Execution completed", "execution", current.ID)
		telemetry.WorkflowsCompleted.WithLabelValues(current.Namespace, current.WorkflowID).Inc()
	}
	ok, err := e.updateExecIf(ctx, current, models.TaskRunning)
	if err != nil {
		return err
	}
	if !ok {
		return nil // finished, failed or cancelled concurrently
	}
	*exec = *current
	evType := EventTypeWorkflowCompleted
	if current.Status == models.TaskFailed {
		evType = EventTypeWorkflowFailed
	}
	e.emit(ctx, Event{Type: evType, Namespace: current.Namespace, Execution: current})
	return nil
}

// finishedAfter reports whether a finished later than b.
func finishedAfter(a, b *models.ExecutionStep) bool {
	at, bt := a.StartedAt, b.StartedAt
	if a.FinishedAt != nil {
		at = *a.FinishedAt
	}
	if b.FinishedAt != nil {
		bt = *b.FinishedAt
	}
	return at.After(bt)
}

// ExecutionSnapshot returns an execution with its workflow and steps, without
// auth checks. It is meant for executors running inside that execution
// (e.g. expression engines resolving other nodes' outputs).
func (e *Engine) ExecutionSnapshot(ctx context.Context, namespace, execID string) (*models.Execution, *models.Workflow, []*models.ExecutionStep, error) {
	exec, err := e.storage.GetExecution(ctx, namespace, execID)
	if err != nil {
		return nil, nil, nil, err
	}
	wf, err := e.storage.GetWorkflow(ctx, namespace, exec.WorkflowID)
	if err != nil {
		return nil, nil, nil, err
	}
	steps, err := e.storage.GetExecutionSteps(ctx, namespace, execID)
	if err != nil {
		return nil, nil, nil, err
	}
	return exec, wf, steps, nil
}

// NodeRegistry returns the executor registry used by this engine.
func (e *Engine) NodeRegistry() *Registry {
	return e.registry
}

// WorkflowDefinition loads a workflow definition without auth checks (for
// executors that run other workflows of the same namespace).
func (e *Engine) WorkflowDefinition(ctx context.Context, namespace, id string) (*models.Workflow, error) {
	return e.storage.GetWorkflow(ctx, namespace, id)
}

func isTerminal(s models.TaskStatus) bool {
	return s == models.TaskCompleted || s == models.TaskFailed || s == models.TaskSkipped
}

// WorkflowDefinitions lists workflow definitions of a namespace without auth
// checks (for trigger routing).
func (e *Engine) WorkflowDefinitions(ctx context.Context, namespace string) ([]*models.Workflow, error) {
	return e.storage.ListWorkflows(ctx, namespace)
}

// SweepStuckExecutions finishes RUNNING executions whose steps have all
// settled but that were never finalized (e.g. a process crashed between
// completing a step and scheduling its children). It returns how many
// executions changed state. Workers call it periodically.
func (e *Engine) SweepStuckExecutions(ctx context.Context, namespace string, olderThan time.Duration) (int, error) {
	execs, err := e.storage.ListExecutions(ctx, namespace)
	if err != nil {
		return 0, err
	}
	changed := 0
	for _, exec := range execs {
		if exec.Status != models.TaskRunning || time.Since(exec.StartedAt) < olderThan {
			continue
		}
		steps, err := e.storage.GetExecutionSteps(ctx, namespace, exec.ID)
		if err != nil {
			continue
		}
		settled := true
		for _, s := range steps {
			if !isTerminal(s.Status) {
				settled = false
				break
			}
		}
		if !settled {
			continue
		}
		before := exec.Status
		if err := e.checkAndFinishExecution(ctx, exec); err != nil {
			e.logger.Error("Sweep failed", "execution", exec.ID, "error", err)
			continue
		}
		if exec.Status != before {
			changed++
		}
	}
	return changed, nil
}
