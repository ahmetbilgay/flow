package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
)

type MemoryStorage struct {
	mu                  sync.RWMutex
	workflows           map[string]*models.Workflow
	executions          map[string]*models.Execution
	executionSteps      map[string]*models.ExecutionStep
	compensationJournal []*models.CompensationRecord
	schedules           map[string]*models.Schedule
	ZombieThreshold     time.Duration
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		workflows:           make(map[string]*models.Workflow),
		executions:          make(map[string]*models.Execution),
		executionSteps:      make(map[string]*models.ExecutionStep),
		compensationJournal: make([]*models.CompensationRecord, 0),
		schedules:           make(map[string]*models.Schedule),
		ZombieThreshold:     5 * time.Minute,
	}
}

func (s *MemoryStorage) SaveWorkflow(ctx context.Context, namespace string, wf *models.Workflow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	wf.Namespace = namespace
	key := fmt.Sprintf("%s:%s", namespace, wf.ID)
	s.workflows[key] = wf
	return nil
}

func (s *MemoryStorage) GetWorkflow(ctx context.Context, namespace string, id string) (*models.Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", namespace, id)
	wf, ok := s.workflows[key]
	if !ok {
		return nil, fmt.Errorf("workflow not found in namespace %s", namespace)
	}
	copy := *wf
	if wf.Nodes != nil {
		copy.Nodes = append([]models.Node(nil), wf.Nodes...)
	}
	if wf.Edges != nil {
		copy.Edges = append([]models.Edge(nil), wf.Edges...)
	}
	return &copy, nil
}

func (s *MemoryStorage) ListWorkflows(ctx context.Context, namespace string) ([]*models.Workflow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var wfs []*models.Workflow
	for _, wf := range s.workflows {
		if wf.Namespace == namespace {
			wfs = append(wfs, wf)
		}
	}
	return wfs, nil
}

func (s *MemoryStorage) CreateExecution(ctx context.Context, namespace string, exec *models.Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec.Namespace = namespace
	if exec.Variables == nil {
		exec.Variables = make(map[string]interface{})
	}
	key := fmt.Sprintf("%s:%s", namespace, exec.ID)
	execCopy := *exec
	s.executions[key] = &execCopy
	return nil
}

func (s *MemoryStorage) GetExecution(ctx context.Context, namespace string, id string) (*models.Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", namespace, id)
	exec, ok := s.executions[key]
	if !ok {
		return nil, fmt.Errorf("execution not found in namespace %s", namespace)
	}
	execCopy := *exec
	if exec.Variables != nil {
		varsCopy := make(map[string]interface{}, len(exec.Variables))
		for k, v := range exec.Variables {
			varsCopy[k] = v
		}
		execCopy.Variables = varsCopy
	}
	return &execCopy, nil
}

func (s *MemoryStorage) UpdateExecution(ctx context.Context, namespace string, exec *models.Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s:%s", namespace, exec.ID)
	execCopy := *exec
	s.executions[key] = &execCopy
	return nil
}

func (s *MemoryStorage) ListExecutions(ctx context.Context, namespace string) ([]*models.Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var execs []*models.Execution
	for _, exec := range s.executions {
		if exec.Namespace == namespace {
			execs = append(execs, exec)
		}
	}
	return execs, nil
}

func (s *MemoryStorage) CreateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	step.Namespace = namespace
	step.UpdatedAt = time.Now()
	key := fmt.Sprintf("%s:%s", namespace, step.ID)
	if _, exists := s.executionSteps[key]; exists {
		return storage.ErrStepExists
	}
	stepCopy := *step
	s.executionSteps[key] = &stepCopy
	return nil
}

func (s *MemoryStorage) UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s:%s", namespace, step.ID)
	step.UpdatedAt = time.Now()
	// Store a copy so callers can keep mutating their pointer without racing
	// with readers that hold the storage lock.
	stepCopy := *step
	s.executionSteps[key] = &stepCopy
	return nil
}

// HeartbeatStep extends the lease of a RUNNING step owned by workerID.
func (s *MemoryStorage) HeartbeatStep(ctx context.Context, namespace, stepID, workerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	step, ok := s.executionSteps[fmt.Sprintf("%s:%s", namespace, stepID)]
	if !ok || step.Status != models.TaskRunning || step.WorkerID != workerID {
		return false, nil
	}
	step.UpdatedAt = time.Now()
	return true, nil
}

func (s *MemoryStorage) GetExecutionSteps(ctx context.Context, namespace string, executionID string) ([]*models.ExecutionStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var steps []*models.ExecutionStep
	for _, step := range s.executionSteps {
		if step.ExecutionID == executionID && step.Namespace == namespace {
			stepCopy := *step
			steps = append(steps, &stepCopy)
		}
	}
	return steps, nil
}

func (s *MemoryStorage) ClaimReadyStep(ctx context.Context, namespace string, workerID string) (*models.ExecutionStep, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, step := range s.executionSteps {
		isZombie := step.Status == models.TaskRunning && s.ZombieThreshold > 0 && time.Since(step.UpdatedAt) > s.ZombieThreshold

		if step.Status == models.TaskPending || isZombie {
			if namespace == "" || step.Namespace == namespace {
				if step.Status == models.TaskPending && (step.ScheduledAt != nil && step.ScheduledAt.After(now)) {
					continue
				}

				step.Status = models.TaskRunning
				step.WorkerID = workerID
				step.UpdatedAt = now
				claimed := *step
				return &claimed, nil
			}
		}
	}
	return nil, nil
}

func (s *MemoryStorage) ListWaitingSteps(ctx context.Context, namespace string, filter storage.TaskFilter) ([]*models.ExecutionStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*models.ExecutionStep
	for _, step := range s.executionSteps {
		if step.Namespace != namespace || step.Status != models.TaskWaiting {
			continue
		}
		if filter.ExecutionID != "" && step.ExecutionID != filter.ExecutionID {
			continue
		}
		if filter.NodeID != "" && step.NodeID != filter.NodeID {
			continue
		}
		if filter.Assignee != "" {
			found := false
			for _, a := range step.Assignees {
				if a == filter.Assignee {
					found = true
					break
				}
			}
			if !found && step.ClaimedBy != filter.Assignee {
				continue
			}
		}
		copy := *step
		out = append(out, &copy)
	}
	return out, nil
}

func (s *MemoryStorage) GetExecutionStepByNode(ctx context.Context, namespace, executionID, nodeID string) (*models.ExecutionStep, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest *models.ExecutionStep
	for _, step := range s.executionSteps {
		if step.Namespace == namespace && step.ExecutionID == executionID && step.NodeID == nodeID {
			copy := *step
			if latest == nil || copy.StartedAt.After(latest.StartedAt) {
				latest = &copy
			}
		}
	}
	if latest == nil {
		return nil, fmt.Errorf("step not found for node %s", nodeID)
	}
	return latest, nil
}

func (s *MemoryStorage) AppendCompensationRecord(ctx context.Context, namespace string, rec *models.CompensationRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.Namespace = namespace
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	copy := *rec
	s.compensationJournal = append(s.compensationJournal, &copy)
	return nil
}

func (s *MemoryStorage) ListCompensationRecords(ctx context.Context, namespace, executionID string) ([]*models.CompensationRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*models.CompensationRecord
	for _, r := range s.compensationJournal {
		if r.Namespace == namespace && r.ExecutionID == executionID {
			copy := *r
			out = append(out, &copy)
		}
	}
	return out, nil
}

func scheduleKey(namespace, id string) string {
	return fmt.Sprintf("%s:%s", namespace, id)
}

func (s *MemoryStorage) SaveSchedule(ctx context.Context, sched *models.Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if sched.CreatedAt.IsZero() {
		sched.CreatedAt = now
	}
	sched.UpdatedAt = now
	if sched.Timezone == "" {
		sched.Timezone = "UTC"
	}
	s.schedules[scheduleKey(sched.Namespace, sched.ID)] = cloneSchedule(sched)
	return nil
}

func (s *MemoryStorage) DeleteSchedule(ctx context.Context, namespace, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scheduleKey(namespace, id)
	if _, ok := s.schedules[key]; !ok {
		return fmt.Errorf("schedule not found: namespace=%s, id=%s", namespace, id)
	}
	delete(s.schedules, key)
	return nil
}

func (s *MemoryStorage) GetActiveSchedules(ctx context.Context) ([]*models.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	var out []*models.Schedule
	for _, sched := range s.schedules {
		if !sched.IsActive {
			continue
		}
		if sched.EndsAt != nil && !sched.EndsAt.After(now) {
			continue
		}
		out = append(out, cloneSchedule(sched))
	}
	return out, nil
}

func (s *MemoryStorage) GetSchedule(ctx context.Context, namespace, id string) (*models.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sched, ok := s.schedules[scheduleKey(namespace, id)]
	if !ok {
		return nil, fmt.Errorf("schedule not found")
	}
	return cloneSchedule(sched), nil
}

func (s *MemoryStorage) ListSchedules(ctx context.Context, namespace string, filter storage.ScheduleFilter) ([]*models.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*models.Schedule
	for _, sched := range s.schedules {
		if sched.Namespace != namespace {
			continue
		}
		if filter.ActiveOnly && !sched.IsActive {
			continue
		}
		out = append(out, cloneSchedule(sched))
	}
	if filter.Offset > 0 {
		if filter.Offset >= len(out) {
			return []*models.Schedule{}, nil
		}
		out = out[filter.Offset:]
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *MemoryStorage) TryClaimScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fireAt = fireAt.UTC().Truncate(time.Second)
	key := scheduleKey(namespace, id)
	sched, ok := s.schedules[key]
	if !ok || !sched.IsActive {
		return false, nil
	}
	if sched.LastRunAt != nil && !sched.LastRunAt.Before(fireAt) {
		return false, nil
	}
	t := fireAt
	sched.LastRunAt = &t
	sched.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (s *MemoryStorage) RecordScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time, execID, lastErr string, nextRun *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scheduleKey(namespace, id)
	sched, ok := s.schedules[key]
	if !ok {
		return fmt.Errorf("schedule not found")
	}
	t := fireAt.UTC().Truncate(time.Second)
	sched.LastRunAt = &t
	sched.LastExecID = execID
	sched.LastError = lastErr
	if nextRun != nil {
		n := nextRun.UTC()
		sched.NextRunAt = &n
	} else {
		sched.NextRunAt = nil
	}
	sched.UpdatedAt = time.Now().UTC()
	return nil
}

func cloneSchedule(sched *models.Schedule) *models.Schedule {
	copy := *sched
	if sched.LastRunAt != nil {
		t := *sched.LastRunAt
		copy.LastRunAt = &t
	}
	if sched.NextRunAt != nil {
		t := *sched.NextRunAt
		copy.NextRunAt = &t
	}
	if sched.StartsAt != nil {
		t := *sched.StartsAt
		copy.StartsAt = &t
	}
	if sched.EndsAt != nil {
		t := *sched.EndsAt
		copy.EndsAt = &t
	}
	return &copy
}
