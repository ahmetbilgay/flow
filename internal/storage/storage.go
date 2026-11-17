package storage

import (
	"context"
	"errors"
	"time"

	"github.com/dgmos/flow/internal/models"
)

// ErrStepExists is returned by CreateExecutionStep when a step with the same ID
// already exists. The engine uses deterministic step IDs for DAG nodes, so this
// error means another goroutine/worker already scheduled the node and the
// caller should treat creation as a no-op.
var ErrStepExists = errors.New("execution step already exists")

// StaleStepThreshold is how long a RUNNING step may go without a heartbeat
// before another worker may reclaim it. Workers heartbeat well within this.
const StaleStepThreshold = 5 * time.Minute

// StepHeartbeater is optionally implemented by storages that support worker
// leases. Workers call HeartbeatStep periodically while a step runs so that
// long-running steps are not reclaimed and executed twice. It returns
// (false, nil) when the step is no longer owned by workerID.
type StepHeartbeater interface {
	HeartbeatStep(ctx context.Context, namespace, stepID, workerID string) (bool, error)
}

// ConditionalUpdater is implemented by storages that support compare-and-set
// writes (all built-in backends). The engine uses it for every state
// transition so concurrent actors (two resume requests, a reclaimed zombie
// worker, the timeout reaper) cannot overwrite each other's results.
type ConditionalUpdater interface {
	// UpdateExecutionStepIf writes step only if the stored step currently has
	// status from and, when worker is not empty, is owned by worker. It
	// returns false (and no error) when the condition does not hold.
	UpdateExecutionStepIf(ctx context.Context, namespace string, step *models.ExecutionStep, from models.TaskStatus, worker string) (bool, error)
	// UpdateExecutionIf writes exec only if the stored status is from.
	UpdateExecutionIf(ctx context.Context, namespace string, exec *models.Execution, from models.TaskStatus) (bool, error)
}

// TaskFilter filters waiting / human task steps.
type TaskFilter struct {
	ExecutionID string
	Assignee    string
	NodeID      string
}

// ScheduleFilter filters and paginates schedule listings.
type ScheduleFilter struct {
	ActiveOnly bool
	Limit      int // 0 = no limit
	Offset     int
}

type Storage interface {
	SaveWorkflow(ctx context.Context, namespace string, wf *models.Workflow) error
	GetWorkflow(ctx context.Context, namespace string, id string) (*models.Workflow, error)
	ListWorkflows(ctx context.Context, namespace string) ([]*models.Workflow, error)

	CreateExecution(ctx context.Context, namespace string, exec *models.Execution) error
	GetExecution(ctx context.Context, namespace string, id string) (*models.Execution, error)
	UpdateExecution(ctx context.Context, namespace string, exec *models.Execution) error
	ListExecutions(ctx context.Context, namespace string) ([]*models.Execution, error)

	CreateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error
	UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error
	GetExecutionSteps(ctx context.Context, namespace string, executionID string) ([]*models.ExecutionStep, error)

	ClaimReadyStep(ctx context.Context, namespace string, workerID string) (*models.ExecutionStep, error)

	ListWaitingSteps(ctx context.Context, namespace string, filter TaskFilter) ([]*models.ExecutionStep, error)
	GetExecutionStepByNode(ctx context.Context, namespace, executionID, nodeID string) (*models.ExecutionStep, error)

	AppendCompensationRecord(ctx context.Context, namespace string, rec *models.CompensationRecord) error
	ListCompensationRecords(ctx context.Context, namespace, executionID string) ([]*models.CompensationRecord, error)

	SaveSchedule(ctx context.Context, s *models.Schedule) error
	DeleteSchedule(ctx context.Context, namespace, id string) error
	GetActiveSchedules(ctx context.Context) ([]*models.Schedule, error)
	GetSchedule(ctx context.Context, namespace, id string) (*models.Schedule, error)
	ListSchedules(ctx context.Context, namespace string, filter ScheduleFilter) ([]*models.Schedule, error)
	TryClaimScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time) (bool, error)
	RecordScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time, execID, lastErr string, nextRun *time.Time) error
}
