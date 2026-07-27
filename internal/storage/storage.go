package storage

import (
	"context"
	"time"

	"github.com/dgmos/flow/internal/models"
)

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
