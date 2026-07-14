package storage

import (
	"context"

	"github.com/dgmos/flow/internal/models"
)

// TaskFilter filters waiting / human task steps.
type TaskFilter struct {
	ExecutionID string
	Assignee    string
	NodeID      string
}

type Storage interface {
	// Workflow operations
	SaveWorkflow(ctx context.Context, namespace string, wf *models.Workflow) error
	GetWorkflow(ctx context.Context, namespace string, id string) (*models.Workflow, error)
	ListWorkflows(ctx context.Context, namespace string) ([]*models.Workflow, error)

	// Execution operations
	CreateExecution(ctx context.Context, namespace string, exec *models.Execution) error
	GetExecution(ctx context.Context, namespace string, id string) (*models.Execution, error)
	UpdateExecution(ctx context.Context, namespace string, exec *models.Execution) error
	ListExecutions(ctx context.Context, namespace string) ([]*models.Execution, error)

	// ExecutionStep operations
	CreateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error
	UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error
	GetExecutionSteps(ctx context.Context, namespace string, executionID string) ([]*models.ExecutionStep, error)

	// Worker operations
	ClaimReadyStep(ctx context.Context, namespace string, workerID string) (*models.ExecutionStep, error)

	// Waiting / human tasks
	ListWaitingSteps(ctx context.Context, namespace string, filter TaskFilter) ([]*models.ExecutionStep, error)
	GetExecutionStepByNode(ctx context.Context, namespace, executionID, nodeID string) (*models.ExecutionStep, error)

	// Compensation journal
	AppendCompensationRecord(ctx context.Context, namespace string, rec *models.CompensationRecord) error
	ListCompensationRecords(ctx context.Context, namespace, executionID string) ([]*models.CompensationRecord, error)
}
