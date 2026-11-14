package models

import (
	"time"
)

type WorkflowStatus string

const (
	WorkflowActive   WorkflowStatus = "ACTIVE"
	WorkflowInactive WorkflowStatus = "INACTIVE"
)

type TaskStatus string

const (
	TaskPending   TaskStatus = "PENDING"
	TaskRunning   TaskStatus = "RUNNING"
	TaskCompleted TaskStatus = "COMPLETED"
	TaskFailed    TaskStatus = "FAILED"
	TaskSkipped   TaskStatus = "SKIPPED"
	TaskWaiting   TaskStatus = "WAITING"
	TaskCancelled TaskStatus = "CANCELLED"
)

// ClaimMode controls how human tasks accept claims.
type ClaimMode string

const (
	ClaimAny   ClaimMode = "any"
	ClaimAll   ClaimMode = "all"
	ClaimFirst ClaimMode = "first"
)

// Workflow represents the blueprint of a workflow
type Workflow struct {
	ID          string                 `json:"id" db:"id"`
	Namespace   string                 `json:"namespace" db:"namespace"`
	Name        string                 `json:"name" db:"name"`
	Description string                 `json:"description" db:"description"`
	Version     int                    `json:"version" db:"version"`
	Status      WorkflowStatus         `json:"status" db:"status"`
	Nodes       []Node                 `json:"nodes" db:"-"`
	Edges       []Edge                 `json:"edges" db:"-"`
	Labels      map[string]string      `json:"labels" db:"-"`
	Metadata    map[string]interface{} `json:"metadata,omitempty" db:"-"`
	CreatedAt   time.Time              `json:"createdAt" db:"created_at"`
	UpdatedAt   time.Time              `json:"updatedAt" db:"updated_at"`
}

// RetryPolicy defines how a node should be retried on failure
type RetryPolicy struct {
	MaxAttempts        int           `json:"maxAttempts"`
	InitialInterval    time.Duration `json:"initialInterval"`
	BackoffCoefficient float64       `json:"backoffCoefficient"`
	MaximumInterval    time.Duration `json:"maximumInterval"`
}

// Node represents a single step in a workflow
type Node struct {
	ID               string                 `json:"id"`
	Type             string                 `json:"type"`
	Name             string                 `json:"name"`
	Config           map[string]interface{} `json:"config"`
	RetryCount       int                    `json:"retryCount"` // Deprecated, use RetryPolicy
	RetryPolicy      *RetryPolicy           `json:"retryPolicy,omitempty"`
	CompensateNodeID string                 `json:"compensateNodeId,omitempty"`
	// Meta is opaque UI/presentation metadata ignored by the engine runtime.
	Meta map[string]interface{} `json:"meta,omitempty"`
}

// Edge represents a connection between two nodes
type Edge struct {
	ID        string `json:"id"`
	SourceID  string `json:"sourceId"`
	TargetID  string `json:"targetId"`
	Condition string `json:"condition,omitempty"`
}

// Execution represents a single run instance of a workflow
type Execution struct {
	ID           string                 `json:"id" db:"id"`
	Namespace    string                 `json:"namespace" db:"namespace"`
	WorkflowID   string                 `json:"workflowId" db:"workflow_id"`
	Version      int                    `json:"version" db:"version"`
	Status       TaskStatus             `json:"status" db:"status"`
	Input        string                 `json:"input" db:"input"`
	Output       string                 `json:"output" db:"output"`
	ErrorMessage string                 `json:"errorMessage,omitempty" db:"error_message"`
	Labels       map[string]string      `json:"labels" db:"-"`
	Metadata     map[string]interface{} `json:"metadata,omitempty" db:"-"`
	// Variables is the durable execution-scoped key/value store.
	Variables  map[string]interface{} `json:"variables,omitempty" db:"-"`
	StartedAt  time.Time              `json:"startedAt" db:"started_at"`
	FinishedAt *time.Time             `json:"finishedAt,omitempty" db:"finished_at"`
}

// ExecutionStep represents the state of an individual node in an execution
type ExecutionStep struct {
	ID            string            `json:"id" db:"id"`
	Namespace     string            `json:"namespace" db:"namespace"`
	ExecutionID   string            `json:"executionId" db:"execution_id"`
	NodeID        string            `json:"nodeId" db:"node_id"`
	Status        TaskStatus        `json:"status" db:"status"`
	Input         string            `json:"input" db:"input"`
	Output        string            `json:"output" db:"output"`
	Error         string            `json:"error,omitempty" db:"error"`
	AttemptNumber int               `json:"attemptNumber" db:"attempt_number"`
	WorkerID      string            `json:"workerId,omitempty" db:"worker_id"`
	Labels        map[string]string `json:"labels" db:"-"`
	ScheduledAt   *time.Time        `json:"scheduledAt,omitempty" db:"scheduled_at"`
	StartedAt     time.Time         `json:"startedAt" db:"started_at"`
	UpdatedAt     time.Time         `json:"updatedAt" db:"updated_at"`
	FinishedAt    *time.Time        `json:"finishedAt,omitempty" db:"finished_at"`

	// Human-task fields (serialized to task_meta in SQL)
	Assignees       []string   `json:"assignees,omitempty" db:"-"`
	CandidateGroups []string   `json:"candidateGroups,omitempty" db:"-"`
	ClaimMode       ClaimMode  `json:"claimMode,omitempty" db:"-"`
	ClaimedBy       string     `json:"claimedBy,omitempty" db:"-"`
	ClaimedAt       *time.Time `json:"claimedAt,omitempty" db:"-"`
	DueAt           *time.Time `json:"dueAt,omitempty" db:"-"`
	Priority        int        `json:"priority,omitempty" db:"-"`
	Action          string     `json:"action,omitempty" db:"-"` // last complete action
}

// CompensationRecord audits saga/rollback activity.
type CompensationRecord struct {
	ID               string    `json:"id" db:"id"`
	Namespace        string    `json:"namespace" db:"namespace"`
	ExecutionID      string    `json:"executionId" db:"execution_id"`
	FailedNodeID     string    `json:"failedNodeId" db:"failed_node_id"`
	CompensateNodeID string    `json:"compensateNodeId" db:"compensate_node_id"`
	Reason           string    `json:"reason" db:"reason"`
	Actor            string    `json:"actor" db:"actor"`
	InputSnapshot    string    `json:"inputSnapshot,omitempty" db:"input_snapshot"`
	OutputSnapshot   string    `json:"outputSnapshot,omitempty" db:"output_snapshot"`
	CreatedAt        time.Time `json:"createdAt" db:"created_at"`
}

// LegacyStep describes a linear/legacy step for ImportStepChain.
type LegacyStep struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Name       string                 `json:"name"`
	Config     map[string]interface{} `json:"config,omitempty"`
	Meta       map[string]interface{} `json:"meta,omitempty"`
	Next       []string               `json:"next,omitempty"`
	Conditions map[string]string      `json:"conditions,omitempty"` // targetID -> condition
}

// Schedule is a durable cron trigger for a workflow (tenant-isolated via Namespace).
type Schedule struct {
	ID                 string     `json:"id" db:"id"`
	Namespace          string     `json:"namespace" db:"namespace"`
	WorkflowID         string     `json:"workflow_id" db:"workflow_id"`
	Name               string     `json:"name,omitempty" db:"name"`
	CronExpression     string     `json:"cron_expression" db:"cron_expression"`
	Input              string     `json:"input" db:"input"`       // JSON string
	Timezone           string     `json:"timezone" db:"timezone"` // IANA, default UTC
	IsActive           bool       `json:"is_active" db:"is_active"`
	CatchUp            bool       `json:"catch_up" db:"catch_up"`
	CatchUpMax         int        `json:"catch_up_max" db:"catch_up_max"`                   // 0 → default 10
	CatchUpLookbackSec int        `json:"catch_up_lookback_sec" db:"catch_up_lookback_sec"` // 0 → default 86400
	StartsAt           *time.Time `json:"starts_at,omitempty" db:"starts_at"`
	EndsAt             *time.Time `json:"ends_at,omitempty" db:"ends_at"`
	LastRunAt          *time.Time `json:"last_run_at,omitempty" db:"last_run_at"`
	NextRunAt          *time.Time `json:"next_run_at,omitempty" db:"next_run_at"`
	LastExecID         string     `json:"last_exec_id,omitempty" db:"last_exec_id"`
	LastError          string     `json:"last_error,omitempty" db:"last_error"`
	CreatedAt          time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at" db:"updated_at"`
}
