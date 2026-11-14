// Package flow provides a high-performance workflow orchestration engine for Go.
//
// Quick Start with MySQL:
//
//	import (
//	    "github.com/dgmos/flow"
//	    _ "github.com/go-sql-driver/mysql"
//	    "github.com/jmoiron/sqlx"
//	)
//
//	func main() {
//	    // Connect to MySQL
//	    db, _ := sqlx.Connect("mysql", "user:pass@tcp(localhost:3306)/db?parseTime=true")
//
//	    // Create storage (auto-creates tables!)
//	    storage, _ := flow.NewMySQLStorage(db)
//
//	    // Create engine
//	    registry := flow.NewRegistry()
//	    engine := flow.NewEngine(storage, registry)
//
//	    // Register your functions
//	    registry.RegisterFunction("MyTask", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
//	        return map[string]interface{}{"status": "done"}, nil
//	    })
//
//	    // Start worker
//	    go engine.StartWorker(ctx, "default", "worker-1")
//
//	    // Execute workflow
//	    execID, _ := engine.Execute(ctx, "default", "my-workflow", `{"key":"value"}`)
//	}
package flow

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"github.com/dgmos/flow/internal/auth"
	"github.com/dgmos/flow/internal/builder"
	"github.com/dgmos/flow/internal/engine"
	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/node"
	"github.com/dgmos/flow/internal/storage"
	"github.com/dgmos/flow/internal/storage/memory"
	"github.com/dgmos/flow/internal/storage/redis"
	sqlstore "github.com/dgmos/flow/internal/storage/sql"
	"github.com/dgmos/flow/internal/telemetry"
	"github.com/dgmos/flow/internal/trigger"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
)

// ========================================
// CORE TYPES
// ========================================

// Engine is the workflow execution engine
type Engine struct {
	*engine.Engine
	CronManager *CronManager
}

// Registry manages workflow function handlers
type Registry struct {
	*engine.Registry
}

// Storage interface for persistence
type Storage = storage.Storage

// Worker executes workflow tasks
type Worker struct {
	*engine.Worker
}

// Graph represents the DAG structure of a workflow
type Graph = engine.Graph

// ========================================
// WORKFLOW MODELS
// ========================================

type (
	Workflow       = models.Workflow
	Node           = models.Node
	Edge           = models.Edge
	Execution      = models.Execution
	ExecutionStep  = models.ExecutionStep
	RetryPolicy    = models.RetryPolicy
	WorkflowStatus = models.WorkflowStatus
	TaskStatus     = models.TaskStatus
	Schedule       = models.Schedule
)

// Node types (use these strings in Node.Type field)
const (
	NodeTypeFunction    = "function"
	NodeTypeHTTP        = "http"
	NodeTypeCondition   = "condition"
	NodeTypeLog         = "log"
	NodeTypeTransform   = "transform"
	NodeTypeSignal      = "signal"
	NodeTypeSubWorkflow = "subworkflow"
	NodeTypeAI          = "ai"
	NodeTypeNotify      = "notify"
	NodeTypeSwitch      = "switch"
	NodeTypeWait        = "wait"
	NodeTypeSetVariable = "setvariable"
	NodeTypeHumanTask   = "humantask"
)

// Workflow statuses
const (
	WorkflowActive   = models.WorkflowActive
	WorkflowInactive = models.WorkflowInactive
)

// Task/Execution statuses
const (
	StatusPending   = models.TaskPending
	StatusRunning   = models.TaskRunning
	StatusCompleted = models.TaskCompleted
	StatusFailed    = models.TaskFailed
	StatusSkipped   = models.TaskSkipped
	StatusWaiting   = models.TaskWaiting
	StatusCancelled = models.TaskCancelled
)

// ========================================
// FUNCTION HANDLER TYPE
// ========================================

// HandlerFunc is the signature for workflow functions
//
// Example:
//
//	func ProcessOrder(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
//	    orderID := input["order_id"].(string)
//	    // ... process order ...
//	    return map[string]interface{}{"status": "processed"}, nil
//	}
type HandlerFunc func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)

// NodeExecutor defines the interface for custom node types
type NodeExecutor = engine.NodeExecutor

// ========================================
// ENGINE METHODS
// ========================================

// NewEngine creates a new workflow engine
func NewEngine(storage Storage, registry *Registry) *Engine {
	// Auto-register all built-in node types
	funcNode := node.NewFunctionNode()
	funcNode.SetRegistry(registry.Registry)

	subWFNode := node.NewSubWorkflowNode(nil)

	registry.Register("function", funcNode)
	registry.Register("http", node.NewHTTPNode())
	registry.Register("condition", &node.ConditionNode{})
	registry.Register("log", node.NewLogNode())
	registry.Register("transform", &node.TransformNode{})
	registry.Register("signal", &node.SignalNode{})
	registry.Register("subworkflow", subWFNode)
	registry.Register("ai", &node.AINode{})
	registry.Register("notify", node.NewNotifyNode())
	registry.Register("switch", &node.SwitchNode{})
	registry.Register("wait", &node.WaitNode{})
	registry.Register("setvariable", &node.SetVariableNode{})
	registry.Register("humantask", &node.HumanTaskNode{})

	eng := &Engine{Engine: engine.NewEngine(storage, registry.Registry)}
	subWFNode.SetEngine(eng)
	eng.CronManager = &CronManager{CronManager: trigger.NewCronManager(eng.Engine, storage, eng.GetLogger())}
	eng.CronManager.CronManager.SetEventBus(eng.EventBus())

	return eng
}

// WithLogger sets a custom logger for the engine and bound CronManager.
func (e *Engine) WithLogger(l *slog.Logger) *Engine {
	e.Engine.WithLogger(l)
	if e.CronManager != nil {
		e.CronManager.SetLogger(l)
	}
	return e
}

// AuthProvider is the pluggable authorization interface.
type AuthProvider = auth.AuthProvider

// SetAuthProvider sets authorization for the engine and CronManager.
func (e *Engine) SetAuthProvider(provider AuthProvider) *Engine {
	e.Engine.SetAuthProvider(provider)
	if e.CronManager != nil {
		e.CronManager.SetAuthProvider(provider)
	}
	return e
}

// GetLogger returns the engine's logger
func (e *Engine) GetLogger() *slog.Logger {
	return e.Engine.GetLogger()
}

// RegisterWorkflow saves a workflow definition
func (e *Engine) RegisterWorkflow(ctx context.Context, namespace string, wf *Workflow) error {
	return e.Engine.RegisterWorkflow(ctx, namespace, wf)
}

// Execute starts a new workflow execution (alias for StartWorkflow)
func (e *Engine) Execute(ctx context.Context, namespace, workflowID string, input []byte) (string, error) {
	return e.Engine.StartWorkflow(ctx, namespace, workflowID, string(input))
}

// StartWorkflow starts a new workflow execution
func (e *Engine) StartWorkflow(ctx context.Context, namespace, workflowID string, input string) (string, error) {
	return e.Engine.StartWorkflow(ctx, namespace, workflowID, input)
}

// GetExecution retrieves an execution by ID
func (e *Engine) GetExecution(ctx context.Context, namespace, execID string) (*Execution, error) {
	return e.Engine.GetExecutionStatus(ctx, namespace, execID)
}

// ListExecutions lists all executions in a namespace.
func (e *Engine) ListExecutions(ctx context.Context, namespace string) ([]*Execution, error) {
	return e.Engine.ListExecutions(ctx, namespace)
}

// CheckAccess runs the configured AuthProvider (no-op when unset).
func (e *Engine) CheckAccess(ctx context.Context, resource, action string) error {
	return e.Engine.CheckAccess(ctx, resource, action)
}

// GetExecutionStatus retrieves an execution's status
func (e *Engine) GetExecutionStatus(ctx context.Context, namespace, execID string) (*Execution, error) {
	return e.Engine.GetExecutionStatus(ctx, namespace, execID)
}

// GetExecutionSteps retrieves all steps for an execution
func (e *Engine) GetExecutionSteps(ctx context.Context, namespace, execID string) ([]*ExecutionStep, error) {
	return e.Engine.GetExecutionSteps(ctx, namespace, execID)
}

// CancelExecution cancels a running execution
func (e *Engine) CancelExecution(ctx context.Context, namespace, execID string) error {
	return e.Engine.CancelExecution(ctx, namespace, execID)
}

// FailExecution marks an execution as failed
func (e *Engine) FailExecution(ctx context.Context, namespace, execID, message string) error {
	return e.Engine.FailExecution(ctx, namespace, execID, message)
}

// SignalExecution sends a signal to resume a waiting workflow
func (e *Engine) SignalExecution(ctx context.Context, namespace, execID, nodeID, output string) error {
	return e.Engine.SignalExecution(ctx, namespace, execID, nodeID, output)
}

// ReapTimeouts checks for and handles timed-out tasks
func (e *Engine) ReapTimeouts(ctx context.Context, namespace string) error {
	return e.Engine.ReapTimeouts(ctx, namespace)
}

// StartWorker starts a worker that processes tasks
func (e *Engine) StartWorker(ctx context.Context, namespace, workerID string) {
	e.Engine.StartWorker(ctx, namespace, workerID)
}

// SetHostHooks wires a synchronous host integration handler (e.g. WebSocket bridge).
func (e *Engine) SetHostHooks(h EventHandler) *Engine {
	e.Engine.SetHostHooks(h)
	if e.CronManager != nil {
		e.CronManager.SetHostHooks(h)
	}
	return e
}

// SetEventBus replaces the engine event bus and rewires CronManager.
func (e *Engine) SetEventBus(bus *EventBus) *Engine {
	if bus != nil {
		e.Engine.SetEventBus(bus.EventBus)
		if e.CronManager != nil {
			e.CronManager.SetEventBus(bus)
		}
	}
	return e
}

// Advance interactively completes a waiting/interactive step.
func (e *Engine) Advance(ctx context.Context, namespace, execID, nodeID, action, payload string) (string, []*ExecutionStep, error) {
	return e.Engine.Advance(ctx, namespace, execID, nodeID, action, payload)
}

// ListTasks lists WAITING human/interactive tasks.
func (e *Engine) ListTasks(ctx context.Context, namespace string, filter TaskFilter) ([]*ExecutionStep, error) {
	return e.Engine.ListTasks(ctx, namespace, filter)
}

// ClaimTask claims a human task for an actor.
func (e *Engine) ClaimTask(ctx context.Context, namespace, execID, nodeID, actor string) error {
	return e.Engine.ClaimTask(ctx, namespace, execID, nodeID, actor)
}

// ReleaseTask releases a claimed human task.
func (e *Engine) ReleaseTask(ctx context.Context, namespace, execID, nodeID, actor string) error {
	return e.Engine.ReleaseTask(ctx, namespace, execID, nodeID, actor)
}

// CompleteTask claims (if needed) and completes a human task.
func (e *Engine) CompleteTask(ctx context.Context, namespace, execID, nodeID, actor, action, payload string) (string, []*ExecutionStep, error) {
	return e.Engine.CompleteTask(ctx, namespace, execID, nodeID, actor, action, payload)
}

// GetVar reads a durable execution variable.
func (e *Engine) GetVar(ctx context.Context, namespace, execID, key string) (interface{}, error) {
	return e.Engine.GetVar(ctx, namespace, execID, key)
}

// SetVar sets a durable execution variable.
func (e *Engine) SetVar(ctx context.Context, namespace, execID, key string, value interface{}) error {
	return e.Engine.SetVar(ctx, namespace, execID, key, value)
}

// MergeVars merges into durable execution variables.
func (e *Engine) MergeVars(ctx context.Context, namespace, execID string, vars map[string]interface{}) error {
	return e.Engine.MergeVars(ctx, namespace, execID, vars)
}

// GetVars returns all durable execution variables.
func (e *Engine) GetVars(ctx context.Context, namespace, execID string) (map[string]interface{}, error) {
	return e.Engine.GetVars(ctx, namespace, execID)
}

// RollbackTo journals and enqueues compensation toward a node.
func (e *Engine) RollbackTo(ctx context.Context, namespace, execID, nodeID, reason, actor string) error {
	return e.Engine.RollbackTo(ctx, namespace, execID, nodeID, reason, actor)
}

// ListCompensationJournal lists compensation/rollback records.
func (e *Engine) ListCompensationJournal(ctx context.Context, namespace, execID string) ([]*CompensationRecord, error) {
	return e.Engine.ListCompensationJournal(ctx, namespace, execID)
}

// TaskFilter filters ListTasks results.
type TaskFilter = engine.TaskFilter

// CompensationRecord audits saga/rollback activity.
type CompensationRecord = models.CompensationRecord

// Claim mode constants
const (
	ClaimAny   = models.ClaimAny
	ClaimAll   = models.ClaimAll
	ClaimFirst = models.ClaimFirst
)

// ========================================
// REGISTRY METHODS
// ========================================

// NewRegistry creates a new function registry
func NewRegistry() *Registry {
	return &Registry{
		Registry: engine.NewRegistry(),
	}
}

// RegisterFunction registers a function handler
//
// Example:
//
//	registry.RegisterFunction("SendEmail", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
//	    email := input["email"].(string)
//	    // Send email...
//	    return map[string]interface{}{"sent": true}, nil
//	})
func (r *Registry) RegisterFunction(name string, handler HandlerFunc) {
	// Wrap user handler to match internal signature
	wrappedHandler := func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		return handler(ctx, input)
	}
	r.Registry.RegisterFunction(name, wrappedHandler)
}

// Register registers a custom node executor
func (r *Registry) Register(nodeType string, executor NodeExecutor) {
	r.Registry.Register(nodeType, executor)
}

// Get retrieves a registered executor by type
func (r *Registry) Get(nodeType string) (NodeExecutor, error) {
	return r.Registry.Get(nodeType)
}

// ========================================
// WORKER
// ========================================

// NewWorker creates a new worker instance
func NewWorker(id string, eng *Engine, registry *Registry, interval time.Duration) *Worker {
	return &Worker{engine.NewWorker(id, eng.Engine, registry.Registry, interval)}
}

// SetNamespace sets the worker's namespace
func (w *Worker) SetNamespace(namespace string) {
	w.Worker.SetNamespace(namespace)
}

// Start begins the worker's task processing loop
func (w *Worker) Start(ctx context.Context) {
	w.Worker.Start(ctx)
}

// ========================================
// STORAGE BACKENDS
// ========================================

// NewMemoryStorage creates an in-memory storage backend (for development)
func NewMemoryStorage() Storage {
	return memory.NewMemoryStorage()
}

// NewMySQLStorage creates a MySQL storage backend with auto-migration
func NewMySQLStorage(db *sqlx.DB) (Storage, error) {
	return sqlstore.NewSQLStorage(db, "mysql")
}

// SQLStorageOptions configures table prefix and migrations.
type SQLStorageOptions = sqlstore.Options

// NewMySQLStorageWithOptions creates MySQL storage with prefix / migrate options.
func NewMySQLStorageWithOptions(db *sqlx.DB, opts SQLStorageOptions) (Storage, error) {
	return sqlstore.NewSQLStorageWithOptions(db, "mysql", opts)
}

// NewPostgreSQLStorage creates a PostgreSQL storage backend with auto-migration
func NewPostgreSQLStorage(db *sqlx.DB) (Storage, error) {
	return sqlstore.NewSQLStorage(db, "postgres")
}

// NewPostgreSQLStorageWithOptions creates Postgres storage with options.
func NewPostgreSQLStorageWithOptions(db *sqlx.DB, opts SQLStorageOptions) (Storage, error) {
	return sqlstore.NewSQLStorageWithOptions(db, "postgres", opts)
}

// NewSQLStorageFromDB wraps database/sql.DB (dialect: "mysql" or "postgres").
func NewSQLStorageFromDB(db *sql.DB, dialect string, opts SQLStorageOptions) (Storage, error) {
	return sqlstore.NewSQLStorageFromDB(db, dialect, opts)
}

// NewRedisStorage creates a Redis storage backend
//
// Example:
//
//	storage := flow.NewRedisStorage("localhost:6379", "", 0)
func NewRedisStorage(addr string, password string, db int) Storage {
	return redis.NewRedisStorage(addr, password, db)
}

// ========================================
// ENCRYPTION
// ========================================

// Crypto provides data-at-rest encryption
type Crypto = storage.Crypto

// NewCrypto creates a new encryption instance
//
// Example:
//
//	crypto, _ := flow.NewCrypto("your-32-byte-encryption-key-here")
//	sqlStorage.(*sql.SQLStorage).SetEncryption(crypto)
func NewCrypto(key string) (*Crypto, error) {
	return storage.NewCrypto(key)
}

// ========================================
// WORKFLOW BUILDER
// ========================================

// WorkflowBuilder provides a fluent API for building workflows
type WorkflowBuilder struct {
	*builder.WorkflowBuilder
}

// NodeBuilder provides a fluent API for configuring nodes
type NodeBuilder struct {
	*builder.NodeBuilder
}

// NewWorkflow creates a new workflow builder
//
// Example:
//
//	wf := flow.NewWorkflow("order-processing", "Order Processing Workflow").
//	    AddNode("validate", flow.NodeTypeFunction).
//	        WithConfig("functionName", "ValidateOrder").
//	        Then("process").
//	    AddNode("process", flow.NodeTypeFunction).
//	        WithConfig("functionName", "ProcessOrder").
//	    Build()
func NewWorkflow(id, name string) *WorkflowBuilder {
	return &WorkflowBuilder{builder.NewWorkflow(id, name)}
}

// AddNode adds a node to the workflow
func (b *WorkflowBuilder) AddNode(id, nodeType string) *NodeBuilder {
	return &NodeBuilder{b.WorkflowBuilder.AddNode(id, nodeType)}
}

// Connect creates an edge between two nodes
func (b *WorkflowBuilder) Connect(sourceID, targetID string) *WorkflowBuilder {
	b.WorkflowBuilder.Connect(sourceID, targetID)
	return b
}

// ConnectIf creates a conditional edge
func (b *WorkflowBuilder) ConnectIf(sourceID, targetID, condition string) *WorkflowBuilder {
	b.WorkflowBuilder.ConnectIf(sourceID, targetID, condition)
	return b
}

// Build returns the completed workflow
func (b *WorkflowBuilder) Build() *Workflow {
	return b.WorkflowBuilder.Build()
}

// Visualize generates a Mermaid diagram of the workflow
func (b *WorkflowBuilder) Visualize() string {
	return b.Visualise()
}

// WithConfig adds configuration to a node
func (nb *NodeBuilder) WithConfig(key string, value interface{}) *NodeBuilder {
	nb.NodeBuilder.WithConfig(key, value)
	return nb
}

// WithSaga sets a compensation node (saga pattern)
func (nb *NodeBuilder) WithSaga(compensateNodeID string) *NodeBuilder {
	nb.NodeBuilder.WithSaga(compensateNodeID)
	return nb
}

// WithRetry sets retry count
func (nb *NodeBuilder) WithRetry(count int) *NodeBuilder {
	nb.NodeBuilder.WithRetry(count)
	return nb
}

// Then connects this node to the next node
func (nb *NodeBuilder) Then(targetID string) *NodeBuilder {
	return &NodeBuilder{nb.NodeBuilder.Then(targetID)}
}

// If creates a conditional connection
func (nb *NodeBuilder) If(targetID, condition string) *NodeBuilder {
	return &NodeBuilder{nb.NodeBuilder.If(targetID, condition)}
}

// ToMermaid generates a Mermaid.js diagram from a workflow
func ToMermaid(wf *Workflow) string {
	return builder.ToMermaid(wf)
}

// ========================================
// DAG / GRAPH
// ========================================

// NewGraph creates a new graph from a workflow
func NewGraph(wf *Workflow) (*Graph, error) {
	return engine.NewGraph(wf)
}

// ========================================
// TRIGGERS
// ========================================

// CronManager manages scheduled workflow execution
type CronManager struct {
	*trigger.CronManager
}

// ScheduleOpts configures optional schedule create behavior.
type ScheduleOpts = trigger.ScheduleOpts

// ScheduleUpdateOpts is a partial update patch for schedules.
type ScheduleUpdateOpts = trigger.ScheduleUpdateOpts

// ScheduleListFilter filters ListSchedules results.
type ScheduleListFilter = trigger.ScheduleListFilter

// NewCronManager creates a durable cron scheduler backed by storage.
//
// Example:
//
//	cronMgr := flow.NewCronManager(engine, storage, logger)
//	_ = cronMgr.Start(ctx)
//	id, _ := cronMgr.AddSchedule(ctx, "default", "backup-workflow", "0 0 2 * * *", `{"type":"daily"}`)
func NewCronManager(eng *Engine, store Storage, logger *slog.Logger) *CronManager {
	cm := &CronManager{CronManager: trigger.NewCronManager(eng.Engine, store, logger)}
	cm.CronManager.SetEventBus(eng.EventBus())
	return cm
}

// Start loads active schedules from storage and begins the cron runner.
func (c *CronManager) Start(ctx context.Context) error {
	return c.CronManager.Start(ctx)
}

// Stop halts the cron scheduler
func (c *CronManager) Stop() {
	c.CronManager.Stop()
}

// AddSchedule persists and registers a cron trigger; returns durable schedule ID.
func (c *CronManager) AddSchedule(ctx context.Context, namespace, workflowID, cronExpr, input string) (string, error) {
	return c.CronManager.AddSchedule(ctx, namespace, workflowID, cronExpr, input)
}

// AddScheduleWithOpts is AddSchedule with CatchUp and related options.
func (c *CronManager) AddScheduleWithOpts(ctx context.Context, namespace, workflowID, cronExpr, input string, opts ScheduleOpts) (string, error) {
	return c.CronManager.AddScheduleWithOpts(ctx, namespace, workflowID, cronExpr, input, opts)
}

// UpdateSchedule applies a partial patch to an existing schedule.
func (c *CronManager) UpdateSchedule(ctx context.Context, namespace, scheduleID string, patch ScheduleUpdateOpts) error {
	return c.CronManager.UpdateSchedule(ctx, namespace, scheduleID, patch)
}

// PauseSchedule soft-disables a schedule without deleting it.
func (c *CronManager) PauseSchedule(ctx context.Context, namespace, scheduleID string) error {
	return c.CronManager.PauseSchedule(ctx, namespace, scheduleID)
}

// ResumeSchedule reactivates a paused schedule.
func (c *CronManager) ResumeSchedule(ctx context.Context, namespace, scheduleID string) error {
	return c.CronManager.ResumeSchedule(ctx, namespace, scheduleID)
}

// RemoveSchedule deletes a schedule from storage and unregisters it from memory.
func (c *CronManager) RemoveSchedule(ctx context.Context, namespace, scheduleID string) error {
	return c.CronManager.RemoveSchedule(ctx, namespace, scheduleID)
}

// GetSchedule returns a durable schedule definition.
func (c *CronManager) GetSchedule(ctx context.Context, namespace, scheduleID string) (*Schedule, error) {
	return c.CronManager.GetSchedule(ctx, namespace, scheduleID)
}

// ListSchedules lists schedules for a tenant namespace.
func (c *CronManager) ListSchedules(ctx context.Context, namespace string) ([]*Schedule, error) {
	return c.CronManager.ListSchedules(ctx, namespace)
}

// ListSchedulesFiltered lists schedules with optional filters.
func (c *CronManager) ListSchedulesFiltered(ctx context.Context, namespace string, filter ScheduleListFilter) ([]*Schedule, error) {
	return c.CronManager.ListSchedulesFiltered(ctx, namespace, filter)
}

// PreviewNextRuns returns upcoming fire times for a cron expression (UI helper).
func (c *CronManager) PreviewNextRuns(cronExpr, timezone string, n int) ([]time.Time, error) {
	return c.CronManager.PreviewNextRuns(cronExpr, timezone, n)
}

// SetAuthProvider sets authorization checks for schedule operations.
func (c *CronManager) SetAuthProvider(provider AuthProvider) {
	c.CronManager.SetAuthProvider(provider)
}

// SetHostHooks wires sync host integration for schedule events.
func (c *CronManager) SetHostHooks(h EventHandler) {
	c.CronManager.SetHostHooks(h)
}

// SetEventBus wires schedule.* lifecycle events.
func (c *CronManager) SetEventBus(bus *EventBus) {
	if bus != nil {
		c.CronManager.SetEventBus(bus.EventBus)
	}
}

// WebhookManager handles HTTP webhook triggers
type WebhookManager struct {
	*trigger.WebhookManager
}

// NewWebhookManager creates a webhook handler
//
// Example:
//
//	webhookMgr := flow.NewWebhookManager(engine)
//	http.Handle("/webhooks/", webhookMgr)
//
// POST /webhooks/{namespace}/{workflow_id} with JSON body to trigger workflows
func NewWebhookManager(eng *Engine) *WebhookManager {
	return &WebhookManager{trigger.NewWebhookManager(eng.Engine)}
}

// ServeHTTP handles webhook requests
func (w *WebhookManager) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.WebhookManager.ServeHTTP(rw, r)
}

// APIManager provides REST API for workflow management
type APIManager struct {
	*trigger.APIManager
}

// NewAPIManager creates a REST API handler.
//
// Example:
//
//	apiMgr := flow.NewAPIManager(webhookMgr).WithCron(engine.CronManager)
//	http.Handle("/", apiMgr)
//
// Endpoints:
//   - GET /health
//   - GET /metrics (Prometheus)
//   - GET /api/{namespace}/executions
//   - GET /api/{namespace}/executions/{id}
//   - GET /api/{namespace}/executions/{id}/steps
//   - POST /api/{namespace}/executions/{id}/cancel
//   - POST /api/{namespace}/executions/{id}/signal/{nodeID}
//   - POST /api/{namespace}/executions/{id}/rollback
//   - POST /api/{namespace}/executions/{id}/tasks/{nodeId}/claim|release|complete|advance
//   - GET /api/{namespace}/tasks
//   - GET|POST /api/{namespace}/schedules
//   - GET|PUT|DELETE /api/{namespace}/schedules/{id}
//   - POST /api/{namespace}/schedules/{id}/pause|resume
//   - POST /api/{namespace}/schedules/preview
func NewAPIManager(webhookMgr *WebhookManager) *APIManager {
	return &APIManager{APIManager: trigger.NewAPIManager(webhookMgr.WebhookManager)}
}

// WithCron enables schedule management REST endpoints.
func (a *APIManager) WithCron(c *CronManager) *APIManager {
	if c != nil {
		a.APIManager.SetCronManager(c.CronManager)
	}
	return a
}

// ServeHTTP handles API requests
func (a *APIManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.APIManager.ServeHTTP(w, r)
}

// ========================================
// EVENTS
// ========================================

// EventType represents workflow event types
type EventType = engine.EventType

// Event types
const (
	EventWorkflowStarted       = engine.EventTypeWorkflowStarted
	EventWorkflowCompleted     = engine.EventTypeWorkflowCompleted
	EventWorkflowFailed        = engine.EventTypeWorkflowFailed
	EventWorkflowCancelled     = engine.EventTypeWorkflowCancelled
	EventStepStarted           = engine.EventTypeStepStarted
	EventStepCompleted         = engine.EventTypeStepCompleted
	EventStepFailed            = engine.EventTypeStepFailed
	EventStepRetrying          = engine.EventTypeStepRetrying
	EventStepWaitingSignal     = engine.EventTypeStepWaitingSignal
	EventStepWaitingHuman      = engine.EventTypeStepWaitingHuman
	EventSignalReceived        = engine.EventTypeSignalReceived
	EventTaskClaimed           = engine.EventTypeTaskClaimed
	EventTaskCompleted         = engine.EventTypeTaskCompleted
	EventCompensationTriggered = engine.EventTypeCompensationTriggered
	EventRollbackStarted       = engine.EventTypeRollbackStarted
	EventScheduleCreated       = engine.EventTypeScheduleCreated
	EventScheduleUpdated       = engine.EventTypeScheduleUpdated
	EventSchedulePaused        = engine.EventTypeSchedulePaused
	EventScheduleResumed       = engine.EventTypeScheduleResumed
	EventScheduleDeleted       = engine.EventTypeScheduleDeleted
	EventScheduleFired         = engine.EventTypeScheduleFired
	EventScheduleFireFailed    = engine.EventTypeScheduleFireFailed
)

// Event represents a workflow event
type Event = engine.Event

// EventHandler handles workflow events
type EventHandler = engine.EventHandler

// HostHooks is a sync host integration alias (WS, notifications).
type HostHooks = engine.HostHooks

// EventBus manages event subscriptions
type EventBus struct {
	*engine.EventBus
}

// NewEventBus creates a new event bus
//
// Example:
//
//	eventBus := flow.NewEventBus()
//	eventBus.RegisterHandler(flow.EventWorkflowCompleted, myHandler)
func NewEventBus() *EventBus {
	return &EventBus{engine.NewEventBus()}
}

// RegisterHandler registers a handler for a specific event type
func (eb *EventBus) RegisterHandler(eventType EventType, handler EventHandler) {
	eb.EventBus.RegisterHandler(eventType, handler)
}

// RegisterGlobalHandler registers a handler for all events
func (eb *EventBus) RegisterGlobalHandler(handler EventHandler) {
	eb.EventBus.RegisterGlobalHandler(handler)
}

// Emit emits an event asynchronously
func (eb *EventBus) Emit(ctx context.Context, event Event) {
	eb.EventBus.Emit(ctx, event)
}

// EmitSync emits an event synchronously
func (eb *EventBus) EmitSync(ctx context.Context, event Event) []error {
	return eb.EventBus.EmitSync(ctx, event)
}

// ========================================
// TELEMETRY / METRICS
// ========================================

// Prometheus metrics (pre-registered)
var (
	WorkflowsStarted   = telemetry.WorkflowsStarted
	WorkflowsCompleted = telemetry.WorkflowsCompleted
	WorkflowsFailed    = telemetry.WorkflowsFailed
	StepsProcessed     = telemetry.StepsProcessed
	StepDuration       = telemetry.StepDuration
	ActiveWorkers      = telemetry.ActiveWorkers
	SchedulesActive    = telemetry.SchedulesActive
	ScheduleFires      = telemetry.ScheduleFires
)

// MetricsRegistry returns the default Prometheus registerer.
func MetricsRegistry() prometheus.Registerer {
	return prometheus.DefaultRegisterer
}

// ========================================
// BUILT-IN NODE TYPES
// ========================================

// Built-in node types are automatically registered when creating an Engine
// No need to manually register them - just use the node type constants:
//   - flow.NodeTypeFunction
//   - flow.NodeTypeHTTP
//   - flow.NodeTypeCondition
//   etc.
