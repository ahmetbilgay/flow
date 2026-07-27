# Advanced Features

Learn about advanced enterprise features: Pluggable Authorization, Cron Tickers, Webhooks, and Monitoring.

---

## 🔒 Pluggable Authorization

Dgmos Flow enforces access controls on workflow definitions using a pluggable `AuthProvider` interface.

```go
type AuthProvider interface {
    CheckAccess(ctx context.Context, resource string, action string) error
}
```

### 1. No Auth (Development)
By default, the engine is created without an auth provider, allowing all actions:
```go
engine := flow.NewEngine(storage, registry)
```

### 2. Custom RBAC & Tenant Isolation
You can pass credentials (like JWT tokens, User ID, Team ID) using `context.Context`, and define custom checks inside `CheckAccess`:

```go
type CustomAuth struct{}

func (a *CustomAuth) CheckAccess(ctx context.Context, resource string, action string) error {
	// 1. Retrieve tenant info from context
	customerID, _ := ctx.Value("customer_id").(string)
	if customerID == "" {
		return errors.New("unauthorized: missing customer_id")
	}

	// 2. Format: "workflow:namespace:workflow-id"
	// Parse resource details to verify ownership
	// ...
	
	return nil
}

// Register Auth Provider
engine.SetAuthProvider(&CustomAuth{})
```

---

## ⏱️ Scheduling (Cron Manager)

Durable, multi-replica-safe cron triggers with IANA timezones, catch-up, schedule windows, and REST API.

```go
ctx := context.Background()
_ = engine.CronManager.Start(ctx)
defer engine.CronManager.Stop()

id, err := engine.CronManager.AddScheduleWithOpts(ctx, "tenant-a", "db-backup-wf", "0 0 9 * * *", `{"type":"daily"}`, flow.ScheduleOpts{
	Name:     "morning-backup",
	Timezone: "Europe/Istanbul",
	CatchUp:  true,
})

expr := "0 30 9 * * *"
tz := "Europe/Istanbul"
_ = engine.CronManager.UpdateSchedule(ctx, "tenant-a", id, flow.ScheduleUpdateOpts{
	CronExpression: &expr,
	Timezone:       &tz,
})
_ = engine.CronManager.PauseSchedule(ctx, "tenant-a", id)
_ = engine.CronManager.ResumeSchedule(ctx, "tenant-a", id)
list, _ := engine.CronManager.ListSchedulesFiltered(ctx, "tenant-a", flow.ScheduleListFilter{ActiveOnly: true})
_ = list
next, _ := engine.CronManager.PreviewNextRuns("0 0 9 * * *", "Europe/Istanbul", 5)
_ = next

// REST (optional)
api := flow.NewAPIManager(flow.NewWebhookManager(engine)).WithCron(engine.CronManager)
http.Handle("/", api)
```

Schedule REST endpoints:
- `GET|POST /api/{namespace}/schedules` (`?active_only=true&limit=&offset=`)
- `GET|PUT|DELETE /api/{namespace}/schedules/{id}` (PUT = partial update)
- `POST /api/{namespace}/schedules/{id}/pause|resume`
- `POST /api/{namespace}/schedules/preview`

Semantics: Pause = soft disable; DELETE = hard remove. Expired `ends_at` auto-pauses without auth.

---

## 📡 Webhook Manager

Launch workflows dynamically using standard HTTP POST requests. The `WebhookManager` handles routing and triggers executions immediately.

```go
webhookMgr := flow.NewWebhookManager(engine)

// Bind endpoint mapping: /webhooks/{namespace}/{workflow_id}
http.Handle("/webhooks/", webhookMgr)
log.Fatal(http.ListenAndServe(":8080", nil))
```
Trigger execution via terminal:
```bash
curl -X POST http://localhost:8080/webhooks/default/my-workflow -d '{"data": "value"}'
```

---

## 📊 Telemetry & Monitoring

### 1. Prometheus Metrics
Metrics are pre-registered and served using standard handlers.

```go
import (
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

http.Handle("/metrics", promhttp.Handler())
```

*   **Workflows Metrics**:
    *   `flow_workflows_started_total`
    *   `flow_workflows_completed_total`
    *   `flow_workflows_failed_total`
*   **Step Metrics**:
    *   `flow_steps_processed_total` (Labels: `namespace`, `node_type`, `status`)
    *   `flow_step_duration_seconds` (Labels: `namespace`, `node_type`)
*   **Worker Metrics**:
    *   `flow_active_workers` (Labels: `worker_id`)

### 2. Structured Logging
Inject custom `slog.Logger` handles into the engine:
```go
import "log/slog"

logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
engine.WithLogger(logger)
```

---

## Interactive Advance & Human Tasks (DGMOS-ready)

Host-driven step advancement for human/API workflows. Interactive / `humantask` nodes are created as `WAITING` so workers do not claim them.

```go
execID, _ := engine.StartWorkflow(ctx, "default", "approval", `{}`)

tasks, _ := engine.ListTasks(ctx, "default", flow.TaskFilter{ExecutionID: execID})
_, next, _ := engine.CompleteTask(ctx, "default", execID, "approve", "user@acme.com", "approve", `{"comment":"ok"}`)
_, next, _ = engine.Advance(ctx, "default", execID, "next-node", "submit", `{}`)
```

Node config highlights:

* `type: "humantask"` with `assignees`, `candidate_groups`, `claim_mode` (`any|all|first`)
* `execution_mode: "interactive" | "hybrid" | "worker"` (default worker)
* Opaque `Node.Meta` for UI (`title`, `ui_schema`, …) — ignored by the runtime

See `examples/interactive_approval/`.

---

## Durable Variables

```go
_ = engine.SetVar(ctx, ns, execID, "customer_id", "C-1")
_ = engine.MergeVars(ctx, ns, execID, map[string]interface{}{"cargo": payload})
v, _ := engine.GetVar(ctx, ns, execID, "customer_id")
```

`setvariable` nodes also write to the durable store when executed by a worker/Advance.

---

## HostHooks & EventBus

Engine lifecycle events are emitted on the engine path. Use `SetHostHooks` for synchronous host bridges (WebSocket / notifications):

```go
engine.SetHostHooks(myHandler) // HandleEvent called sync
bus := flow.NewEventBus()
bus.RegisterGlobalHandler(myHandler)
engine.SetEventBus(bus)
```

Additional event types: `EventStepWaitingHuman`, `EventTaskClaimed`, `EventTaskCompleted`, `EventRollbackStarted`.

---

## Auth Resource Naming

Recommended resource strings for `AuthProvider.CheckAccess`:

| Resource | Example |
|----------|---------|
| Workflow | `workflow:{namespace}:{workflowId}` |
| Execution | `execution:{namespace}:{executionId}` |
| Task | `task:{namespace}:{stepId}` |

Suggested metadata keys on workflows/executions: `customer_id`, `owner_id`.

---

## Rollback Journal

```go
_ = engine.RollbackTo(ctx, ns, execID, "charge", "user cancelled", "admin")
records, _ := engine.ListCompensationJournal(ctx, ns, execID)
```

Saga compensation enqueue is preserved; journal stores actor/reason/snapshots.

---

## Legacy Import

```go
wf, err := flow.ImportStepChain("onboarding", "Onboarding", []flow.LegacyStep{
    {ID: "a", Type: flow.NodeTypeHumanTask, Next: []string{"b"}},
    {ID: "b", Type: flow.NodeTypeFunction, Config: map[string]interface{}{"function": "Done"}},
})
```
