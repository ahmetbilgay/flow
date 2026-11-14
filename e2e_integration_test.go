//go:build integration

package flow_test

import (
	"context"
	"testing"
	"time"

	"github.com/dgmos/flow"
	"github.com/dgmos/flow/internal/storage/integrationtest"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func TestE2EWorkflowMySQL(t *testing.T) {
	runE2EWorkflow(t, "mysql", integrationtest.MySQLDSN())
}

func TestE2EWorkflowPostgres(t *testing.T) {
	runE2EWorkflow(t, "postgres", integrationtest.PostgresDSN())
}

func runE2EWorkflow(t *testing.T, dialect, dsn string) {
	t.Helper()
	db, err := sqlx.Connect(dialect, dsn)
	integrationtest.SkipIfUnavailable(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var storage flow.Storage
	switch dialect {
	case "mysql":
		storage, err = flow.NewMySQLStorageWithOptions(db, flow.SQLStorageOptions{
			TablePrefix: "e2e_" + integrationtest.UniqueID()[:8] + "_",
			AutoMigrate: true,
		})
	case "postgres":
		storage, err = flow.NewPostgreSQLStorageWithOptions(db, flow.SQLStorageOptions{
			TablePrefix: "e2e_" + integrationtest.UniqueID()[:8] + "_",
			AutoMigrate: true,
		})
	default:
		t.Fatalf("unsupported dialect %s", dialect)
	}
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	ctx := context.Background()
	ns := integrationtest.UniqueNamespace()
	registry := flow.NewRegistry()
	engine := flow.NewEngine(storage, registry)

	registry.RegisterFunction("Echo", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"echo": true, "in": input}, nil
	})

	wfBuilder := flow.NewWorkflow("e2e-wf", "E2E")
	wfBuilder.AddNode("echo", flow.NodeTypeFunction).WithConfig("function", "Echo")
	wf := wfBuilder.Build()
	if err := engine.RegisterWorkflow(ctx, ns, wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	go engine.StartWorker(wctx, ns, "e2e-worker")

	execID, err := engine.Execute(ctx, ns, "e2e-wf", []byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		exec, err := engine.GetExecution(ctx, ns, execID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if exec.Status == flow.StatusCompleted {
			steps, _ := engine.GetExecutionSteps(ctx, ns, execID)
			if len(steps) != 1 {
				t.Fatalf("expected 1 step, got %d", len(steps))
			}
			return
		}
		if exec.Status == flow.StatusFailed {
			t.Fatalf("execution failed: %s", exec.ErrorMessage)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("execution did not complete in time")
}

func TestE2ECronScheduleMySQL(t *testing.T) {
	db, err := sqlx.Connect("mysql", integrationtest.MySQLDSN())
	integrationtest.SkipIfUnavailable(t, err)
	t.Cleanup(func() { _ = db.Close() })

	storage, err := flow.NewMySQLStorageWithOptions(db, flow.SQLStorageOptions{
		TablePrefix: "e2e_cron_" + integrationtest.UniqueID()[:8] + "_",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	ctx := context.Background()
	ns := integrationtest.UniqueNamespace()
	registry := flow.NewRegistry()
	engine := flow.NewEngine(storage, registry)

	registry.RegisterFunction("Tick", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"ok": true}, nil
	})
	_ = engine.RegisterWorkflow(ctx, ns, &flow.Workflow{
		ID: "tick-wf", Name: "Tick", Version: 1,
		Nodes: []flow.Node{{ID: "n1", Type: flow.NodeTypeFunction, Config: map[string]interface{}{"function": "Tick"}}},
	})

	_ = engine.CronManager.Start(ctx)
	defer engine.CronManager.Stop()

	id, err := engine.CronManager.AddSchedule(ctx, ns, "tick-wf", "0 0 9 * * *", `{}`)
	if err != nil {
		t.Fatalf("AddSchedule: %v", err)
	}
	sched, err := engine.CronManager.GetSchedule(ctx, ns, id)
	if err != nil || sched.WorkflowID != "tick-wf" {
		t.Fatalf("GetSchedule: %v %#v", err, sched)
	}

	// Reload from storage via new engine instance (simulates restart).
	engine2 := flow.NewEngine(storage, registry)
	_ = engine2.CronManager.Start(ctx)
	defer engine2.CronManager.Stop()

	sched2, err := engine2.CronManager.GetSchedule(ctx, ns, id)
	if err != nil || !sched2.IsActive {
		t.Fatalf("reload schedule: %v %#v", err, sched2)
	}
}
