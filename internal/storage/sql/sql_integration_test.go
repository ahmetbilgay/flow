//go:build integration

package sql

import (
	"context"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/dgmos/flow/internal/storage/integrationtest"
)

func openMySQL(t *testing.T) *SQLStorage {
	t.Helper()
	db, err := sqlx.Connect("mysql", integrationtest.MySQLDSN())
	integrationtest.SkipIfUnavailable(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewSQLStorageWithOptions(db, "mysql", Options{
		TablePrefix: "it_" + integrationtest.UniqueID()[:8] + "_",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("NewSQLStorageWithOptions: %v", err)
	}
	return store
}

func openPostgres(t *testing.T) *SQLStorage {
	t.Helper()
	db, err := sqlx.Connect("postgres", integrationtest.PostgresDSN())
	integrationtest.SkipIfUnavailable(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewSQLStorageWithOptions(db, "postgres", Options{
		TablePrefix: "it_" + integrationtest.UniqueID()[:8] + "_",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("NewSQLStorageWithOptions: %v", err)
	}
	return store
}

func runStorageContract(t *testing.T, store *SQLStorage) {
	t.Helper()
	ctx := context.Background()
	ns := integrationtest.UniqueNamespace()
	now := time.Now().UTC()

	wf := &models.Workflow{
		ID: "wf-1", Namespace: ns, Name: "Integration WF", Version: 1,
		Status: models.WorkflowActive, CreatedAt: now, UpdatedAt: now,
		Nodes: []models.Node{{ID: "n1", Type: "log", Config: map[string]interface{}{"message": "hi"}}},
	}
	if err := store.SaveWorkflow(ctx, ns, wf); err != nil {
		t.Fatalf("SaveWorkflow: %v", err)
	}
	got, err := store.GetWorkflow(ctx, ns, "wf-1")
	if err != nil || got.Name != "Integration WF" {
		t.Fatalf("GetWorkflow: err=%v wf=%#v", err, got)
	}

	exec := &models.Execution{
		ID: "exec-1", Namespace: ns, WorkflowID: "wf-1", Version: 1,
		Status: models.TaskRunning, Input: `{}`, Variables: map[string]interface{}{"k": "v"},
		StartedAt: now,
	}
	if err := store.CreateExecution(ctx, ns, exec); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	exec.Variables["k"] = "updated"
	if err := store.UpdateExecution(ctx, ns, exec); err != nil {
		t.Fatalf("UpdateExecution: %v", err)
	}
	execGot, err := store.GetExecution(ctx, ns, "exec-1")
	if err != nil || execGot.Variables["k"] != "updated" {
		t.Fatalf("GetExecution variables: err=%v vars=%#v", err, execGot.Variables)
	}

	step := &models.ExecutionStep{
		ID: "step-1", Namespace: ns, ExecutionID: "exec-1", NodeID: "n1",
		Status: models.TaskPending, Input: `{}`, StartedAt: now, UpdatedAt: now,
	}
	if err := store.CreateExecutionStep(ctx, ns, step); err != nil {
		t.Fatalf("CreateExecutionStep: %v", err)
	}

	claimed, err := store.ClaimReadyStep(ctx, ns, "worker-1")
	if err != nil {
		t.Fatalf("ClaimReadyStep: %v", err)
	}
	if claimed == nil || claimed.ID != "step-1" || claimed.Status != models.TaskRunning {
		t.Fatalf("unexpected claim: %#v", claimed)
	}

	waitStep := &models.ExecutionStep{
		ID: "step-w", Namespace: ns, ExecutionID: "exec-1", NodeID: "approve",
		Status: models.TaskWaiting, Input: `{}`, StartedAt: now, UpdatedAt: now,
		Assignees: []string{"u1"}, ClaimMode: models.ClaimAny,
	}
	if err := store.CreateExecutionStep(ctx, ns, waitStep); err != nil {
		t.Fatalf("CreateExecutionStep waiting: %v", err)
	}
	waiting, err := store.ListWaitingSteps(ctx, ns, storage.TaskFilter{Assignee: "u1"})
	if err != nil || len(waiting) != 1 {
		t.Fatalf("ListWaitingSteps: err=%v len=%d", err, len(waiting))
	}

	sched := &models.Schedule{
		ID: integrationtest.UniqueID(), Namespace: ns, WorkflowID: "wf-1",
		CronExpression: "0 0 9 * * *", Input: `{}`, Timezone: "UTC", IsActive: true,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveSchedule(ctx, sched); err != nil {
		t.Fatalf("SaveSchedule: %v", err)
	}
	active, err := store.GetActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("GetActiveSchedules: %v", err)
	}
	found := false
	for _, s := range active {
		if s.ID == sched.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("schedule %s not in active list", sched.ID)
	}

	rec := &models.CompensationRecord{
		ID: integrationtest.UniqueID(), Namespace: ns, ExecutionID: "exec-1",
		FailedNodeID: "n1", CompensateNodeID: "refund", Reason: "test", Actor: "admin",
		CreatedAt: now,
	}
	if err := store.AppendCompensationRecord(ctx, ns, rec); err != nil {
		t.Fatalf("AppendCompensationRecord: %v", err)
	}
	journal, err := store.ListCompensationRecords(ctx, ns, "exec-1")
	if err != nil || len(journal) != 1 {
		t.Fatalf("ListCompensationRecords: err=%v len=%d", err, len(journal))
	}
}

func TestMySQLStorageIntegration(t *testing.T) {
	runStorageContract(t, openMySQL(t))
}

func TestPostgresStorageIntegration(t *testing.T) {
	runStorageContract(t, openPostgres(t))
}
