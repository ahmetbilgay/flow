//go:build integration

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/dgmos/flow/internal/storage/integrationtest"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

func runClaimIsolationAndLease(t *testing.T, store *SQLStorage) {
	t.Helper()
	ctx := context.Background()
	nsA := integrationtest.UniqueNamespace()
	nsB := integrationtest.UniqueNamespace()
	now := time.Now().UTC()

	if err := store.CreateExecutionStep(ctx, nsB, &models.ExecutionStep{
		ID: integrationtest.UniqueID(), ExecutionID: "exec-b", NodeID: "n1", Status: models.TaskPending, StartedAt: now,
	}); err != nil {
		t.Fatalf("CreateExecutionStep: %v", err)
	}
	// A worker pinned to namespace A must not see namespace B's pending work.
	if s, err := store.ClaimReadyStep(ctx, nsA, "worker-a"); err != nil || s != nil {
		t.Fatalf("namespace leak: claimed %+v err=%v", s, err)
	}
	claimed, err := store.ClaimReadyStep(ctx, nsB, "worker-b")
	if err != nil || claimed == nil {
		t.Fatalf("ClaimReadyStep nsB: %v %+v", err, claimed)
	}

	// Duplicate IDs are reported as ErrStepExists.
	dupErr := store.CreateExecutionStep(ctx, nsB, &models.ExecutionStep{
		ID: claimed.ID, ExecutionID: "exec-b", NodeID: "n1", Status: models.TaskPending, StartedAt: now,
	})
	if dupErr != storage.ErrStepExists {
		t.Fatalf("duplicate create: got %v, want ErrStepExists", dupErr)
	}

	if ok, err := store.HeartbeatStep(ctx, nsB, claimed.ID, "worker-b"); err != nil || !ok {
		t.Fatalf("owner heartbeat: ok=%v err=%v", ok, err)
	}
	if ok, err := store.HeartbeatStep(ctx, nsB, claimed.ID, "someone-else"); err != nil || ok {
		t.Fatalf("non-owner heartbeat: ok=%v err=%v", ok, err)
	}
}

func TestMySQLClaimIsolation(t *testing.T)    { runClaimIsolationAndLease(t, openMySQL(t)) }
func TestPostgresClaimIsolation(t *testing.T) { runClaimIsolationAndLease(t, openPostgres(t)) }

func runCAS(t *testing.T, store *SQLStorage) {
	t.Helper()
	ctx := context.Background()
	ns := integrationtest.UniqueNamespace()
	id := integrationtest.UniqueID()
	step := &models.ExecutionStep{ID: id, ExecutionID: "e", NodeID: "n", Status: models.TaskWaiting, StartedAt: time.Now()}
	if err := store.CreateExecutionStep(ctx, ns, step); err != nil {
		t.Fatal(err)
	}
	step.Status = models.TaskCompleted
	if ok, err := store.UpdateExecutionStepIf(ctx, ns, step, models.TaskWaiting, ""); err != nil || !ok {
		t.Fatalf("first CAS: %v %v", ok, err)
	}
	if ok, err := store.UpdateExecutionStepIf(ctx, ns, step, models.TaskWaiting, ""); err != nil || ok {
		t.Fatalf("second CAS must fail: %v %v", ok, err)
	}
	exec := &models.Execution{ID: integrationtest.UniqueID(), WorkflowID: "w", Status: models.TaskRunning, StartedAt: time.Now()}
	if err := store.CreateExecution(ctx, ns, exec); err != nil {
		t.Fatal(err)
	}
	exec.Status = models.TaskCompleted
	if ok, _ := store.UpdateExecutionIf(ctx, ns, exec, models.TaskRunning); !ok {
		t.Fatal("execution CAS should apply")
	}
	if ok, _ := store.UpdateExecutionIf(ctx, ns, exec, models.TaskRunning); ok {
		t.Fatal("execution CAS must not apply twice")
	}
}

func TestMySQLCAS(t *testing.T)    { runCAS(t, openMySQL(t)) }
func TestPostgresCAS(t *testing.T) { runCAS(t, openPostgres(t)) }
