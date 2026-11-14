//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/dgmos/flow/internal/storage/integrationtest"
)

func openRedis(t *testing.T) *RedisStorage {
	t.Helper()
	addr := integrationtest.RedisAddr()
	integrationtest.SkipIfUnavailable(t, integrationtest.PingRedis(context.Background(), addr))
	return NewRedisStorage(addr, "", 15) // DB 15 reserved for integration tests
}

func runRedisContract(t *testing.T, store *RedisStorage) {
	t.Helper()
	ctx := context.Background()
	ns := integrationtest.UniqueNamespace()
	now := time.Now().UTC()

	wf := &models.Workflow{
		ID: "wf-1", Namespace: ns, Name: "Redis WF", Version: 1,
		Status: models.WorkflowActive, CreatedAt: now, UpdatedAt: now,
		Nodes: []models.Node{{ID: "n1", Type: "log"}},
	}
	if err := store.SaveWorkflow(ctx, ns, wf); err != nil {
		t.Fatalf("SaveWorkflow: %v", err)
	}
	got, err := store.GetWorkflow(ctx, ns, "wf-1")
	if err != nil || got.Name != "Redis WF" {
		t.Fatalf("GetWorkflow: %v %#v", err, got)
	}

	exec := &models.Execution{
		ID: "exec-1", Namespace: ns, WorkflowID: "wf-1", Version: 1,
		Status: models.TaskRunning, Input: `{}`, Variables: map[string]interface{}{"a": 1},
		StartedAt: now,
	}
	if err := store.CreateExecution(ctx, ns, exec); err != nil {
		t.Fatalf("CreateExecution: %v", err)
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
	if claimed == nil || claimed.ID != "step-1" {
		t.Fatalf("unexpected claim: %#v", claimed)
	}

	wait := &models.ExecutionStep{
		ID: "step-w", Namespace: ns, ExecutionID: "exec-1", NodeID: "approve",
		Status: models.TaskWaiting, Input: `{}`, StartedAt: now, UpdatedAt: now,
		Assignees: []string{"u1"},
	}
	if err := store.CreateExecutionStep(ctx, ns, wait); err != nil {
		t.Fatalf("CreateExecutionStep waiting: %v", err)
	}
	tasks, err := store.ListWaitingSteps(ctx, ns, storage.TaskFilter{Assignee: "u1"})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("ListWaitingSteps: err=%v len=%d", err, len(tasks))
	}

	sched := &models.Schedule{
		ID: integrationtest.UniqueID(), Namespace: ns, WorkflowID: "wf-1",
		CronExpression: "0 0 9 * * *", Input: `{}`, Timezone: "UTC", IsActive: true,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveSchedule(ctx, sched); err != nil {
		t.Fatalf("SaveSchedule: %v", err)
	}
	gotSched, err := store.GetSchedule(ctx, ns, sched.ID)
	if err != nil || gotSched.WorkflowID != "wf-1" {
		t.Fatalf("GetSchedule: %v %#v", err, gotSched)
	}
}

func TestRedisStorageIntegration(t *testing.T) {
	runRedisContract(t, openRedis(t))
}

func TestRedisRetryRequeueAndLease(t *testing.T) {
	store := openRedis(t)
	ctx := context.Background()
	ns := integrationtest.UniqueNamespace()
	id := integrationtest.UniqueID()
	step := &models.ExecutionStep{ID: id, ExecutionID: "e1", NodeID: "n1", Status: models.TaskPending}
	if err := store.CreateExecutionStep(ctx, ns, step); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateExecutionStep(ctx, ns, step); err != storage.ErrStepExists {
		t.Fatalf("duplicate create: got %v, want ErrStepExists", err)
	}

	claimed, err := store.ClaimReadyStep(ctx, ns, "w1")
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	if ok, err := store.HeartbeatStep(ctx, ns, id, "w1"); err != nil || !ok {
		t.Fatalf("heartbeat: ok=%v err=%v", ok, err)
	}
	if s, _ := store.ClaimReadyStep(ctx, ns, "w2"); s != nil {
		t.Fatal("running step with fresh lease must not be claimable")
	}

	// A retry puts the step back to PENDING; it must be claimable again.
	claimed.Status = models.TaskPending
	claimed.AttemptNumber = 1
	if err := store.UpdateExecutionStep(ctx, ns, claimed); err != nil {
		t.Fatal(err)
	}
	again, err := store.ClaimReadyStep(ctx, ns, "w2")
	if err != nil || again == nil || again.ID != id {
		t.Fatalf("retried step not re-queued: %v %+v", err, again)
	}

	// Finishing removes the lease.
	again.Status = models.TaskCompleted
	if err := store.UpdateExecutionStep(ctx, ns, again); err != nil {
		t.Fatal(err)
	}
	if n, _ := store.client.ZCard(ctx, runningSet(ns)).Result(); n != 0 {
		t.Fatalf("running set should be empty, has %d", n)
	}
}
