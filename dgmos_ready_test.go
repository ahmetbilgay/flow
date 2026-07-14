package flow_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgmos/flow"
)

func TestInteractiveHumanTaskApprove(t *testing.T) {
	ctx := context.Background()
	storage := flow.NewMemoryStorage()
	registry := flow.NewRegistry()
	eng := flow.NewEngine(storage, registry)

	wf := &flow.Workflow{
		ID: "ht-1", Name: "HT", Version: 1,
		Nodes: []flow.Node{
			{
				ID: "approve", Type: flow.NodeTypeHumanTask, Name: "Approve",
				Config: map[string]interface{}{
					"assignees": []string{"u1"},
					"claim_mode": "any",
				},
				Meta: map[string]interface{}{"title": "Approve me"},
			},
		},
	}
	if err := eng.RegisterWorkflow(ctx, "ns", wf); err != nil {
		t.Fatal(err)
	}
	execID, err := eng.StartWorkflow(ctx, "ns", "ht-1", `{"x":1}`)
	if err != nil {
		t.Fatal(err)
	}

	tasks, err := eng.ListTasks(ctx, "ns", flow.TaskFilter{ExecutionID: execID})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("expected 1 waiting task, got %d err=%v", len(tasks), err)
	}
	if tasks[0].Assignees[0] != "u1" {
		t.Fatalf("assignee: %v", tasks[0].Assignees)
	}

	// Worker must not claim WAITING human tasks
	wctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	go eng.StartWorker(wctx, "ns", "w1")
	time.Sleep(150 * time.Millisecond)
	tasks, _ = eng.ListTasks(ctx, "ns", flow.TaskFilter{ExecutionID: execID})
	if len(tasks) != 1 || tasks[0].Status != flow.StatusWaiting {
		t.Fatalf("worker should not claim human task: %#v", tasks)
	}

	if err := eng.ClaimTask(ctx, "ns", execID, "approve", "u1"); err != nil {
		t.Fatal(err)
	}
	if err := eng.ReleaseTask(ctx, "ns", execID, "approve", "u1"); err != nil {
		t.Fatal(err)
	}
	out, _, err := eng.CompleteTask(ctx, "ns", execID, "approve", "u1", "approve", `{"ok":true}`)
	if err != nil {
		t.Fatal(err)
	}
	if out == "" {
		t.Fatal("expected output")
	}
	exec, _ := eng.GetExecution(ctx, "ns", execID)
	if exec.Status != flow.StatusCompleted {
		t.Fatalf("status %s", exec.Status)
	}
}

func TestDurableVariables(t *testing.T) {
	ctx := context.Background()
	storage := flow.NewMemoryStorage()
	registry := flow.NewRegistry()
	eng := flow.NewEngine(storage, registry)

	wf := &flow.Workflow{
		ID: "var-1", Name: "vars", Version: 1,
		Nodes: []flow.Node{{
			ID: "set", Type: flow.NodeTypeSetVariable, Name: "Set",
			Config: map[string]interface{}{
				"variables": map[string]interface{}{"customer_id": "C-9"},
			},
		}},
	}
	_ = eng.RegisterWorkflow(ctx, "ns", wf)
	execID, err := eng.StartWorkflow(ctx, "ns", "var-1", `{}`)
	if err != nil {
		t.Fatal(err)
	}

	wctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	go eng.StartWorker(wctx, "ns", "w1")

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		exec, _ := eng.GetExecution(ctx, "ns", execID)
		if exec.Status == flow.StatusCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	v, err := eng.GetVar(ctx, "ns", execID, "customer_id")
	if err != nil {
		t.Fatal(err)
	}
	if v != "C-9" {
		t.Fatalf("got %v", v)
	}
	_ = eng.MergeVars(ctx, "ns", execID, map[string]interface{}{"extra": 1})
	vars, _ := eng.GetVars(ctx, "ns", execID)
	if vars["extra"].(int) != 1 && vars["extra"].(float64) != 1 {
		// memory keeps int
		if vars["extra"] != 1 {
			t.Fatalf("vars %#v", vars)
		}
	}
}

func TestImportStepChainAndRollback(t *testing.T) {
	ctx := context.Background()
	wf, err := flow.ImportStepChain("legacy", "Legacy", []flow.LegacyStep{
		{ID: "a", Type: flow.NodeTypeLog, Name: "A", Config: map[string]interface{}{"message": "a"}, Next: []string{"b"}},
		{ID: "b", Type: flow.NodeTypeLog, Name: "B", Config: map[string]interface{}{"message": "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(wf.Nodes) != 2 || len(wf.Edges) != 1 {
		t.Fatalf("nodes=%d edges=%d", len(wf.Nodes), len(wf.Edges))
	}

	storage := flow.NewMemoryStorage()
	registry := flow.NewRegistry()
	eng := flow.NewEngine(storage, registry)

	var compCalled atomic.Bool
	registry.RegisterFunction("Refund", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		compCalled.Store(true)
		return map[string]interface{}{"refunded": true}, nil
	})

	wf2 := &flow.Workflow{
		ID: "rb", Name: "rb", Version: 1,
		Nodes: []flow.Node{
			{ID: "charge", Type: flow.NodeTypeFunction, Name: "Charge", Config: map[string]interface{}{"function": "Charge"}, CompensateNodeID: "refund"},
			{ID: "refund", Type: flow.NodeTypeFunction, Name: "Refund", Config: map[string]interface{}{"function": "Refund"}},
		},
	}
	registry.RegisterFunction("Charge", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"charged": true}, nil
	})
	_ = eng.RegisterWorkflow(ctx, "ns", wf2)
	execID, _ := eng.StartWorkflow(ctx, "ns", "rb", `{}`)

	wctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	go eng.StartWorker(wctx, "ns", "w1")
	time.Sleep(300 * time.Millisecond)

	if err := eng.RollbackTo(ctx, "ns", execID, "charge", "manual undo", "admin"); err != nil {
		t.Fatal(err)
	}
	jrnl, err := eng.ListCompensationJournal(ctx, "ns", execID)
	if err != nil || len(jrnl) == 0 {
		t.Fatalf("journal: %v %d", err, len(jrnl))
	}
	time.Sleep(300 * time.Millisecond)
	if !compCalled.Load() {
		t.Fatal("expected compensation function to run")
	}
}

func TestHostHooksEmit(t *testing.T) {
	ctx := context.Background()
	storage := flow.NewMemoryStorage()
	registry := flow.NewRegistry()
	eng := flow.NewEngine(storage, registry)

	seen := map[flow.EventType]bool{}
	eng.SetHostHooks(hook(func(ctx context.Context, e flow.Event) error {
		seen[e.Type] = true
		return nil
	}))

	wf := &flow.Workflow{
		ID: "h", Name: "h", Version: 1,
		Nodes: []flow.Node{{
			ID: "t", Type: flow.NodeTypeHumanTask, Name: "T",
			Config: map[string]interface{}{"assignees": []string{"a"}},
		}},
	}
	_ = eng.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := eng.StartWorkflow(ctx, "ns", "h", `{}`)
	if !seen[flow.EventStepWaitingHuman] || !seen[flow.EventWorkflowStarted] {
		t.Fatalf("hooks not fired: %#v", seen)
	}
	_, _, _ = eng.CompleteTask(ctx, "ns", execID, "t", "a", "ok", `{}`)
	if !seen[flow.EventTaskCompleted] {
		t.Fatalf("missing task.completed: %#v", seen)
	}
}

func TestHybridAdvanceAndRejectClaim(t *testing.T) {
	ctx := context.Background()
	storage := flow.NewMemoryStorage()
	registry := flow.NewRegistry()
	eng := flow.NewEngine(storage, registry)
	registry.RegisterFunction("Work", func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error) {
		return map[string]interface{}{"ok": true}, nil
	})

	wf := &flow.Workflow{
		ID: "hy", Name: "hy", Version: 1,
		Nodes: []flow.Node{{
			ID: "n1", Type: flow.NodeTypeFunction, Name: "N1",
			Config: map[string]interface{}{"function": "Work", "execution_mode": "hybrid"},
		}},
	}
	_ = eng.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := eng.StartWorkflow(ctx, "ns", "hy", `{}`)
	out, _, err := eng.Advance(ctx, "ns", execID, "n1", "submit", `{"x":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if out == "" {
		t.Fatal("expected output")
	}

	wf2 := &flow.Workflow{
		ID: "claim", Name: "c", Version: 1,
		Nodes: []flow.Node{{
			ID: "t", Type: flow.NodeTypeHumanTask, Name: "T",
			Config: map[string]interface{}{"assignees": []string{"a"}, "claim_mode": "first"},
		}},
	}
	_ = eng.RegisterWorkflow(ctx, "ns", wf2)
	exec2, _ := eng.StartWorkflow(ctx, "ns", "claim", `{}`)
	_ = eng.ClaimTask(ctx, "ns", exec2, "t", "a")
	if err := eng.ClaimTask(ctx, "ns", exec2, "t", "b"); err == nil {
		t.Fatal("expected claim conflict")
	}
	if _, _, err := eng.CompleteTask(ctx, "ns", exec2, "t", "b", "x", `{}`); err == nil {
		t.Fatal("expected complete denied for wrong actor")
	}
	_, _, err = eng.CompleteTask(ctx, "ns", exec2, "t", "a", "approve", `{}`)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := flow.ImportStepChain("x", "x", nil); err == nil {
		t.Fatal("expected empty import error")
	}
	if _, err := flow.ImportStepChain("x", "x", []flow.LegacyStep{{ID: "a", Next: []string{"missing"}}}); err == nil {
		t.Fatal("expected missing target error")
	}
}

type hook func(context.Context, flow.Event) error

func (h hook) HandleEvent(ctx context.Context, e flow.Event) error { return h(ctx, e) }
