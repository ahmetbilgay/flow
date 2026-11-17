package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage/memory"
)

// echoNode returns its config "output" (or the input) unchanged.
type echoNode struct{}

func (echoNode) Execute(_ context.Context, cfg map[string]interface{}, input string) (string, error) {
	if out, ok := cfg["output"].(string); ok {
		return out, nil
	}
	return input, nil
}
func (echoNode) Validate(cfg map[string]interface{}) error {
	if _, bad := cfg["invalid"]; bad {
		return errors.New("invalid config")
	}
	return nil
}

func newTestEngine(t *testing.T) (*Engine, *memory.MemoryStorage) {
	t.Helper()
	store := memory.NewMemoryStorage()
	reg := NewRegistry()
	reg.Register("echo", echoNode{})
	e := NewEngine(store, reg)
	return e, store
}

func node(id string, cfg map[string]interface{}) models.Node {
	if cfg == nil {
		cfg = map[string]interface{}{}
	}
	return models.Node{ID: id, Type: "echo", Config: cfg}
}

func edge(from, to, cond string) models.Edge {
	return models.Edge{ID: from + "->" + to, SourceID: from, TargetID: to, Condition: cond}
}

func stepsByNode(t *testing.T, store *memory.MemoryStorage, execID string) map[string][]*models.ExecutionStep {
	t.Helper()
	steps, err := store.GetExecutionSteps(context.Background(), "ns", execID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]*models.ExecutionStep{}
	for _, s := range steps {
		out[s.NodeID] = append(out[s.NodeID], s)
	}
	return out
}

// completeNode runs CompleteStep for the forward step of nodeID.
func completeNode(t *testing.T, e *Engine, store *memory.MemoryStorage, execID, nodeID, output string) error {
	t.Helper()
	steps := stepsByNode(t, store, execID)[nodeID]
	if len(steps) != 1 {
		t.Fatalf("expected one step for %s, got %d", nodeID, len(steps))
	}
	return e.CompleteStep(context.Background(), steps[0], output)
}

func TestJoinNodeCreatedOnceUnderConcurrency(t *testing.T) {
	for i := 0; i < 50; i++ {
		e, store := newTestEngine(t)
		ctx := context.Background()
		wf := &models.Workflow{
			ID:    "join",
			Nodes: []models.Node{node("a", nil), node("b", nil), node("c", nil)},
			Edges: []models.Edge{edge("a", "c", ""), edge("b", "c", "")},
		}
		if err := e.RegisterWorkflow(ctx, "ns", wf); err != nil {
			t.Fatal(err)
		}
		execID, err := e.StartWorkflow(ctx, "ns", "join", `{}`)
		if err != nil {
			t.Fatal(err)
		}
		initial := stepsByNode(t, store, execID)
		var wg sync.WaitGroup
		for _, id := range []string{"a", "b"} {
			s := initial[id][0]
			wg.Add(1)
			go func(s *models.ExecutionStep) {
				defer wg.Done()
				if err := e.CompleteStep(ctx, s, fmt.Sprintf(`{"from":%q}`, s.NodeID)); err != nil {
					t.Error(err)
				}
			}(s)
		}
		wg.Wait()

		if got := len(stepsByNode(t, store, execID)["c"]); got != 1 {
			t.Fatalf("iteration %d: join node c created %d times, want 1", i, got)
		}
	}
}

func TestJoinReceivesAggregatedInput(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	wf := &models.Workflow{
		ID:    "join",
		Nodes: []models.Node{node("a", nil), node("b", nil), node("c", nil)},
		Edges: []models.Edge{edge("a", "c", ""), edge("b", "c", "")},
	}
	_ = e.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := e.StartWorkflow(ctx, "ns", "join", `{}`)
	if err := completeNode(t, e, store, execID, "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if n := len(stepsByNode(t, store, execID)["c"]); n != 0 {
		t.Fatalf("c scheduled before b finished")
	}
	if err := completeNode(t, e, store, execID, "b", `{"v":2}`); err != nil {
		t.Fatal(err)
	}
	c := stepsByNode(t, store, execID)["c"][0]
	var in map[string]map[string]float64
	if err := json.Unmarshal([]byte(c.Input), &in); err != nil {
		t.Fatalf("bad input %q: %v", c.Input, err)
	}
	if in["a"]["v"] != 1 || in["b"]["v"] != 2 {
		t.Fatalf("unexpected aggregated input: %s", c.Input)
	}
}

// cond -(true)-> yes -> join ; cond -(false)-> no -> join
// The untaken branch must be skipped, and join must still run exactly once
// with the input of the taken branch.
func TestBranchMergeSkipsDeadPathAndRunsJoin(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	wf := &models.Workflow{
		ID: "branch",
		Nodes: []models.Node{
			node("cond", nil), node("yes", nil), node("no", nil), node("join", nil), node("after", nil),
		},
		Edges: []models.Edge{
			edge("cond", "yes", "true"), edge("cond", "no", "false"),
			edge("yes", "join", ""), edge("no", "join", ""), edge("join", "after", ""),
		},
	}
	_ = e.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := e.StartWorkflow(ctx, "ns", "branch", `{}`)

	if err := completeNode(t, e, store, execID, "cond", `{"branch":"true"}`); err != nil {
		t.Fatal(err)
	}
	got := stepsByNode(t, store, execID)
	if got["no"][0].Status != models.TaskSkipped {
		t.Fatalf("no: want SKIPPED, got %s", got["no"][0].Status)
	}
	if len(got["join"]) != 0 {
		t.Fatalf("join must wait for yes")
	}

	if err := completeNode(t, e, store, execID, "yes", `{"ok":true}`); err != nil {
		t.Fatal(err)
	}
	got = stepsByNode(t, store, execID)
	if len(got["join"]) != 1 || got["join"][0].Status != models.TaskPending {
		t.Fatalf("join: want one PENDING step, got %+v", got["join"])
	}
	if !strings.Contains(got["join"][0].Input, `"ok":true`) {
		t.Fatalf("join input should contain yes output, got %s", got["join"][0].Input)
	}
}

// A node whose only incoming edges are dead is skipped, and the skip
// propagates down the chain until the execution completes.
func TestSkipPropagatesAndExecutionCompletes(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	wf := &models.Workflow{
		ID:    "skipchain",
		Nodes: []models.Node{node("cond", nil), node("x", nil), node("y", nil)},
		Edges: []models.Edge{edge("cond", "x", "true"), edge("x", "y", "")},
	}
	_ = e.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := e.StartWorkflow(ctx, "ns", "skipchain", `{}`)
	if err := completeNode(t, e, store, execID, "cond", `{"branch":"false","r":1}`); err != nil {
		t.Fatal(err)
	}
	got := stepsByNode(t, store, execID)
	if got["x"][0].Status != models.TaskSkipped || got["y"][0].Status != models.TaskSkipped {
		t.Fatalf("x and y should be skipped")
	}
	exec, _ := store.GetExecution(ctx, "ns", execID)
	if exec.Status != models.TaskCompleted {
		t.Fatalf("execution status = %s, want COMPLETED", exec.Status)
	}
	if exec.Output != `{"branch":"false","r":1}` {
		t.Fatalf("execution output = %s", exec.Output)
	}
}

func TestRegisterWorkflowRejectsCycle(t *testing.T) {
	e, _ := newTestEngine(t)
	wf := &models.Workflow{
		ID:    "cycle",
		Nodes: []models.Node{node("a", nil), node("b", nil)},
		Edges: []models.Edge{edge("a", "b", ""), edge("b", "a", "")},
	}
	if err := e.RegisterWorkflow(context.Background(), "ns", wf); err == nil {
		t.Fatal("expected cycle to be rejected")
	}
}

func TestStartWorkflowInvalidConfigLeavesNoExecution(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	wf := &models.Workflow{
		ID:    "bad",
		Nodes: []models.Node{node("a", map[string]interface{}{"invalid": true})},
	}
	_ = e.RegisterWorkflow(ctx, "ns", wf)
	if _, err := e.StartWorkflow(ctx, "ns", "bad", `{}`); err == nil {
		t.Fatal("expected validation error")
	}
	execs, _ := store.ListExecutions(ctx, "ns")
	if len(execs) != 0 {
		t.Fatalf("expected no executions, got %d", len(execs))
	}
}

func TestCancelledExecutionNotOverwrittenByFinish(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	wf := &models.Workflow{ID: "one", Nodes: []models.Node{node("a", nil)}}
	_ = e.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := e.StartWorkflow(ctx, "ns", "one", `{}`)
	if err := e.CancelExecution(ctx, "ns", execID); err != nil {
		t.Fatal(err)
	}
	if err := completeNode(t, e, store, execID, "a", `{}`); err != nil {
		t.Fatal(err)
	}
	exec, _ := store.GetExecution(ctx, "ns", execID)
	if exec.Status != models.TaskCancelled {
		t.Fatalf("status = %s, want CANCELLED", exec.Status)
	}
}

func TestCalculateBackoffIsExponentialAndCapped(t *testing.T) {
	w := &Worker{}
	p := &models.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: 5 * time.Second}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i, d := range want {
		if got := w.calculateBackoff(p, i+1); got != d {
			t.Errorf("attempt %d: got %s, want %s", i+1, got, d)
		}
	}
}

func TestCompensationInputIsValidJSON(t *testing.T) {
	in := compensationInput("pay", errors.New(`upstream said "no"`))
	var m map[string]string
	if err := json.Unmarshal([]byte(in), &m); err != nil {
		t.Fatalf("invalid JSON %q: %v", in, err)
	}
	if m["error"] != `upstream said "no"` {
		t.Fatalf("error not preserved: %q", m["error"])
	}
}

func TestMemoryHeartbeatKeepsLease(t *testing.T) {
	ctx := context.Background()
	store := memory.NewMemoryStorage()
	store.ZombieThreshold = 50 * time.Millisecond
	_ = store.CreateExecutionStep(ctx, "ns", &models.ExecutionStep{ID: "s1", ExecutionID: "e", NodeID: "a", Status: models.TaskPending})

	claimed, _ := store.ClaimReadyStep(ctx, "ns", "w1")
	if claimed == nil {
		t.Fatal("expected claim")
	}
	time.Sleep(30 * time.Millisecond)
	if ok, _ := store.HeartbeatStep(ctx, "ns", "s1", "w1"); !ok {
		t.Fatal("heartbeat should succeed for owner")
	}
	time.Sleep(30 * time.Millisecond)
	if s, _ := store.ClaimReadyStep(ctx, "ns", "w2"); s != nil {
		t.Fatal("step with fresh heartbeat must not be reclaimed")
	}
	if ok, _ := store.HeartbeatStep(ctx, "ns", "s1", "w2"); ok {
		t.Fatal("non-owner heartbeat must fail")
	}
	time.Sleep(60 * time.Millisecond)
	if s, _ := store.ClaimReadyStep(ctx, "ns", "w2"); s == nil {
		t.Fatal("stale step should be reclaimable")
	}
}

// Worker A marks "a" COMPLETED but pauses before scheduling "join"; worker B
// then finishes the last other branch. B must schedule "join" instead of
// finishing the execution and orphaning it.
func TestFinishDoesNotOrphanPendingJoin(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	wf := &models.Workflow{
		ID:    "race",
		Nodes: []models.Node{node("a", nil), node("b", nil), node("join", nil), node("leaf", nil)},
		Edges: []models.Edge{edge("a", "join", ""), edge("b", "join", ""), edge("b", "leaf", "")},
	}
	_ = e.RegisterWorkflow(ctx, "ns", wf)
	execID, _ := e.StartWorkflow(ctx, "ns", "race", `{}`)
	if err := completeNode(t, e, store, execID, "b", `{}`); err != nil {
		t.Fatal(err)
	}
	// Worker A: persist completion of "a" only.
	a := stepsByNode(t, store, execID)["a"][0]
	a.Status = models.TaskCompleted
	_ = store.UpdateExecutionStep(ctx, "ns", a)
	// Worker B: finishes "leaf".
	if err := completeNode(t, e, store, execID, "leaf", `{}`); err != nil {
		t.Fatal(err)
	}
	if exec, _ := store.GetExecution(ctx, "ns", execID); exec.Status != models.TaskRunning {
		t.Fatalf("execution finished with join unscheduled: %s", exec.Status)
	}
	if got := stepsByNode(t, store, execID)["join"]; len(got) != 1 || got[0].Status != models.TaskPending {
		t.Fatalf("join should be scheduled by the finisher: %+v", got)
	}
}

func TestConcurrentResumeOnlyOneWins(t *testing.T) {
	for i := 0; i < 30; i++ {
		e, store := newTestEngine(t)
		ctx := context.Background()
		wf := &models.Workflow{ID: "w", Nodes: []models.Node{node("a", nil), node("b", nil)}, Edges: []models.Edge{edge("a", "b", "")}}
		_ = e.RegisterWorkflow(ctx, "ns", wf)
		execID, _ := e.StartWorkflow(ctx, "ns", "w", `{}`)
		a := stepsByNode(t, store, execID)["a"][0]
		a.Status = models.TaskWaiting
		_ = store.UpdateExecutionStep(ctx, "ns", a)

		var wg sync.WaitGroup
		results := make(chan error, 2)
		for _, out := range []string{`{"approved":true}`, `{"approved":false}`} {
			s := *a
			wg.Add(1)
			go func(s models.ExecutionStep, out string) {
				defer wg.Done()
				results <- e.CompleteStep(ctx, &s, out)
			}(s, out)
		}
		wg.Wait()
		close(results)
		ok, conflict := 0, 0
		for err := range results {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrStepConflict):
				conflict++
			default:
				t.Fatal(err)
			}
		}
		if ok != 1 || conflict != 1 {
			t.Fatalf("want exactly one winner, got ok=%d conflict=%d", ok, conflict)
		}
		if n := len(stepsByNode(t, store, execID)["b"]); n != 1 {
			t.Fatalf("child scheduled %d times", n)
		}
	}
}

func TestZombieWorkerCannotOverwrite(t *testing.T) {
	e, store := newTestEngine(t)
	ctx := context.Background()
	_ = store.CreateExecutionStep(ctx, "ns", &models.ExecutionStep{ID: "s", ExecutionID: "x", NodeID: "a", Status: models.TaskRunning, WorkerID: "new-owner"})
	stale := &models.ExecutionStep{ID: "s", Namespace: "ns", ExecutionID: "x", NodeID: "a", Status: models.TaskFailed, WorkerID: "old-owner"}
	ok, err := e.updateStepIf(ctx, stale, models.TaskRunning, "old-owner")
	if err != nil || ok {
		t.Fatalf("stale owner must not write: ok=%v err=%v", ok, err)
	}
	steps, _ := store.GetExecutionSteps(ctx, "ns", "x")
	if steps[0].Status != models.TaskRunning || steps[0].WorkerID != "new-owner" {
		t.Fatalf("step overwritten: %+v", steps[0])
	}
}
