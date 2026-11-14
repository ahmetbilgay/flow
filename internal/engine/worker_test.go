package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage/memory"
)

// funcNode adapts a function to NodeExecutor.
type funcNode func(ctx context.Context, cfg map[string]interface{}, input string) (string, error)

func (f funcNode) Execute(ctx context.Context, cfg map[string]interface{}, input string) (string, error) {
	return f(ctx, cfg, input)
}
func (funcNode) Validate(map[string]interface{}) error { return nil }

type permErr struct{}

func (permErr) Error() string   { return "bad input" }
func (permErr) Permanent() bool { return true }

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func execStatus(store *memory.MemoryStorage, id string) models.TaskStatus {
	e, err := store.GetExecution(context.Background(), "ns", id)
	if err != nil {
		return ""
	}
	return e.Status
}

func TestWorkerRunsStepsConcurrently(t *testing.T) {
	store := memory.NewMemoryStorage()
	reg := NewRegistry()
	var inFlight, peak int32
	release := make(chan struct{})
	reg.Register("slow", funcNode(func(ctx context.Context, _ map[string]interface{}, in string) (string, error) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&inFlight, -1)
		return in, nil
	}))
	e := NewEngine(store, reg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = e.RegisterWorkflow(ctx, "ns", &models.Workflow{ID: "wf", Nodes: []models.Node{{ID: "a", Type: "slow", Config: map[string]interface{}{}}}})
	var ids []string
	for i := 0; i < 4; i++ {
		id, err := e.StartWorkflow(ctx, "ns", "wf", `{}`)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	done := make(chan struct{})
	go func() {
		e.StartWorkerWithOptions(ctx, "ns", "w1", WorkerOptions{Concurrency: 4, PollInterval: 5 * time.Millisecond})
		close(done)
	}()
	waitFor(t, 2*time.Second, func() bool { return atomic.LoadInt32(&peak) == 4 })
	close(release)
	waitFor(t, 2*time.Second, func() bool {
		for _, id := range ids {
			if execStatus(store, id) != models.TaskCompleted {
				return false
			}
		}
		return true
	})
	cancel()
	<-done
}

func TestWorkerShutdownReleasesInFlightStep(t *testing.T) {
	store := memory.NewMemoryStorage()
	reg := NewRegistry()
	started := make(chan struct{})
	var once sync.Once
	reg.Register("block", funcNode(func(ctx context.Context, _ map[string]interface{}, _ string) (string, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return "", ctx.Err()
	}))
	e := NewEngine(store, reg)
	ctx, cancel := context.WithCancel(context.Background())
	_ = e.RegisterWorkflow(ctx, "ns", &models.Workflow{ID: "wf", Nodes: []models.Node{{ID: "a", Type: "block", Config: map[string]interface{}{}}}})
	execID, _ := e.StartWorkflow(ctx, "ns", "wf", `{}`)

	done := make(chan struct{})
	go func() {
		e.StartWorkerWithOptions(ctx, "ns", "w1", WorkerOptions{PollInterval: 5 * time.Millisecond})
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}

	steps, _ := store.GetExecutionSteps(context.Background(), "ns", execID)
	if len(steps) != 1 || steps[0].Status != models.TaskPending || steps[0].AttemptNumber != 0 {
		t.Fatalf("step should be back to PENDING without a used attempt: %+v", steps[0])
	}
	if execStatus(store, execID) != models.TaskRunning {
		t.Fatalf("execution should still be RUNNING")
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	store := memory.NewMemoryStorage()
	reg := NewRegistry()
	var calls int32
	reg.Register("perm", funcNode(func(context.Context, map[string]interface{}, string) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", fmt.Errorf("wrapped: %w", permErr{})
	}))
	e := NewEngine(store, reg)
	ctx := context.Background()
	_ = e.RegisterWorkflow(ctx, "ns", &models.Workflow{ID: "wf", Nodes: []models.Node{{
		ID: "a", Type: "perm", Config: map[string]interface{}{},
		RetryPolicy: &models.RetryPolicy{MaxAttempts: 5, InitialInterval: time.Millisecond, BackoffCoefficient: 1},
	}}})
	execID, _ := e.StartWorkflow(ctx, "ns", "wf", `{}`)
	w := NewWorker("w1", e, reg, time.Millisecond)
	w.SetNamespace("ns")
	w.processOne(ctx)

	if calls != 1 {
		t.Fatalf("executor called %d times, want 1", calls)
	}
	if st := execStatus(store, execID); st != models.TaskFailed {
		t.Fatalf("execution status = %s, want FAILED", st)
	}
}

func TestExecutionTimeout(t *testing.T) {
	store := memory.NewMemoryStorage()
	reg := NewRegistry()
	reg.Register("hang", funcNode(func(ctx context.Context, _ map[string]interface{}, _ string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}))
	e := NewEngine(store, reg)
	ctx := context.Background()
	_ = e.RegisterWorkflow(ctx, "ns", &models.Workflow{ID: "wf", Nodes: []models.Node{{
		ID: "a", Type: "hang", Config: map[string]interface{}{"execution_timeout": "20ms"},
		RetryPolicy: &models.RetryPolicy{MaxAttempts: 0},
	}}})
	execID, _ := e.StartWorkflow(ctx, "ns", "wf", `{}`)
	w := NewWorker("w1", e, reg, time.Millisecond)
	w.SetNamespace("ns")

	start := time.Now()
	w.processOne(ctx)
	if time.Since(start) > time.Second {
		t.Fatal("timeout not enforced")
	}
	steps, _ := store.GetExecutionSteps(ctx, "ns", execID)
	if steps[0].Status != models.TaskFailed || !strings.Contains(steps[0].Error, "timed out") {
		t.Fatalf("want timed-out failure, got %s %q", steps[0].Status, steps[0].Error)
	}
}

func TestInvalidExecutionTimeoutRejectedAtStart(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	_ = e.RegisterWorkflow(ctx, "ns", &models.Workflow{ID: "wf", Nodes: []models.Node{node("a", map[string]interface{}{"execution_timeout": "soon"})}})
	_, err := e.StartWorkflow(ctx, "ns", "wf", `{}`)
	if err == nil || !IsPermanent(err) {
		t.Fatalf("want permanent config error, got %v", err)
	}
	if !errors.As(err, new(invalidConfigError)) {
		t.Fatalf("unexpected error type %T", err)
	}
}
