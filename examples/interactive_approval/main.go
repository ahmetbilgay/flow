package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dgmos/flow"
)

// Interactive approval demo: human task + Advance/CompleteTask without a worker.
func main() {
	ctx := context.Background()
	storage := flow.NewMemoryStorage()
	registry := flow.NewRegistry()
	eng := flow.NewEngine(storage, registry)

	eng.SetHostHooks(hookFunc(func(ctx context.Context, event flow.Event) error {
		fmt.Printf("[hook] %s exec=%v node=%v\n", event.Type, event.Execution != nil, event.Step != nil && event.Step.NodeID != "")
		return nil
	}))

	wf := &flow.Workflow{
		ID:      "approval-demo",
		Name:    "Manager Approval",
		Version: 1,
		Nodes: []flow.Node{
			{
				ID:   "approve",
				Type: flow.NodeTypeHumanTask,
				Name: "Yönetici Onayı",
				Config: map[string]interface{}{
					"assignees":       []string{"manager@example.com"},
					"claim_mode":      "any",
					"execution_mode":  "interactive",
				},
				Meta: map[string]interface{}{
					"title":       "Leave Approval",
					"description": "Manager must approve or reject",
					"ui_schema":   map[string]interface{}{"fields": []string{"comment"}},
				},
			},
			{
				ID:   "notify",
				Type: flow.NodeTypeSetVariable,
				Name: "Stamp Result",
				Config: map[string]interface{}{
					"execution_mode": "interactive",
					"variables":      map[string]interface{}{"done": true},
				},
			},
		},
		Edges: []flow.Edge{
			{ID: "e1", SourceID: "approve", TargetID: "notify"},
		},
	}

	if err := eng.RegisterWorkflow(ctx, "default", wf); err != nil {
		log.Fatal(err)
	}

	execID, err := eng.StartWorkflow(ctx, "default", "approval-demo", `{"requestId":"REQ-1"}`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("started:", execID)

	tasks, err := eng.ListTasks(ctx, "default", flow.TaskFilter{ExecutionID: execID})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("waiting tasks: %d (assignees=%v)\n", len(tasks), tasks[0].Assignees)

	out, next, err := eng.CompleteTask(ctx, "default", execID, "approve", "manager@example.com", "approve", `{"comment":"LGTM"}`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("complete output:", out)
	fmt.Printf("next waiting: %d\n", len(next))

	// Advance the interactive log step
	if _, _, err := eng.Advance(ctx, "default", execID, "notify", "submit", `{}`); err != nil {
		log.Fatal(err)
	}

	exec, _ := eng.GetExecution(ctx, "default", execID)
	fmt.Println("final status:", exec.Status)
}

type hookFunc func(context.Context, flow.Event) error

func (f hookFunc) HandleEvent(ctx context.Context, event flow.Event) error {
	return f(ctx, event)
}
