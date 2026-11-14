package sql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dgmos/flow/internal/models"
)

func TestMapStepNullColumns(t *testing.T) {
	s := &SQLStorage{}
	now := time.Now()
	row := &stepRow{
		ID:            "step-1",
		Namespace:     "ns",
		ExecutionID:   "exec-1",
		NodeID:        "n1",
		Status:        string(models.TaskPending),
		Input:         sql.NullString{String: `{"a":1}`, Valid: true},
		Output:        sql.NullString{Valid: false}, // NULL — PENDING steps
		Error:         sql.NullString{Valid: false},
		LabelsJSON:    nil,
		TaskMeta:      []byte(`{"assignees":["u1"],"claimMode":"any"}`),
		AttemptNumber: 0,
		WorkerID:      sql.NullString{Valid: false},
		StartedAt:     now,
		UpdatedAt:     now,
	}

	step := s.mapStep(row)
	if step.Output != "" {
		t.Fatalf("expected empty output for NULL, got %q", step.Output)
	}
	if step.Error != "" {
		t.Fatalf("expected empty error for NULL, got %q", step.Error)
	}
	if step.WorkerID != "" {
		t.Fatalf("expected empty worker_id for NULL, got %q", step.WorkerID)
	}
	if step.Input != `{"a":1}` {
		t.Fatalf("input: %q", step.Input)
	}
	if len(step.Assignees) != 1 || step.Assignees[0] != "u1" {
		t.Fatalf("assignees: %#v", step.Assignees)
	}
}

func TestMapStepLabelsJSON(t *testing.T) {
	s := &SQLStorage{}
	row := &stepRow{
		ID:          "s",
		Namespace:   "ns",
		ExecutionID: "e",
		NodeID:      "n",
		Status:      "WAITING",
		LabelsJSON:  []byte(`{"tenant":"t1"}`),
		StartedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	step := s.mapStep(row)
	if step.Labels["tenant"] != "t1" {
		t.Fatalf("labels: %#v", step.Labels)
	}
}

func TestMapExecNullOutputAndLabels(t *testing.T) {
	s := &SQLStorage{}
	row := &execRow{
		ID:            "e1",
		Namespace:     "ns",
		WorkflowID:    "wf",
		Version:       1,
		Status:        string(models.TaskRunning),
		Input:         sql.NullString{String: `{}`, Valid: true},
		Output:        sql.NullString{Valid: false},
		ErrorMessage:  sql.NullString{Valid: false},
		LabelsJSON:    []byte(`{"k":"v"}`),
		VariablesJSON: []byte(`{"x":1}`),
		StartedAt:     time.Now(),
	}
	exec := s.mapExec(row)
	if exec.Output != "" || exec.ErrorMessage != "" {
		t.Fatalf("NULL fields not empty: %#v %#v", exec.Output, exec.ErrorMessage)
	}
	if exec.Labels["k"] != "v" {
		t.Fatalf("labels: %#v", exec.Labels)
	}
	if exec.Variables["x"].(float64) != 1 {
		t.Fatalf("variables: %#v", exec.Variables)
	}
}

func TestJSONColumnArg(t *testing.T) {
	if jsonColumnArg("") != nil {
		t.Fatal("empty string should map to nil")
	}
	if jsonColumnArg("   ") != nil {
		t.Fatal("whitespace should map to nil")
	}
	if jsonColumnArg(`{"a":1}`) != `{"a":1}` {
		t.Fatal("non-empty JSON should be preserved")
	}
}
