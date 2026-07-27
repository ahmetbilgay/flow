package trigger

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/engine"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// APIManager handles management API requests
type APIManager struct {
	webhookMgr *WebhookManager
	cronMgr    *CronManager
}

func NewAPIManager(wm *WebhookManager) *APIManager {
	return &APIManager{webhookMgr: wm}
}

func (m *APIManager) SetCronManager(c *CronManager) *APIManager {
	m.cronMgr = c
	return m
}

func (m *APIManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	if path == "metrics" {
		promhttp.Handler().ServeHTTP(w, r)
		return
	}
	if path == "health" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
		return
	}
	if strings.HasPrefix(path, "api/") {
		m.handleManagementAPI(w, r, path)
		return
	}
	if strings.HasPrefix(path, "webhooks/") {
		m.webhookMgr.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

func (m *APIManager) handleManagementAPI(w http.ResponseWriter, r *http.Request, path string) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid API path", http.StatusBadRequest)
		return
	}
	namespace, resource := parts[1], parts[2]
	switch resource {
	case "executions":
		m.handleExecutions(w, r, namespace, parts)
	case "schedules":
		m.handleSchedules(w, r, namespace, parts)
	case "tasks":
		m.handleTasks(w, r, namespace, parts)
	default:
		http.NotFound(w, r)
	}
}

func (m *APIManager) requireAccess(w http.ResponseWriter, r *http.Request, resource, action string) bool {
	if err := m.webhookMgr.engine.CheckAccess(r.Context(), resource, action); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return false
	}
	return true
}

func (m *APIManager) handleExecutions(w http.ResponseWriter, r *http.Request, namespace string, parts []string) {
	eng := m.webhookMgr.engine
	ctx := r.Context()

	if len(parts) == 3 {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !m.requireAccess(w, r, fmt.Sprintf("execution:%s:*", namespace), "view") {
			return
		}
		execs, err := eng.ListExecutions(ctx, namespace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if status := r.URL.Query().Get("status"); status != "" {
			filtered := execs[:0]
			for _, exec := range execs {
				if string(exec.Status) == status {
					filtered = append(filtered, exec)
				}
			}
			execs = filtered
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"namespace": namespace, "executions": execs})
		return
	}
	if len(parts) == 4 {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		exec, err := eng.GetExecutionStatus(ctx, namespace, parts[3])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, exec)
		return
	}
	if len(parts) == 5 && parts[4] == "steps" {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !m.requireAccess(w, r, fmt.Sprintf("execution:%s:%s", namespace, parts[3]), "view") {
			return
		}
		steps, err := eng.GetExecutionSteps(ctx, namespace, parts[3])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"execution_id": parts[3], "steps": steps})
		return
	}
	if len(parts) == 5 && parts[4] == "rollback" {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			NodeID string `json:"node_id"`
			Reason string `json:"reason"`
			Actor  string `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		if err := eng.RollbackTo(ctx, namespace, parts[3], body.NodeID, body.Reason, body.Actor); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "rollback_started"})
		return
	}
	if len(parts) == 7 && parts[4] == "tasks" {
		m.handleExecutionTask(w, r, namespace, parts[3], parts[5], parts[6])
		return
	}
	if len(parts) == 5 && parts[4] == "cancel" {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := m.webhookMgr.engine.CancelExecution(r.Context(), namespace, parts[3]); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
		return
	}
	if len(parts) == 6 && parts[4] == "signal" {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Output string `json:"output"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		if err := m.webhookMgr.engine.SignalExecution(r.Context(), namespace, parts[3], parts[5], body.Output); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "signaled"})
		return
	}
	http.NotFound(w, r)
}

func (m *APIManager) handleTasks(w http.ResponseWriter, r *http.Request, namespace string, parts []string) {
	if len(parts) != 3 || r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !m.requireAccess(w, r, fmt.Sprintf("task:%s:*", namespace), "view") {
		return
	}
	filter := engine.TaskFilter{}
	if v := r.URL.Query().Get("execution_id"); v != "" {
		filter.ExecutionID = v
	}
	if v := r.URL.Query().Get("assignee"); v != "" {
		filter.Assignee = v
	}
	if v := r.URL.Query().Get("node_id"); v != "" {
		filter.NodeID = v
	}
	tasks, err := m.webhookMgr.engine.ListTasks(r.Context(), namespace, filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"namespace": namespace, "tasks": tasks})
}

func (m *APIManager) handleExecutionTask(w http.ResponseWriter, r *http.Request, namespace, execID, nodeID, taskAction string) {
	eng := m.webhookMgr.engine
	ctx := r.Context()

	switch taskAction {
	case "claim":
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Actor string `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		if err := eng.ClaimTask(ctx, namespace, execID, nodeID, body.Actor); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "claimed"})
	case "release":
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Actor string `json:"actor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		if err := eng.ReleaseTask(ctx, namespace, execID, nodeID, body.Actor); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
	case "complete":
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Actor   string `json:"actor"`
			Action  string `json:"action"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		out, waiting, err := eng.CompleteTask(ctx, namespace, execID, nodeID, body.Actor, body.Action, body.Payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"output": out, "waiting_tasks": waiting})
	case "advance":
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Action  string `json:"action"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		out, waiting, err := eng.Advance(ctx, namespace, execID, nodeID, body.Action, body.Payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"output": out, "waiting_tasks": waiting})
	default:
		http.NotFound(w, r)
	}
}

func (m *APIManager) handleSchedules(w http.ResponseWriter, r *http.Request, namespace string, parts []string) {
	if m.cronMgr == nil {
		http.Error(w, "schedules API not enabled (CronManager not set)", http.StatusNotImplemented)
		return
	}
	ctx := r.Context()

	if len(parts) == 3 {
		switch r.Method {
		case http.MethodGet:
			filter := ScheduleListFilter{
				ActiveOnly: r.URL.Query().Get("active") == "true" || r.URL.Query().Get("active_only") == "true",
			}
			if v := r.URL.Query().Get("limit"); v != "" {
				filter.Limit, _ = strconv.Atoi(v)
			}
			if v := r.URL.Query().Get("offset"); v != "" {
				filter.Offset, _ = strconv.Atoi(v)
			}
			list, err := m.cronMgr.ListSchedulesFiltered(ctx, namespace, filter)
			if err != nil {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			writeJSON(w, http.StatusOK, map[string]interface{}{"namespace": namespace, "schedules": list})
			return
		case http.MethodPost:
			var body scheduleCreateRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "Invalid body", http.StatusBadRequest)
				return
			}
			opts, err := body.toCreateOpts()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			id, err := m.cronMgr.AddScheduleWithOpts(ctx, namespace, body.WorkflowID, body.CronExpression, body.Input, opts)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			sched, _ := m.cronMgr.GetSchedule(ctx, namespace, id)
			writeJSON(w, http.StatusCreated, sched)
			return
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
	}

	if len(parts) < 4 {
		http.NotFound(w, r)
		return
	}
	scheduleID := parts[3]

	if scheduleID == "preview" && r.Method == http.MethodPost {
		var body struct {
			CronExpression string `json:"cron_expression"`
			Timezone       string `json:"timezone"`
			Count          int    `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Invalid body", http.StatusBadRequest)
			return
		}
		times, err := m.cronMgr.PreviewNextRuns(body.CronExpression, body.Timezone, body.Count)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"next_runs": times})
		return
	}

	if len(parts) == 4 {
		switch r.Method {
		case http.MethodGet:
			sched, err := m.cronMgr.GetSchedule(ctx, namespace, scheduleID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, sched)
			return
		case http.MethodPut:
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				http.Error(w, "Invalid body", http.StatusBadRequest)
				return
			}
			patch, err := parseSchedulePatch(raw)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := m.cronMgr.UpdateSchedule(ctx, namespace, scheduleID, patch); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			sched, _ := m.cronMgr.GetSchedule(ctx, namespace, scheduleID)
			writeJSON(w, http.StatusOK, sched)
			return
		case http.MethodDelete:
			if err := m.cronMgr.RemoveSchedule(ctx, namespace, scheduleID); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
			return
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
	}

	if len(parts) == 5 && r.Method == http.MethodPost {
		switch parts[4] {
		case "pause":
			if err := m.cronMgr.PauseSchedule(ctx, namespace, scheduleID); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
			return
		case "resume":
			if err := m.cronMgr.ResumeSchedule(ctx, namespace, scheduleID); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
			return
		}
	}
	http.NotFound(w, r)
}

type scheduleCreateRequest struct {
	WorkflowID         string  `json:"workflow_id"`
	Name               string  `json:"name"`
	CronExpression     string  `json:"cron_expression"`
	Input              string  `json:"input"`
	Timezone           string  `json:"timezone"`
	CatchUp            bool    `json:"catch_up"`
	CatchUpMax         int     `json:"catch_up_max"`
	CatchUpLookbackSec int     `json:"catch_up_lookback_sec"`
	StartsAt           *string `json:"starts_at"`
	EndsAt             *string `json:"ends_at"`
}

func (b scheduleCreateRequest) toCreateOpts() (ScheduleOpts, error) {
	opts := ScheduleOpts{
		Name: b.Name, CatchUp: b.CatchUp, CatchUpMax: b.CatchUpMax, Timezone: b.Timezone,
		CatchUpLookback: time.Duration(b.CatchUpLookbackSec) * time.Second,
	}
	if b.StartsAt != nil {
		if *b.StartsAt == "" {
			return opts, nil
		}
		t, err := time.Parse(time.RFC3339, *b.StartsAt)
		if err != nil {
			return opts, err
		}
		opts.StartsAt = &t
	}
	if b.EndsAt != nil {
		if *b.EndsAt == "" {
			return opts, nil
		}
		t, err := time.Parse(time.RFC3339, *b.EndsAt)
		if err != nil {
			return opts, err
		}
		opts.EndsAt = &t
	}
	return opts, nil
}

func parseSchedulePatch(raw map[string]json.RawMessage) (ScheduleUpdateOpts, error) {
	var patch ScheduleUpdateOpts
	if v, ok := raw["cron_expression"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return patch, err
		}
		patch.CronExpression = &s
	}
	if v, ok := raw["input"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return patch, err
		}
		patch.Input = &s
	}
	if v, ok := raw["name"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return patch, err
		}
		patch.Name = &s
	}
	if v, ok := raw["timezone"]; ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return patch, err
		}
		patch.Timezone = &s
	}
	if v, ok := raw["catch_up"]; ok {
		var b bool
		if err := json.Unmarshal(v, &b); err != nil {
			return patch, err
		}
		patch.CatchUp = &b
	}
	if v, ok := raw["catch_up_max"]; ok {
		var n int
		if err := json.Unmarshal(v, &n); err != nil {
			return patch, err
		}
		patch.CatchUpMax = &n
	}
	if v, ok := raw["catch_up_lookback_sec"]; ok {
		var n int
		if err := json.Unmarshal(v, &n); err != nil {
			return patch, err
		}
		patch.CatchUpLookbackSec = &n
	}
	if v, ok := raw["starts_at"]; ok {
		if string(v) == "null" {
			patch.ClearStartsAt = true
		} else {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return patch, err
			}
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return patch, err
			}
			patch.StartsAt = &t
		}
	}
	if v, ok := raw["ends_at"]; ok {
		if string(v) == "null" {
			patch.ClearEndsAt = true
		} else {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return patch, err
			}
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return patch, err
			}
			patch.EndsAt = &t
		}
	}
	return patch, nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
