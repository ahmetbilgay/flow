package flow

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/dgmos/flow/internal/n8n"
)

// ========================================
// n8n COMPATIBILITY
// ========================================

type (
	// N8NOptions configures the n8n runtime (credentials, outbound network
	// policy, chat memory, timeouts, timezone).
	N8NOptions = n8n.Options
	// N8NImportOptions configures ImportN8NWorkflow.
	N8NImportOptions = n8n.ImportOptions
	// N8NReport describes how well an n8n workflow is supported.
	N8NReport = n8n.Report
	// N8NIssue is one finding of an N8NReport.
	N8NIssue = n8n.Issue
	// N8NTrigger describes one way an imported workflow can be started.
	N8NTrigger = n8n.TriggerSpec
	// N8NCredentialProvider resolves n8n credentials (back it with a vault).
	N8NCredentialProvider = n8n.CredentialProvider
	// N8NCredentialRef identifies a credential referenced by an n8n node.
	N8NCredentialRef = n8n.CredentialRef
	// N8NStaticCredentials is an in-memory N8NCredentialProvider.
	N8NStaticCredentials = n8n.StaticCredentials
	// N8NChatMemoryStore stores AI chat history for memory nodes.
	N8NChatMemoryStore = n8n.ChatMemoryStore
	// N8NChatMemoryReplacer is an optional extension of N8NChatMemoryStore used
	// by the Chat Memory Manager's override and delete modes.
	N8NChatMemoryReplacer = n8n.ChatMemoryReplacer
	// N8NChatMessage is one stored chat turn.
	N8NChatMessage = n8n.ChatMessage
	// N8NItem is one n8n data item.
	N8NItem = n8n.Item
	// N8NHandlerOptions configures the n8n webhook/chat/form HTTP handler.
	N8NHandlerOptions = n8n.HandlerOptions
	// N8NPollStateStore persists polling trigger cursors.
	N8NPollStateStore = n8n.PollStateStore
	// N8NPollStateSwapper is an optional extension of N8NPollStateStore that
	// advances cursors atomically so overlapping polls never emit duplicates.
	N8NPollStateSwapper = n8n.PollStateSwapper
	// N8NDataTableStore persists n8n Data Tables (N8NOptions.DataTables).
	N8NDataTableStore = n8n.DataTableStore
	// N8NDataTable is one n8n Data Table with its columns and rows.
	N8NDataTable = n8n.DataTable
	// N8NDataTableColumn is a typed Data Table column.
	N8NDataTableColumn = n8n.DataTableColumn
)

// NewN8NMemoryDataTables returns a process-local N8NDataTableStore.
func NewN8NMemoryDataTables() *n8n.MemoryDataTables { return n8n.NewMemoryDataTables() }

// NewN8NStaticCredentials returns an empty in-memory credential store.
func NewN8NStaticCredentials() *N8NStaticCredentials { return n8n.NewStaticCredentials() }

// N8NTriggerKey selects the trigger that starts an execution of an imported
// workflow: Execute(ctx, ns, id, []byte(`{"__trigger": "Schedule Trigger"}`)).
// Without it the workflow's default trigger runs.
const N8NTriggerKey = n8n.TriggerKey

// EnableN8N registers the n8n node types with this engine. Call it once,
// before registering or running imported workflows.
func (e *Engine) EnableN8N(opts N8NOptions) *Engine {
	o := opts
	e.n8nOpts = &o
	n8n.Register(e.registry.Registry, e.n8nOpts)
	return e
}

// ImportN8NWorkflow converts an n8n workflow export (or a template from the
// n8n template API) into a Workflow and reports unsupported nodes, required
// credentials and triggers. It does not register the workflow.
func ImportN8NWorkflow(data []byte, opts N8NImportOptions) (*Workflow, *N8NReport, error) {
	return n8n.Import(data, opts)
}

// N8NInstallOptions configures InstallN8NWorkflow.
type N8NInstallOptions struct {
	N8NImportOptions
	// AllowPartial registers the workflow even when the report has blocking
	// issues (nodes that are not supported fail when reached).
	AllowPartial bool
	// SkipSchedules does not create schedules for Schedule Trigger nodes.
	SkipSchedules bool
	// WebhookBaseURL is the public URL N8NHandler is mounted at (e.g.
	// "https://hooks.example.com/n8n"). When set, service triggers (GitHub,
	// Stripe, Typeform, Shopify, …) create their webhooks in the service,
	// as n8n does on activation; see Webhooks in the result.
	WebhookBaseURL string
	// SkipActivationEvents does not start n8n Triggers listening to
	// "Workflow activated" / "Workflow updated".
	SkipActivationEvents bool
	// RenameConflictingWebhooks installs a workflow whose webhook path (or
	// Telegram/WhatsApp-style webhookId) another workflow in the namespace
	// already uses under a new route instead of failing with
	// ErrN8NWebhookConflict: "-<suffix>" is appended, the suffix derived
	// from the workflow ID so a reinstall keeps the same route. The result
	// lists the changes in RenamedWebhooks.
	RenameConflictingWebhooks bool
}

// N8NWebhookRename is a webhook route RenameConflictingWebhooks changed.
type N8NWebhookRename = n8n.WebhookRename

// N8NInstallResult is returned by InstallN8NWorkflow.
type N8NInstallResult struct {
	Workflow    *Workflow
	Report      *N8NReport
	ScheduleIDs []string
	// Webhooks lists the service triggers' webhook URLs and whether they
	// were registered (Manual: enter the URL in the service yourself).
	Webhooks []N8NWebhookRegistration
	// RenamedWebhooks lists the routes RenameConflictingWebhooks changed.
	RenamedWebhooks []N8NWebhookRename
	// ActivationExecutions are the executions started for n8n Triggers
	// listening to "Workflow activated" (first install) or "Workflow
	// updated" (reinstall of an existing workflow ID).
	ActivationExecutions []string
}

// EmitN8NInstanceStarted starts the n8n Triggers of a namespace's workflows
// that listen to "Instance started"; call it once when the application
// starts. It returns the started execution IDs.
func (e *Engine) EmitN8NInstanceStarted(ctx context.Context, namespace string) ([]string, error) {
	wfs, err := e.WorkflowDefinitions(ctx, namespace)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, wf := range wfs {
		for _, input := range n8n.ActivationInputs(wf, n8n.EventInstanceStarted) {
			id, err := e.StartWorkflow(ctx, namespace, wf.ID, input)
			if err != nil {
				return ids, fmt.Errorf("workflow %s: %w", wf.ID, err)
			}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// N8NWebhookRegistration is the webhook of one service trigger.
type N8NWebhookRegistration = n8n.WebhookRegistration

// RegisterN8NWebhooks creates the webhooks of an installed workflow's
// service triggers in their services (see N8NInstallOptions.WebhookBaseURL).
func (e *Engine) RegisterN8NWebhooks(ctx context.Context, namespace, workflowID, baseURL string) ([]N8NWebhookRegistration, error) {
	wf, err := e.WorkflowDefinition(ctx, namespace, workflowID)
	if err != nil {
		return nil, err
	}
	return n8n.RegisterWebhooks(ctx, e.n8nOpts, namespace, wf, baseURL), nil
}

// UnregisterN8NWebhooks deletes the webhooks RegisterN8NWebhooks created;
// call it before removing the workflow.
func (e *Engine) UnregisterN8NWebhooks(ctx context.Context, namespace, workflowID string) error {
	wf, err := e.WorkflowDefinition(ctx, namespace, workflowID)
	if err != nil {
		return err
	}
	return n8n.UnregisterWebhooks(ctx, e.n8nOpts, namespace, wf)
}

// ErrN8NNotRunnable is returned by InstallN8NWorkflow when the workflow
// uses unsupported features and AllowPartial is false.
var ErrN8NNotRunnable = errors.New("n8n workflow uses unsupported features")

// InstallN8NWorkflow imports, registers and activates an n8n workflow:
// schedule triggers become durable schedules (requires the CronManager to
// be started), webhook/chat/form triggers are served by N8NHandler.
func (e *Engine) InstallN8NWorkflow(ctx context.Context, namespace string, data []byte, opts N8NInstallOptions) (*N8NInstallResult, error) {
	if e.n8nOpts == nil {
		return nil, errors.New("n8n support is not enabled: call EnableN8N first")
	}
	wf, rep, err := n8n.Import(data, opts.N8NImportOptions)
	if err != nil {
		return nil, err
	}
	res := &N8NInstallResult{Workflow: wf, Report: rep}
	if !rep.Runnable && !opts.AllowPartial {
		return res, fmt.Errorf("%w: %s", ErrN8NNotRunnable, rep.Blocking[0].Message)
	}
	if opts.RenameConflictingWebhooks {
		used, err := e.webhookRoutesInUse(ctx, namespace, wf.ID)
		if err != nil {
			return res, err
		}
		sum := sha1.Sum([]byte(wf.ID))
		renamed, changes, err := n8n.RenameWebhooks(data, func(k string) bool { _, ok := used[k]; return ok }, hex.EncodeToString(sum[:3]))
		if err != nil {
			return res, err
		}
		if len(changes) > 0 {
			if wf, rep, err = n8n.Import(renamed, opts.N8NImportOptions); err != nil {
				return res, err
			}
			res.Workflow, res.Report, res.RenamedWebhooks = wf, rep, changes
		}
	}
	if err := e.checkWebhookConflicts(ctx, namespace, wf); err != nil {
		return res, err
	}
	_, existsErr := e.WorkflowDefinition(ctx, namespace, wf.ID)
	if err := e.RegisterWorkflow(ctx, namespace, wf); err != nil {
		return res, err
	}
	if !opts.SkipActivationEvents {
		event := n8n.EventWorkflowActivated
		if existsErr == nil {
			event = n8n.EventWorkflowUpdated
		}
		for _, input := range n8n.ActivationInputs(wf, event) {
			id, err := e.StartWorkflow(ctx, namespace, wf.ID, input)
			if err != nil {
				return res, fmt.Errorf("n8n Trigger: %w", err)
			}
			res.ActivationExecutions = append(res.ActivationExecutions, id)
		}
	}
	if opts.WebhookBaseURL != "" {
		res.Webhooks = n8n.RegisterWebhooks(ctx, e.n8nOpts, namespace, wf, opts.WebhookBaseURL)
	}
	if opts.SkipSchedules || e.CronManager == nil {
		return res, nil
	}
	// A re-install replaces the workflow's schedules instead of adding to them.
	if err := e.removeN8NSchedules(ctx, namespace, wf.ID); err != nil {
		return res, err
	}
	tz := ""
	if s, ok := wf.Metadata["settings"].(map[string]interface{}); ok {
		if v, ok := s["timezone"].(string); ok && v != "DEFAULT" {
			tz = v
		}
	}
	if tz == "" && e.n8nOpts.Timezone != "" {
		tz = e.n8nOpts.Timezone
	}
	if tz == "" {
		tz = time.Local.String()
	}
	for _, t := range rep.Triggers {
		if t.Kind != "schedule" && t.Kind != "poll" {
			continue
		}
		input := fmt.Sprintf(`{%q: %q}`, N8NTriggerKey, t.Node)
		for _, c := range t.Cron {
			id, err := e.CronManager.AddScheduleWithOpts(ctx, namespace, wf.ID, c, input, ScheduleOpts{Name: wf.Name + " / " + t.Node, Timezone: tz})
			if err != nil {
				return res, fmt.Errorf("schedule %q: %w", t.Node, err)
			}
			res.ScheduleIDs = append(res.ScheduleIDs, id)
		}
	}
	return res, nil
}

// N8NHandler serves the entry points of imported workflows in a namespace:
// /webhook/{path} (Webhook, Telegram/WhatsApp triggers), /chat/{workflowID}
// and /form/{workflowID}. Mount it under a prefix, e.g.
//
//	http.Handle("/n8n/", http.StripPrefix("/n8n", engine.N8NHandler(flow.N8NHandlerOptions{Namespace: "default"})))
func (e *Engine) N8NHandler(opts N8NHandlerOptions) http.Handler {
	return n8n.NewHandler(e.Engine, e.n8nOpts, opts)
}

// N8NOutputItems decodes the items of an n8n step or execution output.
func N8NOutputItems(output string) []N8NItem { return n8n.OutputItems(output) }

// N8NNodeSupported reports whether an n8n node type is implemented.
func N8NNodeSupported(nodeType string) bool { return n8n.Supported(nodeType) }

// ErrN8NWebhookConflict is returned when another workflow already serves a
// webhook path the imported workflow wants.
var ErrN8NWebhookConflict = errors.New("webhook path is already used by another workflow")

// DeactivateN8NWorkflow stops an installed workflow: N8NHandler stops
// serving its webhook, chat, form and MCP routes (immediately, on every
// replica), its schedules are removed and the webhooks registered in
// external services are deleted. Running executions finish. The definition
// stays in storage as INACTIVE, so its executions remain readable and its
// webhook paths are free for other workflows. Call it when a user stops or
// deletes an automation; InstallN8NWorkflow activates it again.
func (e *Engine) DeactivateN8NWorkflow(ctx context.Context, namespace, workflowID string) error {
	if e.n8nOpts == nil {
		return errors.New("n8n support is not enabled: call EnableN8N first")
	}
	wf, err := e.WorkflowDefinition(ctx, namespace, workflowID)
	if err != nil {
		return err
	}
	// Close the routes first: nothing new starts while the rest is cleaned up.
	if err := e.SetWorkflowStatus(ctx, namespace, workflowID, WorkflowInactive); err != nil {
		return err
	}
	var errs []error
	if err := e.removeN8NSchedules(ctx, namespace, workflowID); err != nil {
		errs = append(errs, err)
	}
	if err := n8n.UnregisterWebhooks(ctx, e.n8nOpts, namespace, wf); err != nil {
		errs = append(errs, fmt.Errorf("unregister service webhooks: %w", err))
	}
	return errors.Join(errs...)
}

func (e *Engine) removeN8NSchedules(ctx context.Context, namespace, workflowID string) error {
	if e.CronManager == nil {
		return nil
	}
	list, err := e.CronManager.ListSchedules(ctx, namespace)
	if err != nil {
		return fmt.Errorf("list schedules: %w", err)
	}
	for _, sc := range list {
		if sc.WorkflowID != workflowID {
			continue
		}
		if err := e.CronManager.RemoveSchedule(ctx, namespace, sc.ID); err != nil {
			return fmt.Errorf("remove schedule %s: %w", sc.ID, err)
		}
	}
	return nil
}

// webhookRoutesInUse maps the webhook routes of a namespace's active
// workflows, other than skipID, to the workflow using them.
func (e *Engine) webhookRoutesInUse(ctx context.Context, namespace, skipID string) (map[string]string, error) {
	existing, err := e.WorkflowDefinitions(ctx, namespace)
	if err != nil {
		return nil, err
	}
	used := map[string]string{}
	for _, other := range existing {
		if other.ID == skipID || other.Status == WorkflowInactive {
			continue
		}
		for _, k := range n8n.WebhookKeys(other) {
			used[k] = other.ID
		}
	}
	return used, nil
}

func (e *Engine) checkWebhookConflicts(ctx context.Context, namespace string, wf *Workflow) error {
	mine := n8n.WebhookKeys(wf)
	if len(mine) == 0 {
		return nil
	}
	used, err := e.webhookRoutesInUse(ctx, namespace, wf.ID)
	if err != nil {
		return err
	}
	for _, k := range mine {
		if owner, ok := used[k]; ok {
			return fmt.Errorf("%w: %s (workflow %s)", ErrN8NWebhookConflict, k, owner)
		}
	}
	return nil
}
