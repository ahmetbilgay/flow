package trigger

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dgmos/flow/internal/auth"
	"github.com/dgmos/flow/internal/engine"
	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/dgmos/flow/internal/telemetry"
	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

const (
	defaultCatchUpMax      = 10
	defaultCatchUpLookback = 24 * time.Hour
	defaultTimezone        = "UTC"
)

var cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type entryRef struct {
	tzKey     string
	entryID   cron.EntryID
	namespace string
}

// CronManager manages durable, dynamic, multi-replica-safe cron triggers.
type CronManager struct {
	crons        map[string]*cron.Cron
	engine       StartableEngine
	storage      storage.Storage
	logger       *slog.Logger
	authProvider auth.AuthProvider
	eventBus     *engine.EventBus
	hostHooks    engine.HostHooks
	entries      map[string]entryRef
	mu           sync.Mutex
	started      bool
	instanceID   string
}

// StartableEngine is the minimal engine surface needed to fire schedules.
type StartableEngine interface {
	StartWorkflow(ctx context.Context, namespace, workflowID string, input string) (string, error)
}

// ScheduleOpts configures create behavior.
type ScheduleOpts struct {
	Name              string
	CatchUp           bool
	CatchUpMax        int
	CatchUpLookback   time.Duration
	Timezone          string
	StartsAt          *time.Time
	EndsAt            *time.Time
	SkipWorkflowCheck bool
}

// ScheduleUpdateOpts is a partial update; nil pointer fields keep existing values.
type ScheduleUpdateOpts struct {
	CronExpression     *string
	Input              *string
	Name               *string
	Timezone           *string
	CatchUp            *bool
	CatchUpMax         *int
	CatchUpLookbackSec *int
	StartsAt           *time.Time
	EndsAt             *time.Time
	ClearStartsAt      bool
	ClearEndsAt        bool
	SkipWorkflowCheck  bool
}

// ScheduleListFilter filters ListSchedules results.
type ScheduleListFilter struct {
	ActiveOnly bool
	Limit      int
	Offset     int
}

// NewCronManager creates a persistent scheduler bound to storage.
func NewCronManager(eng StartableEngine, store storage.Storage, logger *slog.Logger) *CronManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &CronManager{
		crons:      make(map[string]*cron.Cron),
		engine:     eng,
		storage:    store,
		logger:     logger,
		entries:    make(map[string]entryRef),
		instanceID: uuid.New().String(),
	}
}

func (c *CronManager) SetAuthProvider(provider auth.AuthProvider) { c.authProvider = provider }
func (c *CronManager) SetEventBus(bus *engine.EventBus)           { c.eventBus = bus }
func (c *CronManager) SetHostHooks(h engine.HostHooks)            { c.hostHooks = h }
func (c *CronManager) SetLogger(l *slog.Logger) {
	if l != nil {
		c.logger = l
	}
}

func (c *CronManager) checkAuth(ctx context.Context, namespace, id, action string) error {
	if c.authProvider == nil {
		return nil
	}
	if err := c.authProvider.CheckAccess(ctx, fmt.Sprintf("schedule:%s:%s", namespace, id), action); err != nil {
		return fmt.Errorf("access denied: %w", err)
	}
	return nil
}

func (c *CronManager) emit(ctx context.Context, typ engine.EventType, namespace string, meta map[string]interface{}) {
	ev := engine.Event{Type: typ, Namespace: namespace, Metadata: meta}
	if c.eventBus != nil {
		_ = c.eventBus.EmitSync(ctx, ev)
	}
	if c.hostHooks != nil {
		_ = c.hostHooks.HandleEvent(ctx, ev)
	}
}

func (c *CronManager) gaugeInc(ns string) {
	telemetry.SchedulesActive.WithLabelValues(ns).Inc()
}
func (c *CronManager) gaugeDec(ns string) {
	telemetry.SchedulesActive.WithLabelValues(ns).Dec()
}

func normalizeTZ(tz string) (string, *time.Location, error) {
	if tz == "" {
		tz = defaultTimezone
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return "", nil, fmt.Errorf("invalid timezone %q: %w", tz, err)
	}
	return loc.String(), loc, nil
}

func applyCreateDefaults(opts *ScheduleOpts) {
	if opts.CatchUpMax <= 0 {
		opts.CatchUpMax = defaultCatchUpMax
	}
	if opts.CatchUpLookback <= 0 {
		opts.CatchUpLookback = defaultCatchUpLookback
	}
	if opts.Timezone == "" {
		opts.Timezone = defaultTimezone
	}
}

func nextRunInTZ(cronExpr, tz string, from time.Time) (*time.Time, error) {
	_, loc, err := normalizeTZ(tz)
	if err != nil {
		return nil, err
	}
	schedObj, err := cronParser.Parse(cronExpr)
	if err != nil {
		return nil, err
	}
	next := schedObj.Next(from.In(loc))
	return &next, nil
}

// Start loads active schedules and starts cron runners (idempotent, concurrency-safe).
func (c *CronManager) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	// Hold lock only for started flag flip after load registration — load storage outside? 
	// Keep lock through registration to prevent concurrent Start.
	c.logger.Info("CronManager: loading active schedules from storage", "instanceId", c.instanceID)
	c.mu.Unlock()

	schedules, err := c.storage.GetActiveSchedules(ctx)
	if err != nil {
		return fmt.Errorf("cron_manager: failed to load active schedules: %w", err)
	}

	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	loaded := 0
	for _, s := range schedules {
		if _, exists := c.entries[s.ID]; exists {
			continue // already registered via Add before Start
		}
		if err := c.scheduleInMemory(s); err != nil {
			c.logger.Error("CronManager: failed to register loaded schedule",
				"scheduleId", s.ID, "workflowId", s.WorkflowID, "error", err)
			continue
		}
		c.gaugeInc(s.Namespace)
		loaded++
	}
	c.started = true
	for _, runner := range c.crons {
		runner.Start()
	}
	c.mu.Unlock()

	for _, s := range schedules {
		if s.CatchUp {
			c.runCatchUp(ctx, s)
		}
	}

	c.logger.Info("CronManager: started", "loaded", loaded, "totalActive", len(schedules), "instanceId", c.instanceID)
	return nil
}

func (c *CronManager) AddSchedule(ctx context.Context, namespace, workflowID, cronExpr, input string) (string, error) {
	return c.AddScheduleWithOpts(ctx, namespace, workflowID, cronExpr, input, ScheduleOpts{})
}

func (c *CronManager) AddScheduleWithOpts(ctx context.Context, namespace, workflowID, cronExpr, input string, opts ScheduleOpts) (string, error) {
	if err := c.checkAuth(ctx, namespace, "*", "create"); err != nil {
		return "", err
	}
	applyCreateDefaults(&opts)
	if _, err := cronParser.Parse(cronExpr); err != nil {
		return "", fmt.Errorf("cron_manager: invalid cron expression %q: %w", cronExpr, err)
	}
	tzName, _, err := normalizeTZ(opts.Timezone)
	if err != nil {
		return "", err
	}
	if opts.StartsAt != nil && opts.EndsAt != nil && !opts.EndsAt.After(*opts.StartsAt) {
		return "", fmt.Errorf("cron_manager: ends_at must be after starts_at")
	}
	if !opts.SkipWorkflowCheck {
		if _, err := c.storage.GetWorkflow(ctx, namespace, workflowID); err != nil {
			return "", fmt.Errorf("cron_manager: workflow %q not found in namespace %q: %w", workflowID, namespace, err)
		}
	}

	scheduleID := uuid.New().String()
	now := time.Now().UTC()
	next, err := nextRunInTZ(cronExpr, tzName, now)
	if err != nil {
		return "", err
	}
	sched := &models.Schedule{
		ID: scheduleID, Namespace: namespace, WorkflowID: workflowID, Name: opts.Name,
		CronExpression: cronExpr, Input: input, Timezone: tzName, IsActive: true,
		CatchUp: opts.CatchUp, CatchUpMax: opts.CatchUpMax,
		CatchUpLookbackSec: int(opts.CatchUpLookback.Seconds()),
		StartsAt: opts.StartsAt, EndsAt: opts.EndsAt, NextRunAt: next,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := c.storage.SaveSchedule(ctx, sched); err != nil {
		return "", fmt.Errorf("cron_manager: database save failed: %w", err)
	}

	c.mu.Lock()
	err = c.scheduleInMemory(sched)
	shouldInc := err == nil
	c.mu.Unlock()
	if err != nil {
		_ = c.storage.DeleteSchedule(ctx, namespace, scheduleID)
		return "", fmt.Errorf("cron_manager: memory registration failed: %w", err)
	}
	if shouldInc {
		c.gaugeInc(namespace)
	}

	c.emit(ctx, engine.EventTypeScheduleCreated, namespace, map[string]interface{}{
		"scheduleId": scheduleID, "workflowId": workflowID, "cronExpr": cronExpr, "timezone": tzName,
	})
	c.logger.Info("CronManager: schedule added", "scheduleId", scheduleID, "namespace", namespace, "workflowId", workflowID)
	return scheduleID, nil
}

// UpdateSchedule applies a partial patch (nil fields keep current values).
func (c *CronManager) UpdateSchedule(ctx context.Context, namespace, scheduleID string, patch ScheduleUpdateOpts) error {
	if err := c.checkAuth(ctx, namespace, scheduleID, "update"); err != nil {
		return err
	}
	existing, err := c.storage.GetSchedule(ctx, namespace, scheduleID)
	if err != nil {
		return err
	}
	if !patch.SkipWorkflowCheck {
		if _, err := c.storage.GetWorkflow(ctx, namespace, existing.WorkflowID); err != nil {
			return fmt.Errorf("cron_manager: workflow %q not found: %w", existing.WorkflowID, err)
		}
	}

	if patch.CronExpression != nil {
		if _, err := cronParser.Parse(*patch.CronExpression); err != nil {
			return fmt.Errorf("cron_manager: invalid cron expression %q: %w", *patch.CronExpression, err)
		}
		existing.CronExpression = *patch.CronExpression
	}
	if patch.Input != nil {
		existing.Input = *patch.Input
	}
	if patch.Name != nil {
		existing.Name = *patch.Name
	}
	if patch.Timezone != nil {
		tzName, _, err := normalizeTZ(*patch.Timezone)
		if err != nil {
			return err
		}
		existing.Timezone = tzName
	}
	if patch.CatchUp != nil {
		existing.CatchUp = *patch.CatchUp
	}
	if patch.CatchUpMax != nil {
		existing.CatchUpMax = *patch.CatchUpMax
	}
	if patch.CatchUpLookbackSec != nil {
		existing.CatchUpLookbackSec = *patch.CatchUpLookbackSec
	}
	if patch.ClearStartsAt {
		existing.StartsAt = nil
	} else if patch.StartsAt != nil {
		existing.StartsAt = patch.StartsAt
	}
	if patch.ClearEndsAt {
		existing.EndsAt = nil
	} else if patch.EndsAt != nil {
		existing.EndsAt = patch.EndsAt
	}
	if existing.StartsAt != nil && existing.EndsAt != nil && !existing.EndsAt.After(*existing.StartsAt) {
		return fmt.Errorf("cron_manager: ends_at must be after starts_at")
	}

	next, err := nextRunInTZ(existing.CronExpression, existing.Timezone, time.Now().UTC())
	if err != nil {
		return err
	}
	existing.NextRunAt = next
	existing.UpdatedAt = time.Now().UTC()

	if err := c.storage.SaveSchedule(ctx, existing); err != nil {
		return fmt.Errorf("cron_manager: database update failed: %w", err)
	}

	c.mu.Lock()
	c.unscheduleInMemory(scheduleID)
	var regErr error
	if existing.IsActive {
		regErr = c.scheduleInMemory(existing)
	}
	c.mu.Unlock()
	if regErr != nil {
		return fmt.Errorf("cron_manager: memory re-registration failed: %w", regErr)
	}

	c.emit(ctx, engine.EventTypeScheduleUpdated, namespace, map[string]interface{}{"scheduleId": scheduleID})
	c.logger.Info("CronManager: schedule updated", "scheduleId", scheduleID, "namespace", namespace)
	return nil
}

// PauseSchedule soft-disables a schedule (auth required).
func (c *CronManager) PauseSchedule(ctx context.Context, namespace, scheduleID string) error {
	if err := c.checkAuth(ctx, namespace, scheduleID, "pause"); err != nil {
		return err
	}
	return c.deactivate(ctx, namespace, scheduleID, engine.EventTypeSchedulePaused, "paused")
}

// expireSchedule deactivates without auth (used when EndsAt elapses).
func (c *CronManager) expireSchedule(ctx context.Context, namespace, scheduleID string) {
	if err := c.deactivate(ctx, namespace, scheduleID, engine.EventTypeSchedulePaused, "expired"); err != nil {
		c.logger.Error("CronManager: expire failed", "scheduleId", scheduleID, "error", err)
	}
}

func (c *CronManager) deactivate(ctx context.Context, namespace, scheduleID string, ev engine.EventType, reason string) error {
	existing, err := c.storage.GetSchedule(ctx, namespace, scheduleID)
	if err != nil {
		return err
	}
	if !existing.IsActive {
		return nil
	}
	existing.IsActive = false
	existing.UpdatedAt = time.Now().UTC()
	if err := c.storage.SaveSchedule(ctx, existing); err != nil {
		return fmt.Errorf("cron_manager: deactivate failed: %w", err)
	}

	c.mu.Lock()
	_, wasLoaded := c.entries[scheduleID]
	c.unscheduleInMemory(scheduleID)
	c.mu.Unlock()
	if wasLoaded {
		c.gaugeDec(namespace)
	}

	c.emit(ctx, ev, namespace, map[string]interface{}{"scheduleId": scheduleID, "reason": reason})
	c.logger.Info("CronManager: schedule deactivated", "scheduleId", scheduleID, "reason", reason)
	return nil
}

func (c *CronManager) ResumeSchedule(ctx context.Context, namespace, scheduleID string) error {
	if err := c.checkAuth(ctx, namespace, scheduleID, "resume"); err != nil {
		return err
	}
	existing, err := c.storage.GetSchedule(ctx, namespace, scheduleID)
	if err != nil {
		return err
	}
	if existing.IsActive {
		return nil
	}
	if existing.EndsAt != nil && !existing.EndsAt.After(time.Now().UTC()) {
		return fmt.Errorf("cron_manager: cannot resume expired schedule (ends_at in the past)")
	}
	next, err := nextRunInTZ(existing.CronExpression, existing.Timezone, time.Now().UTC())
	if err != nil {
		return err
	}
	existing.IsActive = true
	existing.NextRunAt = next
	existing.UpdatedAt = time.Now().UTC()
	if err := c.storage.SaveSchedule(ctx, existing); err != nil {
		return fmt.Errorf("cron_manager: resume failed: %w", err)
	}

	c.mu.Lock()
	err = c.scheduleInMemory(existing)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	c.gaugeInc(namespace)

	c.emit(ctx, engine.EventTypeScheduleResumed, namespace, map[string]interface{}{"scheduleId": scheduleID})
	c.logger.Info("CronManager: schedule resumed", "scheduleId", scheduleID)
	return nil
}

func (c *CronManager) RemoveSchedule(ctx context.Context, namespace, scheduleID string) error {
	if err := c.checkAuth(ctx, namespace, scheduleID, "delete"); err != nil {
		return err
	}
	if err := c.storage.DeleteSchedule(ctx, namespace, scheduleID); err != nil {
		return fmt.Errorf("cron_manager: database delete failed: %w", err)
	}

	c.mu.Lock()
	_, wasLoaded := c.entries[scheduleID]
	c.unscheduleInMemory(scheduleID)
	c.mu.Unlock()
	if wasLoaded {
		c.gaugeDec(namespace)
	}

	c.emit(ctx, engine.EventTypeScheduleDeleted, namespace, map[string]interface{}{"scheduleId": scheduleID})
	c.logger.Info("CronManager: schedule removed", "scheduleId", scheduleID)
	return nil
}

func (c *CronManager) GetSchedule(ctx context.Context, namespace, scheduleID string) (*models.Schedule, error) {
	if err := c.checkAuth(ctx, namespace, scheduleID, "view"); err != nil {
		return nil, err
	}
	return c.storage.GetSchedule(ctx, namespace, scheduleID)
}

func (c *CronManager) ListSchedules(ctx context.Context, namespace string) ([]*models.Schedule, error) {
	return c.ListSchedulesFiltered(ctx, namespace, ScheduleListFilter{})
}

func (c *CronManager) ListSchedulesFiltered(ctx context.Context, namespace string, filter ScheduleListFilter) ([]*models.Schedule, error) {
	if err := c.checkAuth(ctx, namespace, "*", "view"); err != nil {
		return nil, err
	}
	return c.storage.ListSchedules(ctx, namespace, storage.ScheduleFilter{
		ActiveOnly: filter.ActiveOnly,
		Limit:      filter.Limit,
		Offset:     filter.Offset,
	})
}

func (c *CronManager) PreviewNextRuns(cronExpr, timezone string, n int) ([]time.Time, error) {
	if n <= 0 {
		n = 5
	}
	if n > 50 {
		n = 50
	}
	_, loc, err := normalizeTZ(timezone)
	if err != nil {
		return nil, err
	}
	schedObj, err := cronParser.Parse(cronExpr)
	if err != nil {
		return nil, fmt.Errorf("cron_manager: invalid cron expression %q: %w", cronExpr, err)
	}
	t := time.Now().In(loc)
	out := make([]time.Time, 0, n)
	for i := 0; i < n; i++ {
		t = schedObj.Next(t)
		out = append(out, t)
	}
	return out, nil
}

// Stop stops runners, clears in-memory state, and resets gauges.
func (c *CronManager) Stop() {
	c.mu.Lock()
	wasStarted := c.started
	c.started = false
	for _, ref := range c.entries {
		if r, ok := c.crons[ref.tzKey]; ok {
			r.Remove(ref.entryID)
		}
		c.gaugeDec(ref.namespace)
	}
	runners := make([]*cron.Cron, 0, len(c.crons))
	for _, r := range c.crons {
		runners = append(runners, r)
	}
	c.entries = make(map[string]entryRef)
	c.crons = make(map[string]*cron.Cron)
	c.mu.Unlock()

	if !wasStarted {
		return
	}
	for _, r := range runners {
		ctx := r.Stop()
		<-ctx.Done()
	}
	c.logger.Info("CronManager: scheduler stopped", "instanceId", c.instanceID)
}

func (c *CronManager) runnerFor(tz string) (*cron.Cron, string, error) {
	tzName, loc, err := normalizeTZ(tz)
	if err != nil {
		return nil, "", err
	}
	if r, ok := c.crons[tzName]; ok {
		return r, tzName, nil
	}
	r := cron.New(cron.WithSeconds(), cron.WithLocation(loc))
	c.crons[tzName] = r
	if c.started {
		r.Start()
	}
	return r, tzName, nil
}

func (c *CronManager) unscheduleInMemory(scheduleID string) {
	ref, exists := c.entries[scheduleID]
	if !exists {
		return
	}
	if r, ok := c.crons[ref.tzKey]; ok {
		r.Remove(ref.entryID)
	}
	delete(c.entries, scheduleID)
}

func (c *CronManager) scheduleInMemory(sched *models.Schedule) error {
	c.unscheduleInMemory(sched.ID)
	runner, tzKey, err := c.runnerFor(sched.Timezone)
	if err != nil {
		return err
	}
	namespace, workflowID, scheduleID := sched.Namespace, sched.WorkflowID, sched.ID
	cronExpr, input := sched.CronExpression, sched.Input
	var startsAt, endsAt *time.Time
	if sched.StartsAt != nil {
		t := sched.StartsAt.UTC()
		startsAt = &t
	}
	if sched.EndsAt != nil {
		t := sched.EndsAt.UTC()
		endsAt = &t
	}
	entryID, err := runner.AddFunc(cronExpr, func() {
		c.fire(context.Background(), namespace, workflowID, scheduleID, cronExpr, input, time.Now().UTC(), startsAt, endsAt)
	})
	if err != nil {
		return err
	}
	c.entries[scheduleID] = entryRef{tzKey: tzKey, entryID: entryID, namespace: namespace}
	return nil
}

func (c *CronManager) runCatchUp(ctx context.Context, s *models.Schedule) {
	_, loc, err := normalizeTZ(s.Timezone)
	if err != nil {
		return
	}
	schedObj, err := cronParser.Parse(s.CronExpression)
	if err != nil {
		return
	}
	max := s.CatchUpMax
	if max <= 0 {
		max = defaultCatchUpMax
	}
	lookback := time.Duration(s.CatchUpLookbackSec) * time.Second
	if lookback <= 0 {
		lookback = defaultCatchUpLookback
	}
	from := time.Now().In(loc).Add(-lookback)
	if s.LastRunAt != nil && s.LastRunAt.In(loc).After(from) {
		from = s.LastRunAt.In(loc)
	}
	if s.StartsAt != nil && s.StartsAt.In(loc).After(from) {
		from = s.StartsAt.In(loc)
	}
	t := schedObj.Next(from)
	now := time.Now().In(loc)
	for n := 0; n < max && t.Before(now); n++ {
		if s.EndsAt != nil && t.After(*s.EndsAt) {
			break
		}
		c.fire(ctx, s.Namespace, s.WorkflowID, s.ID, s.CronExpression, s.Input, t.UTC(), s.StartsAt, s.EndsAt)
		t = schedObj.Next(t)
	}
}

func (c *CronManager) fire(ctx context.Context, namespace, workflowID, scheduleID, cronExpr, input string, fireAt time.Time, startsAt, endsAt *time.Time) {
	fireAt = fireAt.UTC().Truncate(time.Second)
	if startsAt != nil && fireAt.Before(startsAt.UTC()) {
		telemetry.ScheduleFires.WithLabelValues(namespace, workflowID, "skipped").Inc()
		return
	}
	if endsAt != nil && !fireAt.Before(endsAt.UTC()) {
		telemetry.ScheduleFires.WithLabelValues(namespace, workflowID, "skipped").Inc()
		c.expireSchedule(ctx, namespace, scheduleID)
		return
	}

	c.logger.Info("CronManager: trigger fired", "namespace", namespace, "workflowId", workflowID, "scheduleId", scheduleID, "fireAt", fireAt)

	claimed, err := c.storage.TryClaimScheduleRun(ctx, namespace, scheduleID, fireAt)
	if err != nil {
		c.logger.Error("CronManager: claim failed", "scheduleId", scheduleID, "error", err)
		telemetry.ScheduleFires.WithLabelValues(namespace, workflowID, "failed").Inc()
		return
	}
	if !claimed {
		telemetry.ScheduleFires.WithLabelValues(namespace, workflowID, "claimed_lost").Inc()
		return
	}

	schedMeta, _ := c.storage.GetSchedule(ctx, namespace, scheduleID)
	tz := defaultTimezone
	if schedMeta != nil && schedMeta.Timezone != "" {
		tz = schedMeta.Timezone
	}
	nextPtr, _ := nextRunInTZ(cronExpr, tz, fireAt)

	execID, err := c.engine.StartWorkflow(ctx, namespace, workflowID, input)
	if err != nil {
		_ = c.storage.RecordScheduleRun(ctx, namespace, scheduleID, fireAt, "", err.Error(), nextPtr)
		telemetry.ScheduleFires.WithLabelValues(namespace, workflowID, "failed").Inc()
		c.emit(ctx, engine.EventTypeScheduleFireFailed, namespace, map[string]interface{}{
			"scheduleId": scheduleID, "workflowId": workflowID, "error": err.Error(),
		})
		return
	}
	_ = c.storage.RecordScheduleRun(ctx, namespace, scheduleID, fireAt, execID, "", nextPtr)
	telemetry.ScheduleFires.WithLabelValues(namespace, workflowID, "started").Inc()
	c.emit(ctx, engine.EventTypeScheduleFired, namespace, map[string]interface{}{
		"scheduleId": scheduleID, "workflowId": workflowID, "executionId": execID, "fireAt": fireAt,
	})
}
