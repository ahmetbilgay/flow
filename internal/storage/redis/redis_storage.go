package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/redis/go-redis/v9"
)

type RedisStorage struct {
	client *redis.Client
	crypto *storage.Crypto
}

func NewRedisStorage(addr string, password string, db int) *RedisStorage {
	return &RedisStorage{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (r *RedisStorage) SetEncryption(crypto *storage.Crypto) {
	r.crypto = crypto
}

// Helper keys
func wfKey(ns, id string) string { return fmt.Sprintf("wf:%s:%s", ns, id) }
func execKey(ns, id string) string { return fmt.Sprintf("ex:%s:%s", ns, id) }
func stepKey(ns, id string) string { return fmt.Sprintf("step:%s:%s", ns, id) }
func pendingSet(ns string) string { return fmt.Sprintf("pending:%s", ns) }
func wfListKey(ns string) string { return fmt.Sprintf("wfs:%s", ns) }
func execListKey(ns string) string { return fmt.Sprintf("execs:%s", ns) }
func execStepsKey(ns, execID string) string { return fmt.Sprintf("ex_steps:%s:%s", ns, execID) }

func (r *RedisStorage) SaveWorkflow(ctx context.Context, namespace string, wf *models.Workflow) error {
	data, _ := json.Marshal(wf)
	if err := r.client.Set(ctx, wfKey(namespace, wf.ID), data, 0).Err(); err != nil {
		return err
	}
	return r.client.SAdd(ctx, wfListKey(namespace), wf.ID).Err()
}

func (r *RedisStorage) GetWorkflow(ctx context.Context, namespace string, id string) (*models.Workflow, error) {
	data, err := r.client.Get(ctx, wfKey(namespace, id)).Bytes()
	if err != nil {
		return nil, err
	}
	var wf models.Workflow
	if err := json.Unmarshal(data, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (r *RedisStorage) ListWorkflows(ctx context.Context, namespace string) ([]*models.Workflow, error) {
	ids, err := r.client.SMembers(ctx, wfListKey(namespace)).Result()
	if err != nil {
		return nil, err
	}
	var wfs []*models.Workflow
	for _, id := range ids {
		wf, err := r.GetWorkflow(ctx, namespace, id)
		if err == nil {
			wfs = append(wfs, wf)
		}
	}
	return wfs, nil
}

func (r *RedisStorage) CreateExecution(ctx context.Context, namespace string, exec *models.Execution) error {
	data, _ := json.Marshal(exec)
	if err := r.client.Set(ctx, execKey(namespace, exec.ID), data, 0).Err(); err != nil {
		return err
	}
	return r.client.SAdd(ctx, execListKey(namespace), exec.ID).Err()
}

func (r *RedisStorage) GetExecution(ctx context.Context, namespace string, id string) (*models.Execution, error) {
	data, err := r.client.Get(ctx, execKey(namespace, id)).Bytes()
	if err != nil {
		return nil, err
	}
	var exec models.Execution
	if err := json.Unmarshal(data, &exec); err != nil {
		return nil, err
	}
	return &exec, nil
}

func (r *RedisStorage) UpdateExecution(ctx context.Context, namespace string, exec *models.Execution) error {
	return r.CreateExecution(ctx, namespace, exec)
}

func (r *RedisStorage) ListExecutions(ctx context.Context, namespace string) ([]*models.Execution, error) {
	ids, err := r.client.SMembers(ctx, execListKey(namespace)).Result()
	if err != nil {
		return nil, err
	}

	var execs []*models.Execution
	for _, id := range ids {
		exec, err := r.GetExecution(ctx, namespace, id)
		if err == nil {
			execs = append(execs, exec)
		}
	}
	return execs, nil
}

func (r *RedisStorage) CreateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	step.UpdatedAt = time.Now()
	data, _ := json.Marshal(step)
	err := r.client.Set(ctx, stepKey(namespace, step.ID), data, 0).Err()
	if err != nil {
		return err
	}

	if err := r.client.SAdd(ctx, execStepsKey(namespace, step.ExecutionID), step.ID).Err(); err != nil {
		return err
	}

	// Only PENDING steps are claimable by workers
	if step.Status != models.TaskPending {
		return nil
	}

	score := float64(time.Now().Unix())
	if step.ScheduledAt != nil {
		score = float64(step.ScheduledAt.Unix())
	}
	return r.client.ZAdd(ctx, pendingSet(namespace), redis.Z{
		Score:  score,
		Member: step.ID,
	}).Err()
}

func (r *RedisStorage) UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	step.UpdatedAt = time.Now()
	data, _ := json.Marshal(step)
	err := r.client.Set(ctx, stepKey(namespace, step.ID), data, 0).Err()
	if err != nil {
		return err
	}

	// If finished, remove from pending
	if step.Status != models.TaskPending {
		r.client.ZRem(ctx, pendingSet(namespace), step.ID)
	}
	return nil
}

func (r *RedisStorage) GetExecutionSteps(ctx context.Context, namespace string, executionID string) ([]*models.ExecutionStep, error) {
	ids, err := r.client.SMembers(ctx, execStepsKey(namespace, executionID)).Result()
	if err != nil {
		return nil, err
	}

	var steps []*models.ExecutionStep
	for _, id := range ids {
		step, err := r.GetExecutionStepByID(ctx, namespace, id)
		if err == nil {
			steps = append(steps, step)
		}
	}
	return steps, nil
}

func (r *RedisStorage) ClaimReadyStep(ctx context.Context, namespace string, workerID string) (*models.ExecutionStep, error) {
	// 10/10 Atomic Claim using LUA
	// We check for ready steps (score <= now)
	now := time.Now().Unix()
	pendingSetKey := pendingSet(namespace)

	// LUA Script: Try to fetch one eligible step, remove it from ZSET, and return its ID
	script := `
		local steps = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
		if #steps > 0 then
			redis.call('ZREM', KEYS[1], steps[1])
			return steps[1]
		end
		return nil
	`

	res, err := r.client.Eval(ctx, script, []string{pendingSetKey}, now).Result()
	if err == redis.Nil || err != nil {
		return nil, nil
	}

	stepID, ok := res.(string)
	if !ok {
		return nil, nil
	}

	step, err := r.GetExecutionStepByID(ctx, namespace, stepID)
	if err != nil {
		return nil, err
	}

	step.Status = models.TaskRunning
	step.WorkerID = workerID
	step.StartedAt = time.Now()
	// Update step data (Status=RUNNING)
	err = r.UpdateExecutionStep(ctx, namespace, step)
	if err != nil {
		return nil, err
	}

	return step, nil
}

func (r *RedisStorage) GetExecutionStepByID(ctx context.Context, namespace string, id string) (*models.ExecutionStep, error) {
	data, err := r.client.Get(ctx, stepKey(namespace, id)).Bytes()
	if err != nil {
		return nil, err
	}
	var step models.ExecutionStep
	if err := json.Unmarshal(data, &step); err != nil {
		return nil, err
	}
	return &step, nil
}

func (r *RedisStorage) ListWaitingSteps(ctx context.Context, namespace string, filter storage.TaskFilter) ([]*models.ExecutionStep, error) {
	execs, err := r.ListExecutions(ctx, namespace)
	if err != nil {
		return nil, err
	}
	var out []*models.ExecutionStep
	for _, exec := range execs {
		if filter.ExecutionID != "" && exec.ID != filter.ExecutionID {
			continue
		}
		steps, err := r.GetExecutionSteps(ctx, namespace, exec.ID)
		if err != nil {
			continue
		}
		for _, step := range steps {
			if step.Status != models.TaskWaiting {
				continue
			}
			if filter.NodeID != "" && step.NodeID != filter.NodeID {
				continue
			}
			if filter.Assignee != "" {
				found := false
				for _, a := range step.Assignees {
					if a == filter.Assignee {
						found = true
						break
					}
				}
				if !found && step.ClaimedBy != filter.Assignee {
					continue
				}
			}
			out = append(out, step)
		}
	}
	return out, nil
}

func (r *RedisStorage) GetExecutionStepByNode(ctx context.Context, namespace, executionID, nodeID string) (*models.ExecutionStep, error) {
	steps, err := r.GetExecutionSteps(ctx, namespace, executionID)
	if err != nil {
		return nil, err
	}
	var latest *models.ExecutionStep
	for _, step := range steps {
		if step.NodeID == nodeID {
			if latest == nil || step.StartedAt.After(latest.StartedAt) {
				latest = step
			}
		}
	}
	if latest == nil {
		return nil, fmt.Errorf("step not found for node %s", nodeID)
	}
	return latest, nil
}

func journalKey(ns, id string) string { return fmt.Sprintf("jrnl:%s:%s", ns, id) }
func journalListKey(ns, execID string) string {
	return fmt.Sprintf("jrnl_list:%s:%s", ns, execID)
}

func (r *RedisStorage) AppendCompensationRecord(ctx context.Context, namespace string, rec *models.CompensationRecord) error {
	rec.Namespace = namespace
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	data, _ := json.Marshal(rec)
	if err := r.client.Set(ctx, journalKey(namespace, rec.ID), data, 0).Err(); err != nil {
		return err
	}
	return r.client.RPush(ctx, journalListKey(namespace, rec.ExecutionID), rec.ID).Err()
}

func (r *RedisStorage) ListCompensationRecords(ctx context.Context, namespace, executionID string) ([]*models.CompensationRecord, error) {
	ids, err := r.client.LRange(ctx, journalListKey(namespace, executionID), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var out []*models.CompensationRecord
	for _, id := range ids {
		data, err := r.client.Get(ctx, journalKey(namespace, id)).Bytes()
		if err != nil {
			continue
		}
		var rec models.CompensationRecord
		if json.Unmarshal(data, &rec) == nil {
			out = append(out, &rec)
		}
	}
	return out, nil
}

func scheduleKey(ns, id string) string { return fmt.Sprintf("sched:%s:%s", ns, id) }
func scheduleActiveSet() string         { return "schedules:active" }
func scheduleNSSet(ns string) string    { return fmt.Sprintf("schedules:%s", ns) }

func (r *RedisStorage) SaveSchedule(ctx context.Context, sched *models.Schedule) error {
	now := time.Now().UTC()
	if sched.CreatedAt.IsZero() {
		sched.CreatedAt = now
	}
	sched.UpdatedAt = now
	data, err := json.Marshal(sched)
	if err != nil {
		return err
	}
	key := scheduleKey(sched.Namespace, sched.ID)
	if err := r.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("redis_storage: failed to save schedule: %w", err)
	}
	_ = r.client.SAdd(ctx, scheduleNSSet(sched.Namespace), sched.ID).Err()
	member := fmt.Sprintf("%s:%s", sched.Namespace, sched.ID)
	if sched.IsActive {
		if err := r.client.SAdd(ctx, scheduleActiveSet(), member).Err(); err != nil {
			return err
		}
	} else {
		_ = r.client.SRem(ctx, scheduleActiveSet(), member).Err()
	}
	return nil
}

func (r *RedisStorage) DeleteSchedule(ctx context.Context, namespace, id string) error {
	key := scheduleKey(namespace, id)
	n, err := r.client.Del(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("redis_storage: failed to delete schedule: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("redis_storage: schedule not found: namespace=%s, id=%s", namespace, id)
	}
	_ = r.client.SRem(ctx, scheduleNSSet(namespace), id).Err()
	_ = r.client.SRem(ctx, scheduleActiveSet(), fmt.Sprintf("%s:%s", namespace, id)).Err()
	_ = r.client.Del(ctx, scheduleClaimKey(namespace, id)).Err()
	return nil
}

func (r *RedisStorage) GetActiveSchedules(ctx context.Context) ([]*models.Schedule, error) {
	members, err := r.client.SMembers(ctx, scheduleActiveSet()).Result()
	if err != nil {
		return nil, fmt.Errorf("redis_storage: failed to list active schedules: %w", err)
	}
	now := time.Now().UTC()
	var out []*models.Schedule
	for _, member := range members {
		ns, id, ok := splitNSID(member)
		if !ok {
			continue
		}
		sched, err := r.GetSchedule(ctx, ns, id)
		if err != nil || sched == nil || !sched.IsActive {
			continue
		}
		if sched.EndsAt != nil && !sched.EndsAt.After(now) {
			continue
		}
		out = append(out, sched)
	}
	return out, nil
}

func (r *RedisStorage) GetSchedule(ctx context.Context, namespace, id string) (*models.Schedule, error) {
	data, err := r.client.Get(ctx, scheduleKey(namespace, id)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("redis_storage: schedule not found")
		}
		return nil, fmt.Errorf("redis_storage: failed to get schedule: %w", err)
	}
	var sched models.Schedule
	if err := json.Unmarshal(data, &sched); err != nil {
		return nil, err
	}
	return &sched, nil
}

func (r *RedisStorage) ListSchedules(ctx context.Context, namespace string, filter storage.ScheduleFilter) ([]*models.Schedule, error) {
	ids, err := r.client.SMembers(ctx, scheduleNSSet(namespace)).Result()
	if err != nil {
		return nil, err
	}
	var out []*models.Schedule
	for _, id := range ids {
		sched, err := r.GetSchedule(ctx, namespace, id)
		if err != nil {
			continue
		}
		if filter.ActiveOnly && !sched.IsActive {
			continue
		}
		out = append(out, sched)
	}
	if filter.Offset > 0 {
		if filter.Offset >= len(out) {
			return []*models.Schedule{}, nil
		}
		out = out[filter.Offset:]
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func scheduleClaimKey(ns, id string) string {
	return fmt.Sprintf("sched:lastfire:%s:%s", ns, id)
}

func (r *RedisStorage) TryClaimScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time) (bool, error) {
	fireAt = fireAt.UTC().Truncate(time.Second)
	sched, err := r.GetSchedule(ctx, namespace, id)
	if err != nil || sched == nil || !sched.IsActive {
		return false, nil
	}
	claimKey := scheduleClaimKey(namespace, id)
	fireUnix := fireAt.Unix()
	// Atomic claim + persist last_run_at into schedule JSON in one Lua script.
	script := redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
local fire = tonumber(ARGV[1])
if cur ~= false and tonumber(cur) >= fire then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
local raw = redis.call('GET', KEYS[2])
if raw ~= false then
  local ok, obj = pcall(cjson.decode, raw)
  if ok then
    obj['last_run_at'] = ARGV[2]
    obj['updated_at'] = ARGV[2]
    redis.call('SET', KEYS[2], cjson.encode(obj))
  end
end
return 1
`)
	fireRFC := fireAt.Format(time.RFC3339Nano)
	res, err := script.Run(ctx, r.client, []string{claimKey, scheduleKey(namespace, id)}, fireUnix, fireRFC).Int()
	if err != nil {
		// Fallback without cjson mutation if Redis lacks cjson
		script2 := redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
local fire = tonumber(ARGV[1])
if cur == false or tonumber(cur) < fire then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
return 0
`)
		res, err = script2.Run(ctx, r.client, []string{claimKey}, fireUnix).Int()
		if err != nil {
			return false, fmt.Errorf("redis_storage: claim schedule run failed: %w", err)
		}
		if res == 1 {
			t := fireAt
			sched.LastRunAt = &t
			sched.UpdatedAt = time.Now().UTC()
			_ = r.SaveSchedule(ctx, sched)
		}
		return res == 1, nil
	}
	return res == 1, nil
}

func (r *RedisStorage) RecordScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time, execID, lastErr string, nextRun *time.Time) error {
	sched, err := r.GetSchedule(ctx, namespace, id)
	if err != nil {
		return err
	}
	t := fireAt.UTC().Truncate(time.Second)
	sched.LastRunAt = &t
	sched.LastExecID = execID
	sched.LastError = lastErr
	if nextRun != nil {
		n := nextRun.UTC()
		sched.NextRunAt = &n
	} else {
		sched.NextRunAt = nil
	}
	return r.SaveSchedule(ctx, sched)
}

func splitNSID(member string) (ns, id string, ok bool) {
	for i := 0; i < len(member); i++ {
		if member[i] == ':' {
			return member[:i], member[i+1:], true
		}
	}
	return "", "", false
}
