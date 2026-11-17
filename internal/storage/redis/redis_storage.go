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
func wfKey(ns, id string) string   { return fmt.Sprintf("wf:%s:%s", ns, id) }
func execKey(ns, id string) string { return fmt.Sprintf("ex:%s:%s", ns, id) }
func stepKey(ns, id string) string { return fmt.Sprintf("step:%s:%s", ns, id) }
func pendingSet(ns string) string  { return fmt.Sprintf("pending:%s", ns) }

// runningSet holds claimed step IDs scored by their last heartbeat (unix
// seconds). Entries older than storage.StaleStepThreshold are reclaimable.
func runningSet(ns string) string           { return fmt.Sprintf("running:%s", ns) }
func wfListKey(ns string) string            { return fmt.Sprintf("wfs:%s", ns) }
func execListKey(ns string) string          { return fmt.Sprintf("execs:%s", ns) }
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
	data, err := json.Marshal(exec)
	if err != nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.Set(ctx, execKey(namespace, exec.ID), data, 0)
	pipe.SAdd(ctx, execListKey(namespace), exec.ID)
	_, err = pipe.Exec(ctx)
	return err
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
	data, err := json.Marshal(exec)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, execKey(namespace, exec.ID), data, 0).Err()
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

// createStepScript atomically creates a step, indexes it under its
// execution and queues it when PENDING.
// KEYS: step, execSteps, pending. ARGV: data, id, queue(0/1), score.
var createStepScript = redis.NewScript(`
	if not redis.call('SET', KEYS[1], ARGV[1], 'NX') then return 0 end
	redis.call('SADD', KEYS[2], ARGV[2])
	if ARGV[3] == '1' then redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2]) end
	return 1
`)

// updateStepScript atomically writes a step and maintains the queues,
// optionally only when the stored status/worker match (compare-and-set).
// KEYS: step, pending, running. ARGV: data, id, newStatus, score, fromStatus, worker.
var updateStepScript = redis.NewScript(`
	if ARGV[5] ~= '' or ARGV[6] ~= '' then
		local cur = redis.call('GET', KEYS[1])
		if not cur then return 0 end
		local s = cjson.decode(cur)
		if ARGV[5] ~= '' and s['status'] ~= ARGV[5] then return 0 end
		if ARGV[6] ~= '' and (s['workerId'] or '') ~= ARGV[6] then return 0 end
	end
	redis.call('SET', KEYS[1], ARGV[1])
	if ARGV[3] == 'PENDING' then
		redis.call('ZREM', KEYS[3], ARGV[2])
		redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
	elseif ARGV[3] ~= 'RUNNING' then
		redis.call('ZREM', KEYS[2], ARGV[2])
		redis.call('ZREM', KEYS[3], ARGV[2])
	end
	return 1
`)

func stepScore(step *models.ExecutionStep) float64 {
	if step.ScheduledAt != nil {
		return float64(step.ScheduledAt.Unix())
	}
	return float64(time.Now().Unix())
}

func (r *RedisStorage) CreateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	step.Namespace = namespace
	step.UpdatedAt = time.Now()
	data, err := json.Marshal(step)
	if err != nil {
		return err
	}
	queue := "0"
	if step.Status == models.TaskPending {
		queue = "1"
	}
	n, err := createStepScript.Run(ctx, r.client, []string{stepKey(namespace, step.ID), execStepsKey(namespace, step.ExecutionID), pendingSet(namespace)},
		string(data), step.ID, queue, stepScore(step)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return storage.ErrStepExists
	}
	return nil
}

func (r *RedisStorage) UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	_, err := r.updateStep(ctx, namespace, step, "", "")
	return err
}

// UpdateExecutionStepIf implements storage.ConditionalUpdater.
func (r *RedisStorage) UpdateExecutionStepIf(ctx context.Context, namespace string, step *models.ExecutionStep, from models.TaskStatus, worker string) (bool, error) {
	return r.updateStep(ctx, namespace, step, string(from), worker)
}

func (r *RedisStorage) updateStep(ctx context.Context, namespace string, step *models.ExecutionStep, from, worker string) (bool, error) {
	step.UpdatedAt = time.Now()
	data, err := json.Marshal(step)
	if err != nil {
		return false, err
	}
	n, err := updateStepScript.Run(ctx, r.client, []string{stepKey(namespace, step.ID), pendingSet(namespace), runningSet(namespace)},
		string(data), step.ID, string(step.Status), stepScore(step), from, worker).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// updateExecScript writes an execution only if its stored status matches.
// KEYS: exec. ARGV: data, fromStatus.
var updateExecScript = redis.NewScript(`
	local cur = redis.call('GET', KEYS[1])
	if not cur then return 0 end
	if cjson.decode(cur)['status'] ~= ARGV[2] then return 0 end
	redis.call('SET', KEYS[1], ARGV[1])
	return 1
`)

// UpdateExecutionIf implements storage.ConditionalUpdater.
func (r *RedisStorage) UpdateExecutionIf(ctx context.Context, namespace string, exec *models.Execution, from models.TaskStatus) (bool, error) {
	data, err := json.Marshal(exec)
	if err != nil {
		return false, err
	}
	n, err := updateExecScript.Run(ctx, r.client, []string{execKey(namespace, exec.ID)}, string(data), string(from)).Int()
	return n == 1, err
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

// claimScript atomically picks a step and records it in the running set.
// Stale running entries (crashed workers) take priority over new work so
// they are not starved. KEYS: pending, running. ARGV: now, staleBefore.
// claimScript atomically picks a claimable step and records it in the
// running set. Stale running entries (crashed workers) take priority; queue
// entries whose step is no longer PENDING/RUNNING (finished, waiting, or
// deleted) are dropped instead of being revived.
// KEYS: pending, running. ARGV: now, staleBefore, stepKeyPrefix.
var claimScript = redis.NewScript(`
	for _ = 1, 20 do
		local stale = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2], 'LIMIT', 0, 1)
		if #stale == 0 then break end
		local cur = redis.call('GET', ARGV[3] .. stale[1])
		if cur and cjson.decode(cur)['status'] == 'RUNNING' then
			redis.call('ZADD', KEYS[2], ARGV[1], stale[1])
			return stale[1]
		end
		redis.call('ZREM', KEYS[2], stale[1])
	end
	for _ = 1, 20 do
		local steps = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
		if #steps == 0 then break end
		redis.call('ZREM', KEYS[1], steps[1])
		local cur = redis.call('GET', ARGV[3] .. steps[1])
		if cur and cjson.decode(cur)['status'] == 'PENDING' then
			redis.call('ZADD', KEYS[2], ARGV[1], steps[1])
			return steps[1]
		end
	end
	return false
`)

func (r *RedisStorage) ClaimReadyStep(ctx context.Context, namespace string, workerID string) (*models.ExecutionStep, error) {
	now := time.Now()
	staleBefore := now.Add(-storage.StaleStepThreshold).Unix()

	res, err := claimScript.Run(ctx, r.client, []string{pendingSet(namespace), runningSet(namespace)}, now.Unix(), staleBefore, stepKey(namespace, "")).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	stepID, ok := res.(string)
	if !ok {
		return nil, nil
	}

	step, err := r.GetExecutionStepByID(ctx, namespace, stepID)
	if err == redis.Nil {
		// Dangling queue entry without step data; drop it.
		_ = r.client.ZRem(ctx, runningSet(namespace), stepID).Err()
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	from, prevWorker := step.Status, step.WorkerID
	step.Status = models.TaskRunning
	step.WorkerID = workerID
	step.StartedAt = now
	ok, err = r.updateStep(ctx, namespace, step, string(from), prevWorker)
	if err != nil {
		// Still listed in the running set, so it will be reclaimed later.
		return nil, err
	}
	if !ok {
		return nil, nil // changed concurrently (e.g. completed); not ours
	}
	return step, nil
}

// HeartbeatStep extends the lease of a RUNNING step owned by workerID.
func (r *RedisStorage) HeartbeatStep(ctx context.Context, namespace, stepID, workerID string) (bool, error) {
	step, err := r.GetExecutionStepByID(ctx, namespace, stepID)
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if step.Status != models.TaskRunning || step.WorkerID != workerID {
		return false, nil
	}
	// XX: only refresh an existing lease, never resurrect a finished step.
	n, err := r.client.ZAddXX(ctx, runningSet(namespace), redis.Z{Score: float64(time.Now().Unix()), Member: stepID}).Result()
	if err != nil {
		return false, err
	}
	_ = n // ZADD XX returns 0 for updates; ownership was verified above.
	return true, nil
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
func scheduleActiveSet() string        { return "schedules:active" }
func scheduleNSSet(ns string) string   { return fmt.Sprintf("schedules:%s", ns) }

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
