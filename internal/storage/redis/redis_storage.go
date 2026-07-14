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
