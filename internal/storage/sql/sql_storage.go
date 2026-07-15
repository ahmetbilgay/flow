package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
)

type Dialect interface {
	Placeholder(n int) string
	SkipLocked(query string) string
	Name() string
}

type MySQLDialect struct{}

func (d MySQLDialect) Placeholder(n int) string { return "?" }
func (d MySQLDialect) SkipLocked(query string) string {
	return query + " FOR UPDATE SKIP LOCKED"
}
func (d MySQLDialect) Name() string { return "mysql" }

type PostgresDialect struct{}

func (d PostgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }
func (d PostgresDialect) SkipLocked(query string) string {
	return query + " FOR UPDATE SKIP LOCKED"
}
func (d PostgresDialect) Name() string { return "postgres" }

// Options configures SQL storage table naming and migrations.
type Options struct {
	TablePrefix string // e.g. "flow_" so tables become flow_workflows
	AutoMigrate bool   // default true when using constructors below
	VarHistory  bool   // reserved; history table optional
}

func (o Options) withDefaults() Options {
	// Zero value AutoMigrate is false; constructors set true explicitly.
	return o
}

type SQLStorage struct {
	db      *sqlx.DB
	dialect Dialect
	crypto  *storage.Crypto
	opts    Options
}

func (s *SQLStorage) t(name string) string {
	return s.opts.TablePrefix + name
}

func (s *SQLStorage) ph(query string) string {
	if _, ok := s.dialect.(PostgresDialect); !ok {
		return query
	}
	// Convert ? to $n
	var b strings.Builder
	n := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(fmt.Sprintf("%d", n))
			n++
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

// NewSQLStorage creates storage with auto-migrate and empty prefix (back-compat).
func NewSQLStorage(db *sqlx.DB, dialectName string) (*SQLStorage, error) {
	return NewSQLStorageWithOptions(db, dialectName, Options{AutoMigrate: true})
}

// NewSQLStorageWithOptions creates storage with custom options.
func NewSQLStorageWithOptions(db *sqlx.DB, dialectName string, opts Options) (*SQLStorage, error) {
	var dialect Dialect
	switch dialectName {
	case "mysql":
		dialect = MySQLDialect{}
	case "postgres":
		dialect = PostgresDialect{}
	default:
		return nil, fmt.Errorf("unsupported dialect: %s", dialectName)
	}
	s := &SQLStorage{db: db, dialect: dialect, opts: opts.withDefaults()}
	if opts.AutoMigrate {
		if err := s.RunMigrations(); err != nil {
			log.Printf("⚠️  Migration warning (tables may already exist): %v", err)
		}
	}
	return s, nil
}

// NewSQLStorageFromDB wraps database/sql.DB via sqlx.
func NewSQLStorageFromDB(db *sql.DB, dialectName string, opts Options) (*SQLStorage, error) {
	driver := dialectName
	if dialectName == "postgres" {
		driver = "postgres"
	}
	return NewSQLStorageWithOptions(sqlx.NewDb(db, driver), dialectName, opts)
}

func (s *SQLStorage) SetEncryption(crypto *storage.Crypto) {
	s.crypto = crypto
}

func (s *SQLStorage) RunMigrations() error {
	schema := s.buildSchema()
	statements := strings.Split(schema, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := s.db.Exec(stmt); err != nil {
			if !strings.Contains(err.Error(), "already exists") &&
				!strings.Contains(err.Error(), "duplicate") {
				return fmt.Errorf("migration error: %w", err)
			}
		}
	}
	log.Println("✅ Database migrations completed successfully")
	return nil
}

func (s *SQLStorage) buildSchema() string {
	w := s.t("workflows")
	e := s.t("executions")
	st := s.t("execution_steps")
	j := s.t("compensation_journal")

	if _, ok := s.dialect.(PostgresDialect); ok {
		return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    version INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    definition JSONB NOT NULL,
    labels JSONB,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_%s_ns ON %s(namespace);

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(36) NOT NULL,
    version INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    input JSONB,
    output JSONB,
    labels JSONB,
    metadata JSONB,
    variables JSONB,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP WITH TIME ZONE NULL
);
CREATE INDEX IF NOT EXISTS idx_%s_ns_status ON %s(namespace, status);

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    execution_id VARCHAR(36) NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    input JSONB,
    output JSONB,
    error TEXT,
    labels JSONB,
    task_meta JSONB,
    attempt_number INT DEFAULT 0,
    worker_id VARCHAR(255),
    scheduled_at TIMESTAMP WITH TIME ZONE NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP WITH TIME ZONE NULL
);
CREATE INDEX IF NOT EXISTS idx_%s_claim ON %s(namespace, status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_%s_exec ON %s(execution_id);

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    execution_id VARCHAR(36) NOT NULL,
    failed_node_id VARCHAR(255),
    compensate_node_id VARCHAR(255),
    reason TEXT,
    actor VARCHAR(255),
    input_snapshot TEXT,
    output_snapshot TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_%s_exec ON %s(execution_id);
`, w, s.opts.TablePrefix+"wf", w,
			e, s.opts.TablePrefix+"exec", e,
			st, s.opts.TablePrefix+"steps", st, s.opts.TablePrefix+"steps_exec", st,
			j, s.opts.TablePrefix+"jrnl", j)
	}

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    version INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    definition JSON NOT NULL,
    labels JSON,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_wf_namespace (namespace)
);

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(36) NOT NULL,
    version INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    input JSON,
    output JSON,
    labels JSON,
    metadata JSON,
    variables JSON,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_exec_namespace_status (namespace, status),
    INDEX idx_exec_workflow (workflow_id)
);

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    execution_id VARCHAR(36) NOT NULL,
    node_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    input JSON,
    output JSON,
    error TEXT,
    labels JSON,
    task_meta JSON,
    attempt_number INT DEFAULT 0,
    worker_id VARCHAR(255),
    scheduled_at TIMESTAMP NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_steps_claim (namespace, status, scheduled_at),
    INDEX idx_steps_execution (execution_id)
);

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    execution_id VARCHAR(36) NOT NULL,
    failed_node_id VARCHAR(255),
    compensate_node_id VARCHAR(255),
    reason TEXT,
    actor VARCHAR(255),
    input_snapshot TEXT,
    output_snapshot TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_jrnl_execution (execution_id)
);
`, w, e, st, j)
}

type taskMetaPayload struct {
	Assignees       []string         `json:"assignees,omitempty"`
	CandidateGroups []string         `json:"candidateGroups,omitempty"`
	ClaimMode       models.ClaimMode `json:"claimMode,omitempty"`
	ClaimedBy       string           `json:"claimedBy,omitempty"`
	ClaimedAt       *time.Time       `json:"claimedAt,omitempty"`
	DueAt           *time.Time       `json:"dueAt,omitempty"`
	Priority        int              `json:"priority,omitempty"`
	Action          string           `json:"action,omitempty"`
}

func encodeTaskMeta(step *models.ExecutionStep) []byte {
	p := taskMetaPayload{
		Assignees: step.Assignees, CandidateGroups: step.CandidateGroups,
		ClaimMode: step.ClaimMode, ClaimedBy: step.ClaimedBy, ClaimedAt: step.ClaimedAt,
		DueAt: step.DueAt, Priority: step.Priority, Action: step.Action,
	}
	b, _ := json.Marshal(p)
	return b
}

func decodeTaskMeta(step *models.ExecutionStep, raw []byte) {
	if len(raw) == 0 {
		return
	}
	var p taskMetaPayload
	if json.Unmarshal(raw, &p) != nil {
		return
	}
	step.Assignees = p.Assignees
	step.CandidateGroups = p.CandidateGroups
	step.ClaimMode = p.ClaimMode
	step.ClaimedBy = p.ClaimedBy
	step.ClaimedAt = p.ClaimedAt
	step.DueAt = p.DueAt
	step.Priority = p.Priority
	step.Action = p.Action
}

func nullStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func decodeLabels(raw []byte) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]string
	if json.Unmarshal(raw, &m) != nil {
		return nil
	}
	return m
}

// execRow is a SQL-scan DTO. Avoid embedding models.Execution: Labels/Metadata are
// db:"-" on the model while the table has labels/metadata JSON columns, and
// input/output/error_message can be NULL (database/sql cannot scan NULL into string).
type execRow struct {
	ID           string         `db:"id"`
	Namespace    string         `db:"namespace"`
	WorkflowID   string         `db:"workflow_id"`
	Version      int            `db:"version"`
	Status       string         `db:"status"`
	Input        sql.NullString `db:"input"`
	Output       sql.NullString `db:"output"`
	LabelsJSON   []byte         `db:"labels"`
	MetadataJSON []byte         `db:"metadata"`
	VariablesJSON []byte        `db:"variables"`
	ErrorMessage sql.NullString `db:"error_message"`
	StartedAt    time.Time      `db:"started_at"`
	FinishedAt   *time.Time     `db:"finished_at"`
}

// stepRow is a SQL-scan DTO for the same NULL / labels reasons as execRow.
type stepRow struct {
	ID            string         `db:"id"`
	Namespace     string         `db:"namespace"`
	ExecutionID   string         `db:"execution_id"`
	NodeID        string         `db:"node_id"`
	Status        string         `db:"status"`
	Input         sql.NullString `db:"input"`
	Output        sql.NullString `db:"output"`
	Error         sql.NullString `db:"error"`
	LabelsJSON    []byte         `db:"labels"`
	TaskMeta      []byte         `db:"task_meta"`
	AttemptNumber int            `db:"attempt_number"`
	WorkerID      sql.NullString `db:"worker_id"`
	ScheduledAt   *time.Time     `db:"scheduled_at"`
	StartedAt     time.Time      `db:"started_at"`
	UpdatedAt     time.Time      `db:"updated_at"`
	FinishedAt    *time.Time     `db:"finished_at"`
}

func (s *SQLStorage) SaveWorkflow(ctx context.Context, namespace string, wf *models.Workflow) error {
	wf.Namespace = namespace
	definitionJSON, err := json.Marshal(wf)
	if err != nil {
		return err
	}
	q := s.ph(fmt.Sprintf(`INSERT INTO %s (id, namespace, name, description, version, status, definition, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.t("workflows")))
	_, err = s.db.ExecContext(ctx, q,
		wf.ID, wf.Namespace, wf.Name, wf.Description, wf.Version, wf.Status, definitionJSON, wf.CreatedAt, wf.UpdatedAt)
	return err
}

func (s *SQLStorage) GetWorkflow(ctx context.Context, namespace string, id string) (*models.Workflow, error) {
	q := s.ph(fmt.Sprintf("SELECT definition FROM %s WHERE id = ? AND namespace = ?", s.t("workflows")))
	var definitionJSON []byte
	if err := s.db.GetContext(ctx, &definitionJSON, q, id, namespace); err != nil {
		return nil, err
	}
	var wf models.Workflow
	if err := json.Unmarshal(definitionJSON, &wf); err != nil {
		return nil, err
	}
	return &wf, nil
}

func (s *SQLStorage) ListWorkflows(ctx context.Context, namespace string) ([]*models.Workflow, error) {
	q := s.ph(fmt.Sprintf("SELECT definition FROM %s WHERE namespace = ?", s.t("workflows")))
	var definitions [][]byte
	if err := s.db.SelectContext(ctx, &definitions, q, namespace); err != nil {
		return nil, err
	}
	var wfs []*models.Workflow
	for _, d := range definitions {
		var wf models.Workflow
		if err := json.Unmarshal(d, &wf); err != nil {
			return nil, err
		}
		wfs = append(wfs, &wf)
	}
	return wfs, nil
}

func (s *SQLStorage) CreateExecution(ctx context.Context, namespace string, exec *models.Execution) error {
	exec.Namespace = namespace
	input := exec.Input
	if s.crypto != nil {
		encryptedInput, err := s.crypto.Encrypt(input)
		if err != nil {
			return err
		}
		input = encryptedInput
	}
	vars, _ := json.Marshal(exec.Variables)
	q := s.ph(fmt.Sprintf(`INSERT INTO %s (id, namespace, workflow_id, version, status, input, variables, started_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, s.t("executions")))
	_, err := s.db.ExecContext(ctx, q,
		exec.ID, exec.Namespace, exec.WorkflowID, exec.Version, exec.Status, input, vars, exec.StartedAt)
	return err
}

func (s *SQLStorage) mapExec(row *execRow) *models.Execution {
	exec := &models.Execution{
		ID:           row.ID,
		Namespace:    row.Namespace,
		WorkflowID:   row.WorkflowID,
		Version:      row.Version,
		Status:       models.TaskStatus(row.Status),
		Input:        nullStr(row.Input),
		Output:       nullStr(row.Output),
		ErrorMessage: nullStr(row.ErrorMessage),
		Labels:       decodeLabels(row.LabelsJSON),
		StartedAt:    row.StartedAt,
		FinishedAt:   row.FinishedAt,
	}
	if len(row.MetadataJSON) > 0 {
		_ = json.Unmarshal(row.MetadataJSON, &exec.Metadata)
	}
	if len(row.VariablesJSON) > 0 {
		_ = json.Unmarshal(row.VariablesJSON, &exec.Variables)
	}
	if exec.Variables == nil {
		exec.Variables = map[string]interface{}{}
	}
	if s.crypto != nil {
		dIn, _ := s.crypto.Decrypt(exec.Input)
		exec.Input = dIn
		dOut, _ := s.crypto.Decrypt(exec.Output)
		exec.Output = dOut
	}
	return exec
}

func (s *SQLStorage) GetExecution(ctx context.Context, namespace string, id string) (*models.Execution, error) {
	q := s.ph(fmt.Sprintf("SELECT * FROM %s WHERE id = ? AND namespace = ?", s.t("executions")))
	var row execRow
	if err := s.db.GetContext(ctx, &row, q, id, namespace); err != nil {
		return nil, err
	}
	return s.mapExec(&row), nil
}

func (s *SQLStorage) UpdateExecution(ctx context.Context, namespace string, exec *models.Execution) error {
	output := exec.Output
	if s.crypto != nil {
		encryptedOutput, _ := s.crypto.Encrypt(output)
		output = encryptedOutput
	}
	vars, _ := json.Marshal(exec.Variables)
	q := s.ph(fmt.Sprintf(`UPDATE %s SET status = ?, output = ?, error_message = ?, finished_at = ?, variables = ? WHERE id = ? AND namespace = ?`, s.t("executions")))
	_, err := s.db.ExecContext(ctx, q,
		exec.Status, output, exec.ErrorMessage, exec.FinishedAt, vars, exec.ID, namespace)
	return err
}

func (s *SQLStorage) ListExecutions(ctx context.Context, namespace string) ([]*models.Execution, error) {
	q := s.ph(fmt.Sprintf("SELECT * FROM %s WHERE namespace = ?", s.t("executions")))
	var rows []execRow
	if err := s.db.SelectContext(ctx, &rows, q, namespace); err != nil {
		return nil, err
	}
	execs := make([]*models.Execution, 0, len(rows))
	for i := range rows {
		execs = append(execs, s.mapExec(&rows[i]))
	}
	return execs, nil
}

func (s *SQLStorage) CreateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	step.Namespace = namespace
	input := step.Input
	if s.crypto != nil {
		encryptedInput, _ := s.crypto.Encrypt(input)
		input = encryptedInput
	}
	meta := encodeTaskMeta(step)
	q := s.ph(fmt.Sprintf(`INSERT INTO %s (id, namespace, execution_id, node_id, status, input, task_meta, worker_id, scheduled_at, started_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.t("execution_steps")))
	now := time.Now()
	_, err := s.db.ExecContext(ctx, q,
		step.ID, step.Namespace, step.ExecutionID, step.NodeID, step.Status, input, meta, step.WorkerID, step.ScheduledAt, step.StartedAt, now)
	return err
}

func (s *SQLStorage) UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	output := step.Output
	errStr := step.Error
	if s.crypto != nil {
		eOutput, _ := s.crypto.Encrypt(output)
		output = eOutput
		eError, _ := s.crypto.Encrypt(errStr)
		errStr = eError
	}
	meta := encodeTaskMeta(step)
	q := s.ph(fmt.Sprintf(`UPDATE %s SET status = ?, output = ?, error = ?, finished_at = ?, attempt_number = ?, scheduled_at = ?, updated_at = ?, worker_id = ?, task_meta = ? WHERE id = ? AND namespace = ?`, s.t("execution_steps")))
	_, err := s.db.ExecContext(ctx, q,
		step.Status, output, errStr, step.FinishedAt, step.AttemptNumber, step.ScheduledAt, time.Now(), step.WorkerID, meta, step.ID, namespace)
	return err
}

func (s *SQLStorage) mapStep(row *stepRow) *models.ExecutionStep {
	step := &models.ExecutionStep{
		ID:            row.ID,
		Namespace:     row.Namespace,
		ExecutionID:   row.ExecutionID,
		NodeID:        row.NodeID,
		Status:        models.TaskStatus(row.Status),
		Input:         nullStr(row.Input),
		Output:        nullStr(row.Output),
		Error:         nullStr(row.Error),
		AttemptNumber: row.AttemptNumber,
		WorkerID:      nullStr(row.WorkerID),
		Labels:        decodeLabels(row.LabelsJSON),
		ScheduledAt:   row.ScheduledAt,
		StartedAt:     row.StartedAt,
		UpdatedAt:     row.UpdatedAt,
		FinishedAt:    row.FinishedAt,
	}
	decodeTaskMeta(step, row.TaskMeta)
	if s.crypto != nil {
		dInput, _ := s.crypto.Decrypt(step.Input)
		step.Input = dInput
		dOutput, _ := s.crypto.Decrypt(step.Output)
		step.Output = dOutput
		dError, _ := s.crypto.Decrypt(step.Error)
		step.Error = dError
	}
	return step
}

func (s *SQLStorage) GetExecutionSteps(ctx context.Context, namespace string, executionID string) ([]*models.ExecutionStep, error) {
	q := s.ph(fmt.Sprintf("SELECT * FROM %s WHERE execution_id = ? AND namespace = ?", s.t("execution_steps")))
	var rows []stepRow
	if err := s.db.SelectContext(ctx, &rows, q, executionID, namespace); err != nil {
		return nil, err
	}
	steps := make([]*models.ExecutionStep, 0, len(rows))
	for i := range rows {
		steps = append(steps, s.mapStep(&rows[i]))
	}
	return steps, nil
}

func (s *SQLStorage) ClaimReadyStep(ctx context.Context, namespace string, workerID string) (*models.ExecutionStep, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	q := fmt.Sprintf("SELECT * FROM %s WHERE (status = 'PENDING' AND (scheduled_at IS NULL OR scheduled_at <= ?)) OR (status = 'RUNNING' AND updated_at <= ?)", s.t("execution_steps"))
	args := []interface{}{time.Now(), time.Now().Add(-5 * time.Minute)}
	if namespace != "" {
		q += " AND namespace = ?"
		args = append(args, namespace)
	}
	q += " LIMIT 1"
	q = s.dialect.SkipLocked(s.ph(q))

	var row stepRow
	err = tx.GetContext(ctx, &row, q, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	updateQuery := s.ph(fmt.Sprintf("UPDATE %s SET status = ?, worker_id = ?, started_at = ?, updated_at = ? WHERE id = ? AND namespace = ?", s.t("execution_steps")))
	if _, err = tx.ExecContext(ctx, updateQuery, "RUNNING", workerID, time.Now(), time.Now(), row.ID, row.Namespace); err != nil {
		return nil, err
	}

	step := s.mapStep(&row)
	step.Status = models.TaskRunning
	step.WorkerID = workerID
	return step, tx.Commit()
}

func (s *SQLStorage) ListWaitingSteps(ctx context.Context, namespace string, filter storage.TaskFilter) ([]*models.ExecutionStep, error) {
	q := fmt.Sprintf("SELECT * FROM %s WHERE namespace = ? AND status = 'WAITING'", s.t("execution_steps"))
	args := []interface{}{namespace}
	if filter.ExecutionID != "" {
		q += " AND execution_id = ?"
		args = append(args, filter.ExecutionID)
	}
	if filter.NodeID != "" {
		q += " AND node_id = ?"
		args = append(args, filter.NodeID)
	}
	var rows []stepRow
	if err := s.db.SelectContext(ctx, &rows, s.ph(q), args...); err != nil {
		return nil, err
	}
	var out []*models.ExecutionStep
	for i := range rows {
		step := s.mapStep(&rows[i])
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
	return out, nil
}

func (s *SQLStorage) GetExecutionStepByNode(ctx context.Context, namespace, executionID, nodeID string) (*models.ExecutionStep, error) {
	q := s.ph(fmt.Sprintf("SELECT * FROM %s WHERE namespace = ? AND execution_id = ? AND node_id = ? ORDER BY started_at DESC LIMIT 1", s.t("execution_steps")))
	var row stepRow
	if err := s.db.GetContext(ctx, &row, q, namespace, executionID, nodeID); err != nil {
		return nil, fmt.Errorf("step not found for node %s: %w", nodeID, err)
	}
	return s.mapStep(&row), nil
}

func (s *SQLStorage) AppendCompensationRecord(ctx context.Context, namespace string, rec *models.CompensationRecord) error {
	rec.Namespace = namespace
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	q := s.ph(fmt.Sprintf(`INSERT INTO %s (id, namespace, execution_id, failed_node_id, compensate_node_id, reason, actor, input_snapshot, output_snapshot, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.t("compensation_journal")))
	_, err := s.db.ExecContext(ctx, q,
		rec.ID, rec.Namespace, rec.ExecutionID, rec.FailedNodeID, rec.CompensateNodeID,
		rec.Reason, rec.Actor, rec.InputSnapshot, rec.OutputSnapshot, rec.CreatedAt)
	return err
}

func (s *SQLStorage) ListCompensationRecords(ctx context.Context, namespace, executionID string) ([]*models.CompensationRecord, error) {
	q := s.ph(fmt.Sprintf("SELECT * FROM %s WHERE namespace = ? AND execution_id = ? ORDER BY created_at ASC", s.t("compensation_journal")))
	var recs []*models.CompensationRecord
	if err := s.db.SelectContext(ctx, &recs, q, namespace, executionID); err != nil {
		return nil, err
	}
	return recs, nil
}
