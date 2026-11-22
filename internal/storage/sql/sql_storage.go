package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dgmos/flow/internal/models"
	"github.com/dgmos/flow/internal/storage"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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
			b.WriteString(strconv.Itoa(n))
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
	if err := s.upgradeSchedulesSchema(); err != nil {
		return err
	}
	log.Println("✅ Database migrations completed successfully")
	return nil
}

func (s *SQLStorage) upgradeSchedulesSchema() error {
	tbl := s.t("schedules")
	var alters []string
	if _, ok := s.dialect.(PostgresDialect); ok {
		alters = []string{
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS name VARCHAR(255) NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS timezone VARCHAR(64) NOT NULL DEFAULT 'UTC'`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS catch_up BOOLEAN NOT NULL DEFAULT FALSE`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS catch_up_max INT NOT NULL DEFAULT 0`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS catch_up_lookback_sec INT NOT NULL DEFAULT 0`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS starts_at TIMESTAMP WITH TIME ZONE NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS ends_at TIMESTAMP WITH TIME ZONE NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_run_at TIMESTAMP WITH TIME ZONE NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMP WITH TIME ZONE NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_exec_id VARCHAR(36) NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS last_error TEXT NULL`, tbl),
		}
	} else {
		alters = []string{
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN name VARCHAR(255) NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN timezone VARCHAR(64) NOT NULL DEFAULT 'UTC'`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN catch_up TINYINT(1) NOT NULL DEFAULT 0`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN catch_up_max INT NOT NULL DEFAULT 0`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN catch_up_lookback_sec INT NOT NULL DEFAULT 0`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN starts_at TIMESTAMP NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN ends_at TIMESTAMP NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN last_run_at TIMESTAMP NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN next_run_at TIMESTAMP NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN last_exec_id VARCHAR(36) NULL`, tbl),
			fmt.Sprintf(`ALTER TABLE %s ADD COLUMN last_error TEXT NULL`, tbl),
		}
	}
	for _, q := range alters {
		if _, err := s.db.Exec(q); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate") || strings.Contains(msg, "exists") {
				continue
			}
			return fmt.Errorf("schedule schema upgrade: %w", err)
		}
	}
	return nil
}

func (s *SQLStorage) buildSchema() string {
	w := s.t("workflows")
	e := s.t("executions")
	st := s.t("execution_steps")
	j := s.t("compensation_journal")
	sch := s.t("schedules")

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
CREATE INDEX IF NOT EXISTS idx_%s_waiting ON %s(namespace, updated_at) WHERE status = 'WAITING';

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

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(36) NOT NULL,
    name VARCHAR(255) NULL,
    cron_expression VARCHAR(100) NOT NULL,
    input JSONB DEFAULT NULL,
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    catch_up BOOLEAN NOT NULL DEFAULT FALSE,
    catch_up_max INT NOT NULL DEFAULT 0,
    catch_up_lookback_sec INT NOT NULL DEFAULT 0,
    starts_at TIMESTAMP WITH TIME ZONE NULL,
    ends_at TIMESTAMP WITH TIME ZONE NULL,
    last_run_at TIMESTAMP WITH TIME ZONE NULL,
    next_run_at TIMESTAMP WITH TIME ZONE NULL,
    last_exec_id VARCHAR(36) NULL,
    last_error TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_%s_ns_active ON %s(namespace, is_active);
CREATE INDEX IF NOT EXISTS idx_%s_active ON %s(is_active);
`, w, s.opts.TablePrefix+"wf", w,
			e, s.opts.TablePrefix+"exec", e,
			st, s.opts.TablePrefix+"steps", st, s.opts.TablePrefix+"steps_exec", st, s.opts.TablePrefix+"steps_waiting", st,
			j, s.opts.TablePrefix+"jrnl", j,
			sch, s.opts.TablePrefix+"sched_ns", sch, s.opts.TablePrefix+"sched_act", sch)
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
    INDEX idx_steps_execution (execution_id),
    INDEX idx_steps_waiting (namespace, status, updated_at)
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

CREATE TABLE IF NOT EXISTS %s (
    id VARCHAR(36) PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL,
    workflow_id VARCHAR(36) NOT NULL,
    name VARCHAR(255) NULL,
    cron_expression VARCHAR(100) NOT NULL,
    input JSON DEFAULT NULL,
    timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    catch_up TINYINT(1) NOT NULL DEFAULT 0,
    catch_up_max INT NOT NULL DEFAULT 0,
    catch_up_lookback_sec INT NOT NULL DEFAULT 0,
    starts_at TIMESTAMP NULL,
    ends_at TIMESTAMP NULL,
    last_run_at TIMESTAMP NULL,
    next_run_at TIMESTAMP NULL,
    last_exec_id VARCHAR(36) NULL,
    last_error TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_schedules_ns_active (namespace, is_active),
    INDEX idx_schedules_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`, w, e, st, j, sch)
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

// jsonColumnArg maps empty strings to SQL NULL for JSON columns (MySQL/Postgres reject "").
func jsonColumnArg(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
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
	ID            string         `db:"id"`
	Namespace     string         `db:"namespace"`
	WorkflowID    string         `db:"workflow_id"`
	Version       int            `db:"version"`
	Status        string         `db:"status"`
	Input         sql.NullString `db:"input"`
	Output        sql.NullString `db:"output"`
	LabelsJSON    []byte         `db:"labels"`
	MetadataJSON  []byte         `db:"metadata"`
	VariablesJSON []byte         `db:"variables"`
	ErrorMessage  sql.NullString `db:"error_message"`
	StartedAt     time.Time      `db:"started_at"`
	FinishedAt    *time.Time     `db:"finished_at"`
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
	// Saving an existing workflow of the same namespace updates it (re-install,
	// activation changes); an id owned by another namespace still fails on the
	// primary key instead of being overwritten.
	var n int
	exists := s.ph(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ? AND namespace = ?", s.t("workflows")))
	if err := s.db.GetContext(ctx, &n, exists, wf.ID, wf.Namespace); err != nil {
		return err
	}
	if n > 0 {
		q := s.ph(fmt.Sprintf(`UPDATE %s SET name = ?, description = ?, version = ?, status = ?, definition = ?, updated_at = ?
			WHERE id = ? AND namespace = ?`, s.t("workflows")))
		_, err = s.db.ExecContext(ctx, q,
			wf.Name, wf.Description, wf.Version, wf.Status, definitionJSON, wf.UpdatedAt, wf.ID, wf.Namespace)
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
		exec.ID, exec.Namespace, exec.WorkflowID, exec.Version, exec.Status, jsonColumnArg(input), vars, exec.StartedAt)
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
	_, err := s.updateExec(ctx, namespace, exec, "")
	return err
}

func (s *SQLStorage) updateExec(ctx context.Context, namespace string, exec *models.Execution, cond string, condArgs ...interface{}) (bool, error) {
	output := exec.Output
	if s.crypto != nil {
		encryptedOutput, _ := s.crypto.Encrypt(output)
		output = encryptedOutput
	}
	vars, _ := json.Marshal(exec.Variables)
	q := s.ph(fmt.Sprintf(`UPDATE %s SET status = ?, output = ?, error_message = ?, finished_at = ?, variables = ? WHERE id = ? AND namespace = ?`+cond, s.t("executions")))
	args := []interface{}{exec.Status, jsonColumnArg(output), exec.ErrorMessage, exec.FinishedAt, vars, exec.ID, namespace}
	res, err := s.db.ExecContext(ctx, q, append(args, condArgs...)...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
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
		step.ID, step.Namespace, step.ExecutionID, step.NodeID, step.Status, jsonColumnArg(input), meta, step.WorkerID, step.ScheduledAt, step.StartedAt, now)
	if err != nil && isDuplicateKeyErr(err) {
		return storage.ErrStepExists
	}
	return err
}

// isDuplicateKeyErr reports whether err is a primary/unique key violation.
func isDuplicateKeyErr(err error) bool {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == 1062
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505"
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") || strings.Contains(msg, "duplicate entry") || strings.Contains(msg, "unique constraint")
}

// HeartbeatStep extends the lease of a RUNNING step owned by workerID.
func (s *SQLStorage) HeartbeatStep(ctx context.Context, namespace, stepID, workerID string) (bool, error) {
	q := s.ph(fmt.Sprintf("UPDATE %s SET updated_at = ? WHERE id = ? AND namespace = ? AND status = 'RUNNING' AND worker_id = ?", s.t("execution_steps")))
	res, err := s.db.ExecContext(ctx, q, time.Now(), stepID, namespace, workerID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n > 0 {
		return true, nil
	}
	// MySQL reports changed (not matched) rows: a heartbeat in the same second
	// as the previous write changes nothing. Confirm ownership explicitly.
	var owned int
	cq := s.ph(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = ? AND namespace = ? AND status = 'RUNNING' AND worker_id = ?", s.t("execution_steps")))
	if err := s.db.GetContext(ctx, &owned, cq, stepID, namespace, workerID); err != nil {
		return false, err
	}
	return owned > 0, nil
}

// UpdateExecutionStepIf implements storage.ConditionalUpdater.
func (s *SQLStorage) UpdateExecutionStepIf(ctx context.Context, namespace string, step *models.ExecutionStep, from models.TaskStatus, worker string) (bool, error) {
	cond := " AND status = ?"
	extra := []interface{}{string(from)}
	if worker != "" {
		cond += " AND worker_id = ?"
		extra = append(extra, worker)
	}
	return s.updateStep(ctx, namespace, step, cond, extra...)
}

// UpdateExecutionIf implements storage.ConditionalUpdater.
func (s *SQLStorage) UpdateExecutionIf(ctx context.Context, namespace string, exec *models.Execution, from models.TaskStatus) (bool, error) {
	return s.updateExec(ctx, namespace, exec, " AND status = ?", string(from))
}

func (s *SQLStorage) UpdateExecutionStep(ctx context.Context, namespace string, step *models.ExecutionStep) error {
	_, err := s.updateStep(ctx, namespace, step, "")
	return err
}

func (s *SQLStorage) updateStep(ctx context.Context, namespace string, step *models.ExecutionStep, cond string, condArgs ...interface{}) (bool, error) {
	output := step.Output
	errStr := step.Error
	if s.crypto != nil {
		eOutput, _ := s.crypto.Encrypt(output)
		output = eOutput
		eError, _ := s.crypto.Encrypt(errStr)
		errStr = eError
	}
	meta := encodeTaskMeta(step)
	q := s.ph(fmt.Sprintf(`UPDATE %s SET status = ?, output = ?, error = ?, finished_at = ?, attempt_number = ?, scheduled_at = ?, updated_at = ?, worker_id = ?, task_meta = ? WHERE id = ? AND namespace = ?`+cond, s.t("execution_steps")))
	args := []interface{}{step.Status, jsonColumnArg(output), errStr, step.FinishedAt, step.AttemptNumber, step.ScheduledAt, time.Now(), step.WorkerID, meta, step.ID, namespace}
	res, err := s.db.ExecContext(ctx, q, append(args, condArgs...)...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
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

	// The PENDING/stale-RUNNING alternatives must be parenthesised as a group:
	// AND binds tighter than OR, so an unparenthesised namespace filter would
	// only apply to the RUNNING branch and leak PENDING steps across namespaces.
	q := fmt.Sprintf("SELECT * FROM %s WHERE ((status = 'PENDING' AND (scheduled_at IS NULL OR scheduled_at <= ?)) OR (status = 'RUNNING' AND updated_at <= ?))", s.t("execution_steps"))
	now := time.Now()
	args := []interface{}{now, now.Add(-storage.StaleStepThreshold)}
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
	if _, err = tx.ExecContext(ctx, updateQuery, "RUNNING", workerID, now, now, row.ID, row.Namespace); err != nil {
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

type scheduleRow struct {
	ID                 string         `db:"id"`
	Namespace          string         `db:"namespace"`
	WorkflowID         string         `db:"workflow_id"`
	Name               sql.NullString `db:"name"`
	CronExpression     string         `db:"cron_expression"`
	Input              sql.NullString `db:"input"`
	Timezone           string         `db:"timezone"`
	IsActive           bool           `db:"is_active"`
	CatchUp            bool           `db:"catch_up"`
	CatchUpMax         int            `db:"catch_up_max"`
	CatchUpLookbackSec int            `db:"catch_up_lookback_sec"`
	StartsAt           sql.NullTime   `db:"starts_at"`
	EndsAt             sql.NullTime   `db:"ends_at"`
	LastRunAt          sql.NullTime   `db:"last_run_at"`
	NextRunAt          sql.NullTime   `db:"next_run_at"`
	LastExecID         sql.NullString `db:"last_exec_id"`
	LastError          sql.NullString `db:"last_error"`
	CreatedAt          time.Time      `db:"created_at"`
	UpdatedAt          time.Time      `db:"updated_at"`
}

func (s *SQLStorage) mapSchedule(row *scheduleRow) *models.Schedule {
	out := &models.Schedule{
		ID:                 row.ID,
		Namespace:          row.Namespace,
		WorkflowID:         row.WorkflowID,
		Name:               row.Name.String,
		CronExpression:     row.CronExpression,
		Input:              row.Input.String,
		Timezone:           row.Timezone,
		IsActive:           row.IsActive,
		CatchUp:            row.CatchUp,
		CatchUpMax:         row.CatchUpMax,
		CatchUpLookbackSec: row.CatchUpLookbackSec,
		LastExecID:         row.LastExecID.String,
		LastError:          row.LastError.String,
		CreatedAt:          row.CreatedAt,
		UpdatedAt:          row.UpdatedAt,
	}
	if out.Timezone == "" {
		out.Timezone = "UTC"
	}
	if row.StartsAt.Valid {
		t := row.StartsAt.Time
		out.StartsAt = &t
	}
	if row.EndsAt.Valid {
		t := row.EndsAt.Time
		out.EndsAt = &t
	}
	if row.LastRunAt.Valid {
		t := row.LastRunAt.Time
		out.LastRunAt = &t
	}
	if row.NextRunAt.Valid {
		t := row.NextRunAt.Time
		out.NextRunAt = &t
	}
	return out
}

func scheduleSelectCols() string {
	return `id, namespace, workflow_id, name, cron_expression, input, timezone, is_active, catch_up, catch_up_max, catch_up_lookback_sec, starts_at, ends_at, last_run_at, next_run_at, last_exec_id, last_error, created_at, updated_at`
}

func (s *SQLStorage) SaveSchedule(ctx context.Context, sched *models.Schedule) error {
	if sched.CreatedAt.IsZero() {
		sched.CreatedAt = time.Now().UTC()
	}
	sched.UpdatedAt = time.Now().UTC()
	if sched.Timezone == "" {
		sched.Timezone = "UTC"
	}

	var input interface{}
	if sched.Input == "" {
		input = nil
	} else {
		input = sched.Input
	}

	tbl := s.t("schedules")
	cols := `(id, namespace, workflow_id, name, cron_expression, input, timezone, is_active, catch_up, catch_up_max, catch_up_lookback_sec, starts_at, ends_at, last_run_at, next_run_at, last_exec_id, last_error, created_at, updated_at)`
	vals := `VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var q string
	if _, ok := s.dialect.(PostgresDialect); ok {
		q = s.ph(fmt.Sprintf(`INSERT INTO %s %s %s
			ON CONFLICT (id) DO UPDATE SET
				name = EXCLUDED.name,
				cron_expression = EXCLUDED.cron_expression,
				input = EXCLUDED.input,
				timezone = EXCLUDED.timezone,
				is_active = EXCLUDED.is_active,
				catch_up = EXCLUDED.catch_up,
				catch_up_max = EXCLUDED.catch_up_max,
				catch_up_lookback_sec = EXCLUDED.catch_up_lookback_sec,
				starts_at = EXCLUDED.starts_at,
				ends_at = EXCLUDED.ends_at,
				next_run_at = EXCLUDED.next_run_at,
				updated_at = EXCLUDED.updated_at,
				workflow_id = EXCLUDED.workflow_id,
				namespace = EXCLUDED.namespace`, tbl, cols, vals))
	} else {
		q = s.ph(fmt.Sprintf(`INSERT INTO %s %s %s
			ON DUPLICATE KEY UPDATE
				name = VALUES(name),
				cron_expression = VALUES(cron_expression),
				input = VALUES(input),
				timezone = VALUES(timezone),
				is_active = VALUES(is_active),
				catch_up = VALUES(catch_up),
				catch_up_max = VALUES(catch_up_max),
				catch_up_lookback_sec = VALUES(catch_up_lookback_sec),
				starts_at = VALUES(starts_at),
				ends_at = VALUES(ends_at),
				next_run_at = VALUES(next_run_at),
				updated_at = VALUES(updated_at),
				workflow_id = VALUES(workflow_id),
				namespace = VALUES(namespace)`, tbl, cols, vals))
	}

	_, err := s.db.ExecContext(ctx, q,
		sched.ID, sched.Namespace, sched.WorkflowID, nullStrPtr(sched.Name), sched.CronExpression,
		input, sched.Timezone, sched.IsActive, sched.CatchUp, sched.CatchUpMax, sched.CatchUpLookbackSec,
		sched.StartsAt, sched.EndsAt, sched.LastRunAt, sched.NextRunAt,
		nullStrPtr(sched.LastExecID), nullStrPtr(sched.LastError), sched.CreatedAt, sched.UpdatedAt)
	if err != nil {
		return fmt.Errorf("sql_storage: failed to save schedule: %w", err)
	}
	return nil
}

func nullStrPtr(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}

func (s *SQLStorage) DeleteSchedule(ctx context.Context, namespace, id string) error {
	q := s.ph(fmt.Sprintf(`DELETE FROM %s WHERE namespace = ? AND id = ?`, s.t("schedules")))
	result, err := s.db.ExecContext(ctx, q, namespace, id)
	if err != nil {
		return fmt.Errorf("sql_storage: failed to delete schedule: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("sql_storage: schedule not found: namespace=%s, id=%s", namespace, id)
	}
	return nil
}

func (s *SQLStorage) GetActiveSchedules(ctx context.Context) ([]*models.Schedule, error) {
	q := s.ph(fmt.Sprintf(`SELECT %s FROM %s WHERE is_active = ? AND (ends_at IS NULL OR ends_at > ?)`, scheduleSelectCols(), s.t("schedules")))
	var rows []scheduleRow
	if err := s.db.SelectContext(ctx, &rows, q, true, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("sql_storage: failed to query active schedules: %w", err)
	}
	out := make([]*models.Schedule, 0, len(rows))
	for i := range rows {
		out = append(out, s.mapSchedule(&rows[i]))
	}
	return out, nil
}

func (s *SQLStorage) GetSchedule(ctx context.Context, namespace, id string) (*models.Schedule, error) {
	q := s.ph(fmt.Sprintf(`SELECT %s FROM %s WHERE namespace = ? AND id = ?`, scheduleSelectCols(), s.t("schedules")))
	var row scheduleRow
	if err := s.db.GetContext(ctx, &row, q, namespace, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("sql_storage: schedule not found")
		}
		return nil, fmt.Errorf("sql_storage: failed to get schedule: %w", err)
	}
	return s.mapSchedule(&row), nil
}

func (s *SQLStorage) ListSchedules(ctx context.Context, namespace string, filter storage.ScheduleFilter) ([]*models.Schedule, error) {
	q := fmt.Sprintf(`SELECT %s FROM %s WHERE namespace = ?`, scheduleSelectCols(), s.t("schedules"))
	args := []interface{}{namespace}
	if filter.ActiveOnly {
		q += ` AND is_active = ?`
		args = append(args, true)
	}
	q += ` ORDER BY created_at ASC`
	if filter.Limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, filter.Limit)
		if filter.Offset > 0 {
			q += fmt.Sprintf(` OFFSET %d`, filter.Offset)
		}
	} else if filter.Offset > 0 {
		q += fmt.Sprintf(` LIMIT 18446744073709551615 OFFSET %d`, filter.Offset)
	}
	var rows []scheduleRow
	if err := s.db.SelectContext(ctx, &rows, s.ph(q), args...); err != nil {
		return nil, fmt.Errorf("sql_storage: failed to list schedules: %w", err)
	}
	out := make([]*models.Schedule, 0, len(rows))
	for i := range rows {
		out = append(out, s.mapSchedule(&rows[i]))
	}
	return out, nil
}

func (s *SQLStorage) TryClaimScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time) (bool, error) {
	fireAt = fireAt.UTC().Truncate(time.Second)
	q := s.ph(fmt.Sprintf(`UPDATE %s SET last_run_at = ?, updated_at = ?
		WHERE id = ? AND namespace = ? AND is_active = ?
		AND (last_run_at IS NULL OR last_run_at < ?)`, s.t("schedules")))
	res, err := s.db.ExecContext(ctx, q, fireAt, time.Now().UTC(), id, namespace, true, fireAt)
	if err != nil {
		return false, fmt.Errorf("sql_storage: claim schedule run failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLStorage) RecordScheduleRun(ctx context.Context, namespace, id string, fireAt time.Time, execID, lastErr string, nextRun *time.Time) error {
	fireAt = fireAt.UTC().Truncate(time.Second)
	q := s.ph(fmt.Sprintf(`UPDATE %s SET last_run_at = ?, last_exec_id = ?, last_error = ?, next_run_at = ?, updated_at = ?
		WHERE id = ? AND namespace = ?`, s.t("schedules")))
	_, err := s.db.ExecContext(ctx, q, fireAt, nullStrPtr(execID), nullStrPtr(lastErr), nextRun, time.Now().UTC(), id, namespace)
	if err != nil {
		return fmt.Errorf("sql_storage: record schedule run failed: %w", err)
	}
	return nil
}
