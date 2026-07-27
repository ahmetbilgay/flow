-- schema.postgres.sql
-- Reference schema (matches auto-migrate in sql_storage.go). Tables use no prefix by default.

CREATE TABLE IF NOT EXISTS workflows (
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
CREATE INDEX IF NOT EXISTS idx_workflows_namespace ON workflows(namespace);

CREATE TABLE IF NOT EXISTS executions (
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
CREATE INDEX IF NOT EXISTS idx_executions_namespace_status ON executions(namespace, status);

CREATE TABLE IF NOT EXISTS execution_steps (
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
CREATE INDEX IF NOT EXISTS idx_steps_namespace_status_sch ON execution_steps(namespace, status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_steps_status_updated ON execution_steps(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_steps_execution_id ON execution_steps(execution_id);
CREATE INDEX IF NOT EXISTS idx_steps_waiting ON execution_steps(namespace, updated_at) WHERE status = 'WAITING';

CREATE TABLE IF NOT EXISTS compensation_journal (
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
CREATE INDEX IF NOT EXISTS idx_compensation_execution ON compensation_journal(execution_id);

CREATE TABLE IF NOT EXISTS schedules (
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
CREATE INDEX IF NOT EXISTS idx_schedules_ns_active ON schedules(namespace, is_active);
CREATE INDEX IF NOT EXISTS idx_schedules_active ON schedules(is_active);
