-- schema.mysql.sql
-- Reference schema (matches auto-migrate in sql_storage.go). Tables use no prefix by default.

CREATE TABLE IF NOT EXISTS workflows (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS executions (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS execution_steps (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_compensation_execution (execution_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS schedules (
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
