-- Landing table for the Fabric Livy Statements API.
-- Endpoint: GET /v1/workspaces/{workspaceId}/spark/livySessions/{sessionId}/statements
-- Grain: one row per (session_id, statement_id, ingested_at_utc).
-- Used to derive t_first_statement_dispatch_seconds = MIN(started_at_utc) - session_ready_time.
CREATE TABLE [raw].[livy_statement] (
    [session_id]             varchar(64)    NOT NULL,
    [statement_id]           int            NOT NULL,
    [workspace_id]           varchar(36)    NOT NULL,
    [job_instance_id]        varchar(36)        NULL,
    [code_excerpt]           varchar(8000)      NULL,
    [state]                  varchar(32)        NULL,
    [created_at_utc]         datetime2(3)       NULL,
    [started_at_utc]         datetime2(3)       NULL,
    [completed_at_utc]       datetime2(3)       NULL,
    [output_status]          varchar(32)        NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
