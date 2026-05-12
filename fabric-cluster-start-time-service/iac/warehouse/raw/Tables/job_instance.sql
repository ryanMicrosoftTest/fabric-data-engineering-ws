-- Landing table for the Fabric Job Instance API.
-- Endpoint: GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}
-- Grain: one row per (job_instance_id, ingested_at_utc). A job instance may be re-fetched if
-- it was still running on a prior collection sweep, so duplicates by job_instance_id are expected.
CREATE TABLE [raw].[job_instance] (
    [job_instance_id]        varchar(36)    NOT NULL,
    [workspace_id]           varchar(36)    NOT NULL,
    [item_id]                varchar(36)    NOT NULL,
    [item_type]              varchar(64)        NULL,
    [job_type]               varchar(64)        NULL,
    [invoke_type]            varchar(32)        NULL,
    [root_activity_id]       varchar(36)        NULL,
    [root_activity_name]     varchar(256)       NULL,
    [pipeline_run_id]        varchar(36)        NULL,
    [start_time_utc]         datetime2(3)       NULL,
    [end_time_utc]           datetime2(3)       NULL,
    [duration_seconds]       decimal(18,3)      NULL,
    [status]                 varchar(32)        NULL,
    [error_code]             varchar(128)       NULL,
    [error_message]          varchar(max)       NULL,
    [submitter_upn]          varchar(256)       NULL,
    [livy_session_id]        varchar(64)        NULL,
    [spark_application_id]   varchar(128)       NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
