-- Landing table for the Fabric Livy Sessions API.
-- Endpoint: GET /v1/workspaces/{workspaceId}/spark/livySessions/{sessionId}
-- Used to derive session_request_time -> session_ready_time (i.e. the session-provisioning
-- portion of bootstrap). state_log_json holds the full state-transition array because Fabric
-- has historically reshaped that payload.
CREATE TABLE [raw].[livy_session] (
    [session_id]             varchar(64)    NOT NULL,
    [job_instance_id]        varchar(36)        NULL,
    [workspace_id]           varchar(36)    NOT NULL,
    [name]                   varchar(256)       NULL,
    [state]                  varchar(32)        NULL,
    [created_at_utc]         datetime2(3)       NULL,
    [starting_at_utc]        datetime2(3)       NULL,
    [idle_at_utc]            datetime2(3)       NULL,
    [busy_at_utc]            datetime2(3)       NULL,
    [terminated_at_utc]      datetime2(3)       NULL,
    [pool_id]                varchar(36)        NULL,
    [pool_name]              varchar(256)       NULL,
    [runtime_version]        varchar(64)        NULL,
    [state_log_json]         varchar(max)       NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
