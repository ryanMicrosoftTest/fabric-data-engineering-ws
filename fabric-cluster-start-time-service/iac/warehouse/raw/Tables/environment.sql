-- Landing table for the Fabric Environments API.
-- Endpoint: GET /v1/workspaces/{workspaceId}/environments/{environmentId}
-- Provides published_version (the integer that ticks every time someone publishes the env).
-- Grain: one row per (environment_id, observed_at_utc).
CREATE TABLE [raw].[environment] (
    [environment_id]         varchar(36)    NOT NULL,
    [workspace_id]           varchar(36)    NOT NULL,
    [display_name]           varchar(256)       NULL,
    [description]            varchar(max)       NULL,
    [published_version]      int                NULL,
    [staging_version]        int                NULL,
    [published_at_utc]       datetime2(3)       NULL,
    [runtime_version]        varchar(64)        NULL,
    [observed_at_utc]        datetime2(3)   NOT NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
