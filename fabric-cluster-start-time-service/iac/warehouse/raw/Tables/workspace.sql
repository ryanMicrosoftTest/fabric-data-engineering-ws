-- Landing table for the Fabric Workspaces API.
-- Endpoint: GET /v1/workspaces/{workspaceId}
-- Provides workspace display_name and the capacity binding used to resolve capacity_sku.
-- Grain: one row per (workspace_id, observed_at_utc).
CREATE TABLE [raw].[workspace] (
    [workspace_id]           varchar(36)    NOT NULL,
    [display_name]           varchar(256)       NULL,
    [capacity_id]            varchar(36)        NULL,
    [capacity_sku]           varchar(32)        NULL,
    [capacity_region]        varchar(32)        NULL,
    [capacity_state]         varchar(32)        NULL,
    [observed_at_utc]        datetime2(3)   NOT NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
