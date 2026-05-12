-- Landing table for the Fabric Spark Pools / Workspace Spark Settings API.
-- Endpoint: GET /v1/workspaces/{workspaceId}/spark/pools/{poolId} or workspace-level settings.
-- Pool config heavily influences cold-start time (auto-pause, node family, min/max nodes).
-- Grain: one row per (pool_id, observed_at_utc).
CREATE TABLE [raw].[spark_pool] (
    [pool_id]                    varchar(36)    NOT NULL,
    [workspace_id]               varchar(36)    NOT NULL,
    [name]                       varchar(256)       NULL,
    [pool_type]                  varchar(32)        NULL,
    [node_size]                  varchar(32)        NULL,
    [node_family]                varchar(32)        NULL,
    [min_nodes]                  int                NULL,
    [max_nodes]                  int                NULL,
    [dynamic_allocation_enabled] bit                NULL,
    [auto_pause_enabled]         bit                NULL,
    [auto_pause_minutes]         int                NULL,
    [runtime_version]            varchar(64)        NULL,
    [observed_at_utc]            datetime2(3)   NOT NULL,
    [raw_payload]                varchar(max)       NULL,
    [ingested_at_utc]            datetime2(3)   NOT NULL,
    [collector_run_id]           varchar(36)    NOT NULL,
    [source_api_version]         varchar(32)        NULL
);
