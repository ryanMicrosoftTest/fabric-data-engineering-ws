-- Landing table for the Fabric Items API filtered to notebooks.
-- Endpoint: GET /v1/workspaces/{workspaceId}/items?type=Notebook
-- Provides notebook display_name and description (the definition API does not).
-- Grain: one row per (notebook_id, observed_at_utc).
CREATE TABLE [raw].[notebook_item] (
    [notebook_id]            varchar(36)    NOT NULL,
    [workspace_id]           varchar(36)    NOT NULL,
    [display_name]           varchar(256)       NULL,
    [description]            varchar(max)       NULL,
    [observed_at_utc]        datetime2(3)   NOT NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
