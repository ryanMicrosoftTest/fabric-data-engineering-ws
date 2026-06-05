-- Landing table for the Fabric Notebook Definition API (notebook-content.py + metadata).
-- Endpoint: POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition
-- Parsed metadata fields are pulled from the dependencies block of notebook-content.py.
-- Grain: one row per (notebook_id, observed_at_utc). We re-snapshot on each collection so
-- that attachment / environment / pool changes over time are visible.
CREATE TABLE [raw].[notebook_definition] (
    [notebook_id]                       varchar(36)    NOT NULL,
    [workspace_id]                      varchar(36)    NOT NULL,
    [default_lakehouse_id]              varchar(36)        NULL,
    [default_lakehouse_workspace_id]    varchar(36)        NULL,
    [attached_lakehouses_json]          varchar(max)       NULL,
    [attached_lakehouse_count]          int                NULL,
    [environment_id]                    varchar(36)        NULL,
    [environment_workspace_id]          varchar(36)        NULL,
    [spark_pool_id]                     varchar(36)        NULL,
    [spark_pool_name]                   varchar(256)       NULL,
    [observed_at_utc]                   datetime2(3)   NOT NULL,
    [raw_payload]                       varchar(max)       NULL,
    [ingested_at_utc]                   datetime2(3)   NOT NULL,
    [collector_run_id]                  varchar(36)    NOT NULL,
    [source_api_version]                varchar(32)        NULL
);
