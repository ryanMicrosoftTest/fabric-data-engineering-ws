-- Landing table for the Fabric Environment Libraries API.
-- Endpoint: GET /v1/workspaces/{workspaceId}/environments/{environmentId}/libraries
-- Each library row is captured against the env's currently-published_version.
-- Grain: one row per (environment_id, environment_published_version, library_name, ingested_at_utc).
CREATE TABLE [raw].[environment_library] (
    [environment_id]                 varchar(36)    NOT NULL,
    [environment_published_version]  int            NOT NULL,
    [library_name]                   varchar(256)   NOT NULL,
    [library_version]                varchar(64)        NULL,
    [library_source]                 varchar(32)        NULL,
    [library_state]                  varchar(32)        NULL,
    [raw_payload]                    varchar(max)       NULL,
    [ingested_at_utc]                datetime2(3)   NOT NULL,
    [collector_run_id]               varchar(36)    NOT NULL,
    [source_api_version]             varchar(32)        NULL
);
