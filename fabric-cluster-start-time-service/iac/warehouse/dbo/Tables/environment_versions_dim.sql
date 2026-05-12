-- Curated dimension — one row per published version of an environment.
-- Populated by [dbo].[usp_merge_environment_versions]. We snapshot library list + count per
-- published_version so that runs can be joined back to the exact env state they used, without
-- bloating notebook_run_facts with library arrays.
CREATE TABLE [dbo].[environment_versions_dim] (
    [environment_id]                 varchar(36)    NOT NULL,
    [environment_published_version]  int            NOT NULL,
    [workspace_id]                   varchar(36)        NULL,
    [environment_name]               varchar(256)       NULL,
    [runtime_version]                varchar(64)        NULL,
    [library_count]                  int                NULL,
    [libraries_json]                 varchar(max)       NULL,
    [published_at_utc]               datetime2(3)       NULL,
    [first_seen_at_utc]              datetime2(3)   NOT NULL,
    [last_seen_at_utc]               datetime2(3)   NOT NULL
);
