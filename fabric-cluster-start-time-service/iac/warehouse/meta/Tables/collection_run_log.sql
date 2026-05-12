-- One row per Function invocation. Lets you answer "did the collector run, when, and what
-- did it cost in row counts?" without reading App Insights.
CREATE TABLE [meta].[collection_run_log] (
    [collector_run_id]                  varchar(36)    NOT NULL,
    [started_at_utc]                    datetime2(3)   NOT NULL,
    [ended_at_utc]                      datetime2(3)       NULL,
    [status]                            varchar(32)        NULL, -- Success | Failed | Partial
    [trigger_type]                      varchar(32)        NULL, -- Timer | Http | Manual
    [workspaces_targeted]               int                NULL,
    [workspaces_processed]              int                NULL,
    [rows_landed_job_instance]          int                NULL,
    [rows_landed_spark_application]     int                NULL,
    [rows_landed_livy_session]          int                NULL,
    [rows_landed_livy_statement]        int                NULL,
    [rows_landed_notebook_definition]   int                NULL,
    [rows_landed_notebook_item]         int                NULL,
    [rows_landed_workspace]             int                NULL,
    [rows_landed_environment]           int                NULL,
    [rows_landed_environment_library]   int                NULL,
    [rows_landed_spark_pool]            int                NULL,
    [rows_merged_notebook_run_facts]    int                NULL,
    [rows_merged_environment_versions]  int                NULL,
    [error_summary]                     varchar(max)       NULL,
    [function_app_version]              varchar(64)        NULL
);
