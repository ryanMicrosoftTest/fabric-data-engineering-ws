-- Landing table for the Fabric Spark Application / Monitoring API.
-- Source: Spark application detail surfaced by Fabric Monitoring Hub (the "Total / Queued /
-- Running duration" panel on a notebook snapshot). Grain: one row per
-- (application_id, ingested_at_utc).
CREATE TABLE [raw].[spark_application] (
    [application_id]         varchar(128)   NOT NULL,
    [job_instance_id]        varchar(36)        NULL,
    [workspace_id]           varchar(36)    NOT NULL,
    [item_id]                varchar(36)        NULL,
    [livy_session_id]        varchar(64)        NULL,
    [submitted_time_utc]     datetime2(3)       NULL,
    [start_time_utc]         datetime2(3)       NULL,
    [end_time_utc]           datetime2(3)       NULL,
    [total_duration_ms]      bigint             NULL,
    [queued_duration_ms]     bigint             NULL,
    [running_duration_ms]    bigint             NULL,
    [state]                  varchar(32)        NULL,
    [final_status]           varchar(32)        NULL,
    [driver_cores]           int                NULL,
    [driver_memory]          varchar(32)        NULL,
    [executor_cores]         int                NULL,
    [executor_memory]        varchar(32)        NULL,
    [executor_count_min]     int                NULL,
    [executor_count_max]     int                NULL,
    [dynamic_allocation_enabled] bit            NULL,
    [runtime_version]        varchar(64)        NULL,
    [raw_payload]            varchar(max)       NULL,
    [ingested_at_utc]        datetime2(3)   NOT NULL,
    [collector_run_id]       varchar(36)    NOT NULL,
    [source_api_version]     varchar(32)        NULL
);
