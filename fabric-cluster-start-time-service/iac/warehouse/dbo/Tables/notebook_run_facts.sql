-- Curated fact table — one row per notebook run.
-- Populated by [dbo].[usp_merge_notebook_runs] from raw.job_instance + raw.spark_application
-- + raw.livy_session + raw.livy_statement, joined with raw.notebook_definition / item /
-- workspace / spark_pool snapshots in effect at run time. This is the primary table Power BI
-- consumes via Direct Lake.
CREATE TABLE [dbo].[notebook_run_facts] (
    -- Identity
    [run_id]                                 varchar(36)    NOT NULL,
    [notebook_id]                            varchar(36)    NOT NULL,
    [workspace_id]                           varchar(36)    NOT NULL,
    [notebook_name]                          varchar(256)       NULL,
    [workspace_name]                         varchar(256)       NULL,
    [activity_name]                          varchar(256)       NULL,

    -- Job timing (from Job Instance API)
    [job_start_utc]                          datetime2(3)       NULL,
    [job_end_utc]                            datetime2(3)       NULL,
    [job_duration_seconds]                   decimal(18,3)      NULL,

    -- Spark application timing (from Spark Application API)
    [spark_submit_utc]                       datetime2(3)       NULL,
    [spark_total_duration_seconds]           decimal(18,3)      NULL,
    [spark_queued_duration_seconds]          decimal(18,3)      NULL,
    [spark_running_duration_seconds]         decimal(18,3)      NULL,

    -- Livy session timing (from Livy Sessions API)
    [livy_session_id]                        varchar(64)        NULL,
    [session_request_time_utc]               datetime2(3)       NULL,
    [session_ready_time_utc]                 datetime2(3)       NULL,

    -- Environment & pool snapshot in effect at run time
    [environment_id]                         varchar(36)        NULL,
    [environment_published_version]          int                NULL,
    [pool_id]                                varchar(36)        NULL,
    [pool_name]                              varchar(256)       NULL,
    [pool_type]                              varchar(32)        NULL,
    [node_size]                              varchar(32)        NULL,

    -- Attached storage
    [default_lakehouse_id]                   varchar(36)        NULL,
    [attached_lakehouse_ids]                 varchar(max)       NULL,
    [attached_lakehouse_count]               int                NULL,

    -- Derived sub-bootstrap timings (seconds)
    [t_session_provision_seconds]            decimal(18,3)      NULL,
    [t_environment_install_seconds]          decimal(18,3)      NULL,
    [t_first_statement_dispatch_seconds]     decimal(18,3)      NULL,

    -- The headline metric: hidden bootstrap window (job duration minus Spark total)
    [bootstrap_seconds]                      decimal(18,3)      NULL,
    [bootstrap_pct]                          decimal(9,6)       NULL,

    -- Pipeline correlation
    [pipeline_run_id]                        varchar(36)        NULL,
    [pipeline_id]                            varchar(36)        NULL,
    [pipeline_name]                          varchar(256)       NULL,
    [invocation_type]                        varchar(32)        NULL,

    -- Capacity context
    [capacity_id]                            varchar(36)        NULL,
    [capacity_sku]                           varchar(32)        NULL,

    -- Outcome
    [status]                                 varchar(32)        NULL,
    [error_code]                             varchar(128)       NULL,
    [error_message]                          varchar(max)       NULL,
    [submitter]                              varchar(256)       NULL,

    -- Executor config (drives cold-start cost)
    [executor_count_min]                     int                NULL,
    [executor_count_max]                     int                NULL,
    [dynamic_allocation_enabled]             bit                NULL,
    [runtime_version]                        varchar(64)        NULL,

    -- Ingestion bookkeeping
    [ingested_at_utc]                        datetime2(3)   NOT NULL,
    [source_api_version]                     varchar(32)        NULL
);
