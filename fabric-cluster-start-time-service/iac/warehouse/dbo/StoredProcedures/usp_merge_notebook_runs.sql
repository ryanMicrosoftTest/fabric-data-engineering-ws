-- Merges raw.job_instance + raw.spark_application + raw.livy_session + raw.livy_statement
-- + raw.notebook_item + raw.workspace + raw.notebook_definition + raw.spark_pool into
-- dbo.notebook_run_facts.
--
-- Strategy: build a CTE of "latest landed row per natural key" across each raw source for the
-- collector_run_id being merged, compute the derived bootstrap timings, then MERGE into the
-- fact on run_id. Fabric Warehouse executes MERGE serially per target table — fine for a
-- single Function writer.
--
-- Parameter:
--   @collector_run_id - the run that just finished landing data; pass NULL to merge ALL raw
--                       rows not already represented in the fact (full backfill mode).
CREATE PROCEDURE [dbo].[usp_merge_notebook_runs]
    @collector_run_id varchar(36) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Latest job_instance row per job_instance_id within this collector run (or all rows
    --    if @collector_run_id is NULL).
    WITH ji AS (
        SELECT *
        FROM (
            SELECT
                j.*,
                ROW_NUMBER() OVER (
                    PARTITION BY j.[job_instance_id]
                    ORDER BY j.[ingested_at_utc] DESC
                ) AS rn
            FROM [raw].[job_instance] AS j
            WHERE (@collector_run_id IS NULL OR j.[collector_run_id] = @collector_run_id)
        ) AS x
        WHERE x.rn = 1
    ),
    -- 2. Latest spark_application row per job_instance_id.
    sa AS (
        SELECT *
        FROM (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.[job_instance_id]
                    ORDER BY a.[ingested_at_utc] DESC
                ) AS rn
            FROM [raw].[spark_application] AS a
            WHERE a.[job_instance_id] IS NOT NULL
              AND (@collector_run_id IS NULL OR a.[collector_run_id] = @collector_run_id)
        ) AS x
        WHERE x.rn = 1
    ),
    -- 3. Latest livy_session row per session_id (we'll join via job_instance.livy_session_id).
    ls AS (
        SELECT *
        FROM (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY s.[session_id]
                    ORDER BY s.[ingested_at_utc] DESC
                ) AS rn
            FROM [raw].[livy_session] AS s
            WHERE (@collector_run_id IS NULL OR s.[collector_run_id] = @collector_run_id)
        ) AS x
        WHERE x.rn = 1
    ),
    -- 4. First statement dispatch per session: MIN(started_at_utc) across livy_statement.
    fs AS (
        SELECT
            [session_id],
            MIN([started_at_utc]) AS first_statement_started_at_utc
        FROM [raw].[livy_statement]
        WHERE [started_at_utc] IS NOT NULL
          AND (@collector_run_id IS NULL OR [collector_run_id] = @collector_run_id)
        GROUP BY [session_id]
    ),
    -- 5. Notebook display_name (latest item snapshot per notebook_id).
    ni AS (
        SELECT *
        FROM (
            SELECT
                n.[notebook_id],
                n.[display_name],
                ROW_NUMBER() OVER (
                    PARTITION BY n.[notebook_id]
                    ORDER BY n.[observed_at_utc] DESC
                ) AS rn
            FROM [raw].[notebook_item] AS n
        ) AS x
        WHERE x.rn = 1
    ),
    -- 6. Notebook definition (env + pool + lakehouse attachments) latest per notebook_id.
    nd AS (
        SELECT *
        FROM (
            SELECT
                d.*,
                ROW_NUMBER() OVER (
                    PARTITION BY d.[notebook_id]
                    ORDER BY d.[observed_at_utc] DESC
                ) AS rn
            FROM [raw].[notebook_definition] AS d
        ) AS x
        WHERE x.rn = 1
    ),
    -- 7. Workspace display_name + capacity latest per workspace_id.
    ws AS (
        SELECT *
        FROM (
            SELECT
                w.*,
                ROW_NUMBER() OVER (
                    PARTITION BY w.[workspace_id]
                    ORDER BY w.[observed_at_utc] DESC
                ) AS rn
            FROM [raw].[workspace] AS w
        ) AS x
        WHERE x.rn = 1
    ),
    -- 8. Spark pool config latest per pool_id.
    sp AS (
        SELECT *
        FROM (
            SELECT
                p.*,
                ROW_NUMBER() OVER (
                    PARTITION BY p.[pool_id]
                    ORDER BY p.[observed_at_utc] DESC
                ) AS rn
            FROM [raw].[spark_pool] AS p
        ) AS x
        WHERE x.rn = 1
    ),
    src AS (
        SELECT
            ji.[job_instance_id]                                       AS run_id,
            ji.[item_id]                                               AS notebook_id,
            ji.[workspace_id],
            ni.[display_name]                                          AS notebook_name,
            ws.[display_name]                                          AS workspace_name,
            ji.[root_activity_name]                                    AS activity_name,

            ji.[start_time_utc]                                        AS job_start_utc,
            ji.[end_time_utc]                                          AS job_end_utc,
            ji.[duration_seconds]                                      AS job_duration_seconds,

            sa.[submitted_time_utc]                                    AS spark_submit_utc,
            CAST(sa.[total_duration_ms]   / 1000.0 AS decimal(18,3))   AS spark_total_duration_seconds,
            CAST(sa.[queued_duration_ms]  / 1000.0 AS decimal(18,3))   AS spark_queued_duration_seconds,
            CAST(sa.[running_duration_ms] / 1000.0 AS decimal(18,3))   AS spark_running_duration_seconds,

            ji.[livy_session_id],
            ls.[created_at_utc]                                        AS session_request_time_utc,
            ls.[idle_at_utc]                                           AS session_ready_time_utc,

            nd.[environment_id],
            NULL                                                       AS environment_published_version, -- set in a second pass once env table is populated
            sp.[pool_id],
            sp.[name]                                                  AS pool_name,
            sp.[pool_type],
            sp.[node_size],

            nd.[default_lakehouse_id],
            nd.[attached_lakehouses_json]                              AS attached_lakehouse_ids,
            nd.[attached_lakehouse_count],

            -- Sub-bootstrap timings
            CAST(DATEDIFF_BIG(MILLISECOND, ls.[created_at_utc], ls.[idle_at_utc]) / 1000.0 AS decimal(18,3))
                                                                       AS t_session_provision_seconds,
            -- env install ~ busy - idle (Fabric reports env install as a state transition after idle)
            CAST(DATEDIFF_BIG(MILLISECOND, ls.[idle_at_utc], ls.[busy_at_utc]) / 1000.0 AS decimal(18,3))
                                                                       AS t_environment_install_seconds,
            CAST(DATEDIFF_BIG(MILLISECOND, ls.[idle_at_utc], fs.first_statement_started_at_utc) / 1000.0 AS decimal(18,3))
                                                                       AS t_first_statement_dispatch_seconds,

            -- Headline bootstrap
            CAST(ji.[duration_seconds] - (sa.[total_duration_ms] / 1000.0) AS decimal(18,3))
                                                                       AS bootstrap_seconds,
            CASE
                WHEN ji.[duration_seconds] IS NULL OR ji.[duration_seconds] = 0 THEN NULL
                ELSE CAST(
                    (ji.[duration_seconds] - (sa.[total_duration_ms] / 1000.0)) / ji.[duration_seconds]
                    AS decimal(9,6))
            END                                                        AS bootstrap_pct,

            ji.[pipeline_run_id],
            NULL                                                       AS pipeline_id,   -- enriched separately if needed
            NULL                                                       AS pipeline_name,
            ji.[invoke_type]                                           AS invocation_type,

            ws.[capacity_id],
            ws.[capacity_sku],

            ji.[status],
            ji.[error_code],
            ji.[error_message],
            ji.[submitter_upn]                                         AS submitter,

            sa.[executor_count_min],
            sa.[executor_count_max],
            sa.[dynamic_allocation_enabled],
            COALESCE(sa.[runtime_version], ls.[runtime_version])       AS runtime_version,

            ji.[ingested_at_utc],
            ji.[source_api_version]
        FROM ji
        LEFT JOIN sa ON sa.[job_instance_id] = ji.[job_instance_id]
        LEFT JOIN ls ON ls.[session_id]      = ji.[livy_session_id]
        LEFT JOIN fs ON fs.[session_id]      = ji.[livy_session_id]
        LEFT JOIN ni ON ni.[notebook_id]     = ji.[item_id]
        LEFT JOIN nd ON nd.[notebook_id]     = ji.[item_id]
        LEFT JOIN ws ON ws.[workspace_id]    = ji.[workspace_id]
        LEFT JOIN sp ON sp.[pool_id]         = nd.[spark_pool_id]
        WHERE ji.[status] IN ('Completed','Failed','Cancelled','Deduped','NotStarted','InProgress')
    )
    MERGE [dbo].[notebook_run_facts] AS tgt
    USING src
       ON tgt.[run_id] = src.run_id
    WHEN MATCHED THEN UPDATE SET
        tgt.[notebook_id]                          = src.notebook_id,
        tgt.[workspace_id]                         = src.workspace_id,
        tgt.[notebook_name]                        = src.notebook_name,
        tgt.[workspace_name]                       = src.workspace_name,
        tgt.[activity_name]                        = src.activity_name,
        tgt.[job_start_utc]                        = src.job_start_utc,
        tgt.[job_end_utc]                          = src.job_end_utc,
        tgt.[job_duration_seconds]                 = src.job_duration_seconds,
        tgt.[spark_submit_utc]                     = src.spark_submit_utc,
        tgt.[spark_total_duration_seconds]         = src.spark_total_duration_seconds,
        tgt.[spark_queued_duration_seconds]        = src.spark_queued_duration_seconds,
        tgt.[spark_running_duration_seconds]       = src.spark_running_duration_seconds,
        tgt.[livy_session_id]                      = src.livy_session_id,
        tgt.[session_request_time_utc]             = src.session_request_time_utc,
        tgt.[session_ready_time_utc]               = src.session_ready_time_utc,
        tgt.[environment_id]                       = src.environment_id,
        tgt.[environment_published_version]        = src.environment_published_version,
        tgt.[pool_id]                              = src.pool_id,
        tgt.[pool_name]                            = src.pool_name,
        tgt.[pool_type]                            = src.pool_type,
        tgt.[node_size]                            = src.node_size,
        tgt.[default_lakehouse_id]                 = src.default_lakehouse_id,
        tgt.[attached_lakehouse_ids]               = src.attached_lakehouse_ids,
        tgt.[attached_lakehouse_count]             = src.attached_lakehouse_count,
        tgt.[t_session_provision_seconds]          = src.t_session_provision_seconds,
        tgt.[t_environment_install_seconds]        = src.t_environment_install_seconds,
        tgt.[t_first_statement_dispatch_seconds]   = src.t_first_statement_dispatch_seconds,
        tgt.[bootstrap_seconds]                    = src.bootstrap_seconds,
        tgt.[bootstrap_pct]                        = src.bootstrap_pct,
        tgt.[pipeline_run_id]                      = src.pipeline_run_id,
        tgt.[pipeline_id]                          = src.pipeline_id,
        tgt.[pipeline_name]                        = src.pipeline_name,
        tgt.[invocation_type]                      = src.invocation_type,
        tgt.[capacity_id]                          = src.capacity_id,
        tgt.[capacity_sku]                         = src.capacity_sku,
        tgt.[status]                               = src.status,
        tgt.[error_code]                           = src.error_code,
        tgt.[error_message]                        = src.error_message,
        tgt.[submitter]                            = src.submitter,
        tgt.[executor_count_min]                   = src.executor_count_min,
        tgt.[executor_count_max]                   = src.executor_count_max,
        tgt.[dynamic_allocation_enabled]           = src.dynamic_allocation_enabled,
        tgt.[runtime_version]                      = src.runtime_version,
        tgt.[ingested_at_utc]                      = src.ingested_at_utc,
        tgt.[source_api_version]                   = src.source_api_version
    WHEN NOT MATCHED THEN INSERT (
        [run_id], [notebook_id], [workspace_id], [notebook_name], [workspace_name], [activity_name],
        [job_start_utc], [job_end_utc], [job_duration_seconds],
        [spark_submit_utc], [spark_total_duration_seconds], [spark_queued_duration_seconds], [spark_running_duration_seconds],
        [livy_session_id], [session_request_time_utc], [session_ready_time_utc],
        [environment_id], [environment_published_version],
        [pool_id], [pool_name], [pool_type], [node_size],
        [default_lakehouse_id], [attached_lakehouse_ids], [attached_lakehouse_count],
        [t_session_provision_seconds], [t_environment_install_seconds], [t_first_statement_dispatch_seconds],
        [bootstrap_seconds], [bootstrap_pct],
        [pipeline_run_id], [pipeline_id], [pipeline_name], [invocation_type],
        [capacity_id], [capacity_sku],
        [status], [error_code], [error_message], [submitter],
        [executor_count_min], [executor_count_max], [dynamic_allocation_enabled], [runtime_version],
        [ingested_at_utc], [source_api_version]
    )
    VALUES (
        src.run_id, src.notebook_id, src.workspace_id, src.notebook_name, src.workspace_name, src.activity_name,
        src.job_start_utc, src.job_end_utc, src.job_duration_seconds,
        src.spark_submit_utc, src.spark_total_duration_seconds, src.spark_queued_duration_seconds, src.spark_running_duration_seconds,
        src.livy_session_id, src.session_request_time_utc, src.session_ready_time_utc,
        src.environment_id, src.environment_published_version,
        src.pool_id, src.pool_name, src.pool_type, src.node_size,
        src.default_lakehouse_id, src.attached_lakehouse_ids, src.attached_lakehouse_count,
        src.t_session_provision_seconds, src.t_environment_install_seconds, src.t_first_statement_dispatch_seconds,
        src.bootstrap_seconds, src.bootstrap_pct,
        src.pipeline_run_id, src.pipeline_id, src.pipeline_name, src.invocation_type,
        src.capacity_id, src.capacity_sku,
        src.status, src.error_code, src.error_message, src.submitter,
        src.executor_count_min, src.executor_count_max, src.dynamic_allocation_enabled, src.runtime_version,
        src.ingested_at_utc, src.source_api_version
    );
END;
