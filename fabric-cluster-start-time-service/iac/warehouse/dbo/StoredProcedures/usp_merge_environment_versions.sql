-- Merges raw.environment + raw.environment_library into dbo.environment_versions_dim.
-- Key: (environment_id, environment_published_version). Library list is collapsed into a
-- JSON string so the dimension stays one-row-per-version.
--
-- Parameter:
--   @collector_run_id - the run that just finished landing data; pass NULL to merge ALL
--                       raw rows.
CREATE PROCEDURE [dbo].[usp_merge_environment_versions]
    @collector_run_id varchar(36) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    WITH env AS (
        SELECT *
        FROM (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.[environment_id], e.[published_version]
                    ORDER BY e.[ingested_at_utc] DESC
                ) AS rn
            FROM [raw].[environment] AS e
            WHERE e.[published_version] IS NOT NULL
              AND (@collector_run_id IS NULL OR e.[collector_run_id] = @collector_run_id)
        ) AS x
        WHERE x.rn = 1
    ),
    libs AS (
        SELECT
            [environment_id],
            [environment_published_version],
            COUNT(*) AS library_count,
            -- STRING_AGG of a JSON-shaped fragment so we don't need a JSON aggregator.
            CONCAT(
                '[',
                STRING_AGG(
                    CONCAT(
                        '{"name":"', REPLACE([library_name], '"', '\"'), '",',
                        '"version":', CASE WHEN [library_version] IS NULL THEN 'null' ELSE CONCAT('"', REPLACE([library_version], '"', '\"'), '"') END, ',',
                        '"source":',  CASE WHEN [library_source]  IS NULL THEN 'null' ELSE CONCAT('"', REPLACE([library_source],  '"', '\"'), '"') END,
                        '}'
                    ),
                    ','
                ) WITHIN GROUP (ORDER BY [library_name]),
                ']'
            ) AS libraries_json
        FROM (
            SELECT *
            FROM (
                SELECT
                    l.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.[environment_id], l.[environment_published_version], l.[library_name]
                        ORDER BY l.[ingested_at_utc] DESC
                    ) AS rn
                FROM [raw].[environment_library] AS l
                WHERE (@collector_run_id IS NULL OR l.[collector_run_id] = @collector_run_id)
            ) AS y
            WHERE y.rn = 1
        ) AS dedup
        GROUP BY [environment_id], [environment_published_version]
    ),
    src AS (
        SELECT
            env.[environment_id],
            env.[published_version]               AS environment_published_version,
            env.[workspace_id],
            env.[display_name]                    AS environment_name,
            env.[runtime_version],
            libs.library_count,
            libs.libraries_json,
            env.[published_at_utc],
            env.[ingested_at_utc]                 AS observed_ingested_at_utc
        FROM env
        LEFT JOIN libs
               ON libs.[environment_id]                = env.[environment_id]
              AND libs.[environment_published_version] = env.[published_version]
    )
    MERGE [dbo].[environment_versions_dim] AS tgt
    USING src
       ON tgt.[environment_id]                = src.[environment_id]
      AND tgt.[environment_published_version] = src.environment_published_version
    WHEN MATCHED THEN UPDATE SET
        tgt.[workspace_id]      = src.[workspace_id],
        tgt.[environment_name]  = src.environment_name,
        tgt.[runtime_version]   = src.runtime_version,
        tgt.[library_count]     = src.library_count,
        tgt.[libraries_json]    = src.libraries_json,
        tgt.[published_at_utc]  = src.[published_at_utc],
        tgt.[last_seen_at_utc]  = src.observed_ingested_at_utc
    WHEN NOT MATCHED THEN INSERT (
        [environment_id], [environment_published_version], [workspace_id], [environment_name],
        [runtime_version], [library_count], [libraries_json], [published_at_utc],
        [first_seen_at_utc], [last_seen_at_utc]
    )
    VALUES (
        src.[environment_id], src.environment_published_version, src.[workspace_id], src.environment_name,
        src.runtime_version, src.library_count, src.libraries_json, src.[published_at_utc],
        src.observed_ingested_at_utc, src.observed_ingested_at_utc
    );
END;
