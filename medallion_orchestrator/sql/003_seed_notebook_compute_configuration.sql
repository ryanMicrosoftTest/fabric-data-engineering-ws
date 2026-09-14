SET NOCOUNT ON;
SET XACT_ABORT ON;

MERGE dbo.notebook_compute_configuration AS target
USING
(
    VALUES
        (
            CONVERT(uniqueidentifier, 'd877f06a-d72a-469a-8c82-4ed918b0686b'),
            CONVERT(nvarchar(256), N'nb_bronze_ingest'),
            CONVERT(bit, 1),
            CONVERT(varchar(8), '56g'),
            CONVERT(int, 8),
            CONVERT(varchar(8), '56g'),
            CONVERT(int, 8),
            CONVERT(int, 2)
        ),
        (
            CONVERT(uniqueidentifier, 'a1168d27-aeb6-419b-b835-38ef62206504'),
            CONVERT(nvarchar(256), N'nb_silver_transform'),
            CONVERT(bit, 0),
            CONVERT(varchar(8), NULL),
            CONVERT(int, NULL),
            CONVERT(varchar(8), NULL),
            CONVERT(int, NULL),
            CONVERT(int, NULL)
        ),
        (
            CONVERT(uniqueidentifier, 'bd214a6e-26f2-40d2-912e-467e5724e2e9'),
            CONVERT(nvarchar(256), N'nb_gold_aggregate'),
            CONVERT(bit, 0),
            CONVERT(varchar(8), NULL),
            CONVERT(int, NULL),
            CONVERT(varchar(8), NULL),
            CONVERT(int, NULL),
            CONVERT(int, NULL)
        )
) AS source
(
    notebook_id,
    notebook_name,
    is_enabled,
    driver_memory,
    driver_cores,
    executor_memory,
    executor_cores,
    num_executors
)
ON target.notebook_id = source.notebook_id
WHEN MATCHED THEN
    UPDATE SET
        notebook_name = source.notebook_name,
        is_enabled = source.is_enabled,
        driver_memory = source.driver_memory,
        driver_cores = source.driver_cores,
        executor_memory = source.executor_memory,
        executor_cores = source.executor_cores,
        num_executors = source.num_executors,
        updated_at = SYSUTCDATETIME(),
        updated_by = SYSTEM_USER
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        notebook_id,
        notebook_name,
        is_enabled,
        driver_memory,
        driver_cores,
        executor_memory,
        executor_cores,
        num_executors,
        created_at,
        updated_at,
        updated_by
    )
    VALUES
    (
        source.notebook_id,
        source.notebook_name,
        source.is_enabled,
        source.driver_memory,
        source.driver_cores,
        source.executor_memory,
        source.executor_cores,
        source.num_executors,
        SYSUTCDATETIME(),
        SYSUTCDATETIME(),
        SYSTEM_USER
    );
GO
