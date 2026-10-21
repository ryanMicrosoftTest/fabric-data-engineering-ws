SET NOCOUNT ON;
SET XACT_ABORT ON;

MERGE dbo.notebook_compute_configuration AS target
USING
(
    VALUES
    (
        CONVERT(uniqueidentifier, 'ab554b94-8a42-406c-9c4b-13f12270206c'),
        CONVERT(nvarchar(256), N'nb_compute_configuration_probe'),
        CONVERT(bit, 1),
        CONVERT(varchar(8), '56g'),
        CONVERT(int, 8),
        CONVERT(varchar(8), '56g'),
        CONVERT(int, 8),
        CONVERT(int, 2)
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
