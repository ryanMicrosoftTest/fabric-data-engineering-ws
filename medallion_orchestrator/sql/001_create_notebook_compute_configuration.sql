SET NOCOUNT ON;
SET XACT_ABORT ON;

IF OBJECT_ID(N'dbo.notebook_compute_configuration', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.notebook_compute_configuration
    (
        notebook_id uniqueidentifier NOT NULL,
        notebook_name nvarchar(256) NOT NULL,
        is_enabled bit NOT NULL
            CONSTRAINT DF_notebook_compute_configuration_is_enabled DEFAULT (0),
        driver_memory varchar(8) NULL,
        driver_cores int NULL,
        executor_memory varchar(8) NULL,
        executor_cores int NULL,
        num_executors int NULL,
        created_at datetime2(7) NOT NULL
            CONSTRAINT DF_notebook_compute_configuration_created_at DEFAULT (SYSUTCDATETIME()),
        updated_at datetime2(7) NOT NULL
            CONSTRAINT DF_notebook_compute_configuration_updated_at DEFAULT (SYSUTCDATETIME()),
        updated_by nvarchar(256) NOT NULL
            CONSTRAINT DF_notebook_compute_configuration_updated_by DEFAULT (SYSTEM_USER),

        CONSTRAINT PK_notebook_compute_configuration
            PRIMARY KEY CLUSTERED (notebook_id),
        CONSTRAINT CK_notebook_compute_configuration_notebook_name
            CHECK (LEN(LTRIM(RTRIM(notebook_name))) > 0),
        CONSTRAINT CK_notebook_compute_configuration_driver_memory
            CHECK (driver_memory IS NULL OR driver_memory IN ('28g', '56g', '112g', '224g', '400g')),
        CONSTRAINT CK_notebook_compute_configuration_executor_memory
            CHECK (executor_memory IS NULL OR executor_memory IN ('28g', '56g', '112g', '224g', '400g')),
        CONSTRAINT CK_notebook_compute_configuration_driver_cores
            CHECK (driver_cores IS NULL OR driver_cores IN (4, 8, 16, 32, 64)),
        CONSTRAINT CK_notebook_compute_configuration_executor_cores
            CHECK (executor_cores IS NULL OR executor_cores IN (4, 8, 16, 32, 64)),
        CONSTRAINT CK_notebook_compute_configuration_num_executors
            CHECK (num_executors IS NULL OR num_executors >= 1),
        CONSTRAINT CK_notebook_compute_configuration_enabled_complete
            CHECK
            (
                is_enabled = 0
                OR
                (
                    driver_memory IS NOT NULL
                    AND driver_cores IS NOT NULL
                    AND executor_memory IS NOT NULL
                    AND executor_cores IS NOT NULL
                    AND num_executors IS NOT NULL
                )
            ),
        CONSTRAINT CK_notebook_compute_configuration_audit_time
            CHECK (updated_at >= created_at)
    );
END;
GO
