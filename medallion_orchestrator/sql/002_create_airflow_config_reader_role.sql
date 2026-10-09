SET NOCOUNT ON;
SET XACT_ABORT ON;

IF DATABASE_PRINCIPAL_ID(N'airflow_config_reader') IS NULL
BEGIN
    CREATE ROLE airflow_config_reader AUTHORIZATION dbo;
END;

GRANT SELECT ON OBJECT::dbo.notebook_compute_configuration TO airflow_config_reader;
GO
