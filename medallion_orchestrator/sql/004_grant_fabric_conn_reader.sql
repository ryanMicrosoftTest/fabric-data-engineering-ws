/*
Run only after safely resolving the existing fabric_conn service principal's
application (client) ID. This script needs no client secret.

Example:
  sqlcmd -S "<server>" -d "airflow_config" -G ^
    -v FabricConnClientId="<application-client-id>" ^
    -i "004_grant_fabric_conn_reader.sql"
*/

:on error exit

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @principal_name sysname = N'fabric_conn';
DECLARE @client_id uniqueidentifier = '$(FabricConnClientId)';

IF DATABASE_PRINCIPAL_ID(@principal_name) IS NULL
BEGIN
    DECLARE @sid_literal varchar(34) =
        CONVERT(varchar(34), CONVERT(varbinary(16), @client_id), 1);
    DECLARE @create_user nvarchar(max) =
        N'CREATE USER ' + QUOTENAME(@principal_name)
        + N' WITH SID = ' + @sid_literal + N', TYPE = E;';

    EXEC sys.sp_executesql @create_user;
END;

IF IS_ROLEMEMBER(N'airflow_config_reader', @principal_name) <> 1
BEGIN
    ALTER ROLE airflow_config_reader ADD MEMBER fabric_conn;
END;
GO
