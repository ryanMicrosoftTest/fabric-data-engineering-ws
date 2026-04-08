-- ============================================================
-- Azure SQL Database Setup Script - Master Database
-- Creates 25 SQL Server logins on master database
-- Run this script against the master database
--
-- Usage: sqlcmd -S <server> -d master -U <admin> -P <pw> \
--        -v ReaderPasswordSuffix="<suffix>" -i setup-database.sql
-- ============================================================

-- SQLCMD variable for password suffix (pass via -v ReaderPasswordSuffix="...")
-- e.g. -v ReaderPasswordSuffix="_Pass123!"
:setvar ReaderPasswordSuffix "_DefaultSuffix!"

-- Create 25 SQL Server logins with generated passwords
DECLARE @Counter INT = 1;
DECLARE @LoginName NVARCHAR(50);
DECLARE @Password NVARCHAR(50);
DECLARE @SQL NVARCHAR(MAX);

PRINT 'Creating 25 SQL Server logins...';
PRINT '==========================================';

WHILE @Counter <= 60
BEGIN
    SET @LoginName = 'reader' + RIGHT('00' + CAST(@Counter AS VARCHAR(2)), 2);
    SET @Password = 'Read' + RIGHT('00' + CAST(@Counter AS VARCHAR(2)), 2) + '$(ReaderPasswordSuffix)';
    
    -- Check if login already exists
    IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = @LoginName)
    BEGIN
        SET @SQL = 'CREATE LOGIN [' + @LoginName + '] WITH PASSWORD = ''' + @Password + ''';';
        EXEC sp_executesql @SQL;
        PRINT 'Created login: ' + @LoginName;
    END
    ELSE
    BEGIN
        PRINT 'Login already exists: ' + @LoginName;
    END
    
    SET @Counter = @Counter + 1;
END

PRINT '';
PRINT 'Master database setup completed!';
PRINT '';
PRINT 'Summary:';
PRINT '- Created 60 SQL Server logins (reader01 through reader60)';
PRINT '- Passwords: Read<NN> + configured suffix (see iac.config)';
PRINT '';
PRINT 'Next Steps:';
PRINT '1. Run setup-database-labdb.sql against the labdb database';
PRINT '2. Or run this command:';
PRINT '   sqlcmd -S <server>.database.windows.net -d labdb -U <admin> -P <password> -i setup-database-labdb.sql';
PRINT '';