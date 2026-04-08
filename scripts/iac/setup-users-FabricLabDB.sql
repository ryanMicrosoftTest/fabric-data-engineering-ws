
-- ============================================================
-- Azure SQL Database Setup Script - FabricLabDB Database
-- Creates database users for FabricLabDB
-- Run this script directly against the FabricLabDB database
-- ============================================================

-- Create database users and assign read permissions
DECLARE @Counter INT = 1;
DECLARE @LoginName NVARCHAR(50);
DECLARE @SQL NVARCHAR(MAX);

PRINT '';
PRINT 'Creating database users and assigning read permissions...';
PRINT '========================================================';

WHILE @Counter <= 60
BEGIN
    SET @LoginName = 'reader' + RIGHT('00' + CAST(@Counter AS VARCHAR(2)), 2);
    
    -- Check if user already exists
    IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = @LoginName)
    BEGIN
        SET @SQL = 'CREATE USER [' + @LoginName + '] FOR LOGIN [' + @LoginName + '];';
        EXEC sp_executesql @SQL;
        
        -- Add user to db_datareader role
        SET @SQL = 'ALTER ROLE db_datareader ADD MEMBER [' + @LoginName + '];';
        EXEC sp_executesql @SQL;
        
        PRINT 'Created user and granted read permissions: ' + @LoginName;
    END
    ELSE
    BEGIN
        PRINT 'User already exists: ' + @LoginName;
    END
    
    SET @Counter = @Counter + 1;
END

PRINT '';
PRINT 'FabricLabDB database setup completed successfully!';
PRINT '';
PRINT 'Summary:';
PRINT '- Database: FabricLabDB';
PRINT '- Users created: reader01 through reader60';
PRINT '- Permissions: Read-only access to all tables';
PRINT '';
PRINT 'Test connection with any user:';
PRINT 'Server: your-sql-server.domain.com';
PRINT 'Database: FabricLabDB';
PRINT 'Username: reader01 (or any reader01-reader60)';
PRINT 'Password: Read<NN> + configured suffix (see iac.config)';
PRINT '';
PRINT 'Test query: SELECT * FROM Sales1;';