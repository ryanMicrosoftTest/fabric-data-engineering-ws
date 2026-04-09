-- ============================================================
-- Azure SQL Database Setup Script - LabDB Database
-- Creates sample table and database users for labdb
-- Run this script against the labdb database
-- ============================================================

-- Create a sample table for testing
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SampleData')
BEGIN
    CREATE TABLE [dbo].[SampleData] (
        [Id] INT IDENTITY(1,1) PRIMARY KEY,
        [Name] NVARCHAR(100) NOT NULL,
        [Description] NVARCHAR(500),
        [CreatedDate] DATETIME2 DEFAULT GETUTCDATE(),
        [IsActive] BIT DEFAULT 1
    );
    
    -- Insert sample data
    INSERT INTO [dbo].[SampleData] ([Name], [Description])
    VALUES 
        ('Sample Record 1', 'This is a test record for demonstration'),
        ('Sample Record 2', 'Another test record with different data'),
        ('Sample Record 3', 'Third sample record for query testing'),
        ('Sample Record 4', 'Fourth record to ensure multiple rows'),
        ('Sample Record 5', 'Final sample record for initial dataset');
        
    PRINT 'Sample table and data created successfully.';
END
ELSE
BEGIN
    PRINT 'Sample table already exists.';
END

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
PRINT 'LabDB database setup completed successfully!';
PRINT '';
PRINT 'Summary:';
PRINT '- Database: labdb';
PRINT '- Users created: reader01 through reader60';
PRINT '- Permissions: Read-only access to all tables';
PRINT '- Sample table: SampleData (with 5 test records)';
PRINT '';
PRINT 'Test connection with any user:';
PRINT 'Server: <your-server>.database.windows.net';
PRINT 'Database: labdb';
PRINT 'Username: reader01 (or any reader01-reader60)';
PRINT 'Password: Read<NN> + configured suffix (see iac.config)';
PRINT '';
PRINT 'Test query: SELECT * FROM SampleData;';