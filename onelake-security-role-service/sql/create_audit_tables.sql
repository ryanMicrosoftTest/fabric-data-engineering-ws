-- OneLake Security Role Service — Audit Table Setup
-- ====================================================
-- Run this script once against your Fabric SQL Database to create
-- the audit tables. The notebooks will only INSERT into these tables.
--
-- Prerequisites:
--   - The SPN used by the notebooks needs INSERT permission on these tables
--   - Run this as a database owner or admin

-- ============================================================
-- Role Creation Audit Table
-- ============================================================
-- Tracks every role definition operation (create, update, delete)

IF OBJECT_ID('dbo.role_creation_audit_tbl', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.role_creation_audit_tbl (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        correlation_id      NVARCHAR(100)       NOT NULL,   -- Links all operations from a single pipeline run
        operation           NVARCHAR(50)        NOT NULL,   -- ROLE_APPLIED, ROLE_DELETED, PARSE_ERROR
        role_name           NVARCHAR(255)       NOT NULL,   -- Role name from the YAML definition
        target_workspace_id NVARCHAR(100)       NOT NULL,   -- Workspace where the role was created/updated
        target_lakehouse_id NVARCHAR(100)       NOT NULL,   -- Lakehouse where the role was created/updated
        source_file         NVARCHAR(500)       NULL,       -- YAML file name (e.g., neurology-read.yml)
        content_hash        NVARCHAR(100)       NULL,       -- SHA-256 hash of the YAML content at time of processing
        success             BIT                 NOT NULL,   -- 1 = success, 0 = failure
        error               NVARCHAR(2000)      NULL,       -- Error message if success = 0
        [timestamp]         DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),
        
        -- Role definition details (denormalized for audit queryability)
        role_state          NVARCHAR(20)        NULL,       -- present | absent
        table_count         INT                 NULL,       -- Number of tables in the role definition
        has_rls             BIT                 NULL,       -- Does the role have row-level security?
        has_cls             BIT                 NULL        -- Does the role have column-level security?
    );

    -- Index for querying by correlation (pipeline run)
    CREATE NONCLUSTERED INDEX IX_role_creation_audit_correlation
        ON dbo.role_creation_audit_tbl (correlation_id);

    -- Index for querying by role name
    CREATE NONCLUSTERED INDEX IX_role_creation_audit_role_name
        ON dbo.role_creation_audit_tbl (role_name, [timestamp] DESC);

    -- Index for querying failures
    CREATE NONCLUSTERED INDEX IX_role_creation_audit_failures
        ON dbo.role_creation_audit_tbl (success) WHERE success = 0;

    PRINT 'Created dbo.role_creation_audit_tbl with indexes';
END
ELSE
    PRINT 'dbo.role_creation_audit_tbl already exists — skipping';
GO


-- ============================================================
-- Role Mapping Audit Table
-- ============================================================
-- Tracks every user-role mapping operation (members added/removed)

IF OBJECT_ID('dbo.role_mapping_audit_tbl', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.role_mapping_audit_tbl (
        id                  INT IDENTITY(1,1) PRIMARY KEY,
        correlation_id      NVARCHAR(100)       NOT NULL,   -- Links all operations from a single pipeline run
        operation           NVARCHAR(50)        NOT NULL,   -- MAPPING_APPLIED, PARSE_ERROR
        role_name           NVARCHAR(255)       NOT NULL,   -- Role name from the YAML mapping
        target_workspace_id NVARCHAR(100)       NOT NULL,   -- Workspace where the role lives
        target_lakehouse_id NVARCHAR(100)       NOT NULL,   -- Lakehouse where the role lives
        source_file         NVARCHAR(500)       NULL,       -- YAML file name (e.g., neurology-read-users.yml)
        content_hash        NVARCHAR(100)       NULL,       -- SHA-256 hash of the YAML content at time of processing
        success             BIT                 NOT NULL,   -- 1 = success, 0 = failure
        error               NVARCHAR(2000)      NULL,       -- Error message if success = 0
        [timestamp]         DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),
        
        -- Membership details (denormalized for audit queryability)
        member_count        INT                 NULL,       -- Total members in this mapping file
        user_count          INT                 NULL,       -- Count of User-type members
        group_count         INT                 NULL,       -- Count of Group-type members
        spn_count           INT                 NULL        -- Count of ServicePrincipal-type members
    );

    -- Index for querying by correlation (pipeline run)
    CREATE NONCLUSTERED INDEX IX_role_mapping_audit_correlation
        ON dbo.role_mapping_audit_tbl (correlation_id);

    -- Index for querying by role name
    CREATE NONCLUSTERED INDEX IX_role_mapping_audit_role_name
        ON dbo.role_mapping_audit_tbl (role_name, [timestamp] DESC);

    -- Index for querying failures
    CREATE NONCLUSTERED INDEX IX_role_mapping_audit_failures
        ON dbo.role_mapping_audit_tbl (success) WHERE success = 0;

    PRINT 'Created dbo.role_mapping_audit_tbl with indexes';
END
ELSE
    PRINT 'dbo.role_mapping_audit_tbl already exists — skipping';
GO
