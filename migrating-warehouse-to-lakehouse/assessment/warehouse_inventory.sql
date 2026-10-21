/* ============================================================================
   Fabric Warehouse → Lakehouse migration: source inventory
   ----------------------------------------------------------------------------
   Run against the SOURCE Fabric Warehouse.
   Read-only. Creates no objects and modifies nothing.

   Each section is independently runnable. Export every result set; together
   they form the Phase 0 inventory described in 03-migration-playbook.md.

   Severity legend used in the OUTPUT of these queries:
     BLOCKER          - no Lakehouse equivalent; requires redesign
     REWRITE          - equivalent exists in a different engine/language
     BEHAVIOUR_CHANGE - works, but differently; validate
     CARRIES_OVER     - no action beyond re-pointing
   ============================================================================ */


/* ----------------------------------------------------------------------------
   1. Schemas
   ---------------------------------------------------------------------------- */
SELECT
    s.name                          AS schema_name,
    COUNT(DISTINCT t.object_id)     AS table_count,
    COUNT(DISTINCT v.object_id)     AS view_count
FROM        sys.schemas AS s
LEFT JOIN   sys.tables  AS t ON t.schema_id = s.schema_id
LEFT JOIN   sys.views   AS v ON v.schema_id = s.schema_id
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'queryinsights')
GROUP BY s.name
ORDER BY s.name;


/* ----------------------------------------------------------------------------
   2. Tables, column counts and row counts
   ---------------------------------------------------------------------------- */
SELECT
    SCHEMA_NAME(t.schema_id)        AS schema_name,
    t.name                          AS table_name,
    (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS column_count,
    SUM(p.rows)                     AS approx_row_count,
    t.create_date,
    t.modify_date
FROM        sys.tables     AS t
LEFT JOIN   sys.partitions AS p
         ON p.object_id = t.object_id
        AND p.index_id IN (0, 1)
GROUP BY SCHEMA_NAME(t.schema_id), t.name, t.create_date, t.modify_date
ORDER BY approx_row_count DESC;


/* ----------------------------------------------------------------------------
   3. Full column list with data types
   Use this to validate type compatibility in the target SQL analytics endpoint.
   ---------------------------------------------------------------------------- */
SELECT
    SCHEMA_NAME(t.schema_id)        AS schema_name,
    t.name                          AS table_name,
    c.column_id                     AS ordinal,
    c.name                          AS column_name,
    ty.name                         AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    c.is_computed,
    c.collation_name
FROM        sys.tables  AS t
INNER JOIN  sys.columns AS c  ON c.object_id = t.object_id
INNER JOIN  sys.types   AS ty ON ty.user_type_id = c.user_type_id
ORDER BY schema_name, table_name, c.column_id;


/* ----------------------------------------------------------------------------
   4. BLOCKER: IDENTITY columns
   No Lakehouse equivalent. See 04-tsql-to-spark-patterns.md section 3.
   ---------------------------------------------------------------------------- */
SELECT
    'BLOCKER'                       AS severity,
    'IDENTITY column'               AS finding,
    SCHEMA_NAME(t.schema_id)        AS schema_name,
    t.name                          AS table_name,
    c.name                          AS column_name,
    ic.seed_value,
    ic.increment_value,
    ic.last_value
FROM        sys.tables           AS t
INNER JOIN  sys.columns          AS c  ON c.object_id = t.object_id
INNER JOIN  sys.identity_columns AS ic ON ic.object_id = t.object_id
                                      AND ic.column_id = c.column_id
ORDER BY schema_name, table_name;


/* ----------------------------------------------------------------------------
   5. Computed columns and default constraints
   Computed columns must be materialized in the transformation.
   ---------------------------------------------------------------------------- */
SELECT
    'REWRITE'                       AS severity,
    'Computed column'               AS finding,
    SCHEMA_NAME(t.schema_id)        AS schema_name,
    t.name                          AS table_name,
    c.name                          AS column_name,
    cc.definition                   AS expression
FROM        sys.tables           AS t
INNER JOIN  sys.columns          AS c  ON c.object_id = t.object_id
INNER JOIN  sys.computed_columns AS cc ON cc.object_id = t.object_id
                                      AND cc.column_id = c.column_id
UNION ALL
SELECT
    'REWRITE',
    'Default constraint',
    SCHEMA_NAME(t.schema_id),
    t.name,
    c.name,
    dc.definition
FROM        sys.tables              AS t
INNER JOIN  sys.columns             AS c  ON c.object_id = t.object_id
INNER JOIN  sys.default_constraints AS dc ON dc.parent_object_id = t.object_id
                                         AND dc.parent_column_id = c.column_id
ORDER BY schema_name, table_name, column_name;


/* ----------------------------------------------------------------------------
   6. Constraints
   Primary and unique keys are informational in Fabric. Foreign keys are a
   specific hazard on the Lakehouse SQL analytics endpoint: adding one blocks
   all further schema changes on the affected tables.
   ---------------------------------------------------------------------------- */
SELECT
    'BEHAVIOUR_CHANGE'              AS severity,
    CASE WHEN kc.type = 'PK' THEN 'Primary key' ELSE 'Unique constraint' END AS finding,
    SCHEMA_NAME(t.schema_id)        AS schema_name,
    t.name                          AS table_name,
    kc.name                         AS constraint_name
FROM        sys.key_constraints AS kc
INNER JOIN  sys.tables          AS t ON t.object_id = kc.parent_object_id
UNION ALL
SELECT
    'REWRITE',
    'Foreign key - do NOT recreate on the SQL analytics endpoint',
    SCHEMA_NAME(t.schema_id),
    t.name,
    fk.name
FROM        sys.foreign_keys AS fk
INNER JOIN  sys.tables       AS t ON t.object_id = fk.parent_object_id
ORDER BY schema_name, table_name, constraint_name;


/* ----------------------------------------------------------------------------
   7. Views
   Generally carry over. Any view feeding a Direct Lake semantic model should
   become a materialized lake view in the target.
   See 04-tsql-to-spark-patterns.md section 9.
   ---------------------------------------------------------------------------- */
SELECT
    'CARRIES_OVER'                  AS severity,
    'View'                          AS finding,
    SCHEMA_NAME(v.schema_id)        AS schema_name,
    v.name                          AS view_name,
    LEN(m.definition)               AS definition_length,
    m.definition
FROM        sys.views       AS v
INNER JOIN  sys.sql_modules AS m ON m.object_id = v.object_id
ORDER BY schema_name, view_name;


/* ----------------------------------------------------------------------------
   8. Stored procedures, classified by whether they write
   Read-only procedures carry over to the SQL analytics endpoint.
   Writing procedures are blockers and must be rewritten as Spark or MLVs.
   ---------------------------------------------------------------------------- */
SELECT
    CASE
        WHEN m.definition LIKE '%INSERT %'
          OR m.definition LIKE '%UPDATE %'
          OR m.definition LIKE '%DELETE %'
          OR m.definition LIKE '%MERGE %'
          OR m.definition LIKE '%TRUNCATE %'
          OR m.definition LIKE '%COPY INTO%'
          OR m.definition LIKE '%CREATE TABLE%'
          OR m.definition LIKE '%DROP TABLE%'
          OR m.definition LIKE '%ALTER TABLE%'
        THEN 'BLOCKER'
        ELSE 'CARRIES_OVER'
    END                                                             AS severity,
    'Stored procedure'                                              AS finding,
    SCHEMA_NAME(p.schema_id)                                        AS schema_name,
    p.name                                                          AS procedure_name,
    LEN(m.definition)                                               AS definition_length,
    CASE WHEN m.definition LIKE '%MERGE %'       THEN 1 ELSE 0 END  AS uses_merge,
    CASE WHEN m.definition LIKE '%COPY INTO%'    THEN 1 ELSE 0 END  AS uses_copy_into,
    CASE WHEN m.definition LIKE '%CURSOR%'       THEN 1 ELSE 0 END  AS uses_cursor,
    CASE WHEN m.definition LIKE '%WHILE %'       THEN 1 ELSE 0 END  AS uses_loop,
    CASE WHEN m.definition LIKE '%BEGIN TRAN%'   THEN 1 ELSE 0 END  AS uses_explicit_tran,
    CASE WHEN m.definition LIKE '%EXEC(%'
           OR m.definition LIKE '%sp_executesql%' THEN 1 ELSE 0 END AS uses_dynamic_sql,
    CASE WHEN m.definition LIKE '%#%'            THEN 1 ELSE 0 END  AS uses_temp_tables,
    m.definition
FROM        sys.procedures  AS p
INNER JOIN  sys.sql_modules AS m ON m.object_id = p.object_id
ORDER BY severity, definition_length DESC;


/* ----------------------------------------------------------------------------
   9. Functions (scalar, inline TVF, multi-statement TVF)
   ---------------------------------------------------------------------------- */
SELECT
    'CARRIES_OVER'                  AS severity,
    o.type_desc                     AS finding,
    SCHEMA_NAME(o.schema_id)        AS schema_name,
    o.name                          AS function_name,
    m.definition
FROM        sys.objects     AS o
INNER JOIN  sys.sql_modules AS m ON m.object_id = o.object_id
WHERE o.type IN ('FN', 'IF', 'TF')
ORDER BY schema_name, function_name;


/* ----------------------------------------------------------------------------
   10. SECURITY: row-level security policies
   Must be re-implemented. See 05-security-mapping.md.
   ---------------------------------------------------------------------------- */
SELECT
    'BEHAVIOUR_CHANGE'              AS severity,
    'Row-level security policy'     AS finding,
    SCHEMA_NAME(sp.schema_id)       AS policy_schema,
    sp.name                         AS policy_name,
    sp.is_enabled,
    OBJECT_SCHEMA_NAME(spr.target_object_id) AS target_schema,
    OBJECT_NAME(spr.target_object_id)        AS target_table,
    spr.predicate_definition,
    spr.predicate_type_desc
FROM        sys.security_policies   AS sp
INNER JOIN  sys.security_predicates AS spr ON spr.object_id = sp.object_id
ORDER BY policy_schema, policy_name;


/* ----------------------------------------------------------------------------
   11. SECURITY: dynamic data masking
   Not supported in OneLake security. Requires delegated identity mode, or
   masking at write time. See 05-security-mapping.md section 3.
   ---------------------------------------------------------------------------- */
SELECT
    'REWRITE'                       AS severity,
    'Dynamic data masking'          AS finding,
    SCHEMA_NAME(t.schema_id)        AS schema_name,
    t.name                          AS table_name,
    c.name                          AS column_name,
    c.masking_function
FROM        sys.masked_columns AS c
INNER JOIN  sys.tables         AS t ON t.object_id = c.object_id
ORDER BY schema_name, table_name, column_name;


/* ----------------------------------------------------------------------------
   12. SECURITY: principals and role membership
   ---------------------------------------------------------------------------- */
SELECT
    dp.name                         AS principal_name,
    dp.type_desc                    AS principal_type,
    dp.authentication_type_desc,
    dp.create_date
FROM sys.database_principals AS dp
WHERE dp.type <> 'R'
  AND dp.name NOT LIKE '##%'
  AND dp.principal_id > 4
ORDER BY dp.type_desc, dp.name;

SELECT
    r.name                          AS role_name,
    m.name                          AS member_name,
    m.type_desc                     AS member_type
FROM        sys.database_role_members AS drm
INNER JOIN  sys.database_principals   AS r ON r.principal_id = drm.role_principal_id
INNER JOIN  sys.database_principals   AS m ON m.principal_id = drm.member_principal_id
ORDER BY r.name, m.name;


/* ----------------------------------------------------------------------------
   13. SECURITY: explicit object and column permissions
   Column-level grants here are the existing CLS implementation.
   ---------------------------------------------------------------------------- */
SELECT
    dp.name                           AS principal_name,
    perm.state_desc                   AS grant_state,
    perm.permission_name,
    perm.class_desc                   AS object_class,
    OBJECT_SCHEMA_NAME(perm.major_id) AS object_schema,
    OBJECT_NAME(perm.major_id)        AS object_name,
    col.name                          AS column_name,
    CASE WHEN col.name IS NOT NULL
         THEN 'Column-level security'
         ELSE 'Object-level security'
    END                               AS security_kind
FROM        sys.database_permissions AS perm
INNER JOIN  sys.database_principals  AS dp  ON dp.principal_id = perm.grantee_principal_id
LEFT JOIN   sys.columns              AS col ON col.object_id = perm.major_id
                                           AND col.column_id = perm.minor_id
WHERE perm.class_desc = 'OBJECT_OR_COLUMN'
ORDER BY principal_name, object_schema, object_name, column_name;


/* ----------------------------------------------------------------------------
   14. Workload profile: slowest and most frequent queries
   Drives the performance baseline in 07-validation-and-cutover.md.
   Adjust the window to a representative period.
   ---------------------------------------------------------------------------- */
SELECT TOP (100)
    qi.distributed_statement_id,
    qi.login_name,
    qi.start_time,
    qi.end_time,
    DATEDIFF(MILLISECOND, qi.start_time, qi.end_time) AS duration_ms,
    qi.status,
    qi.command
FROM queryinsights.exec_requests_history AS qi
WHERE qi.start_time >= DATEADD(DAY, -30, SYSUTCDATETIME())
ORDER BY duration_ms DESC;

SELECT TOP (100)
    qi.query_hash,
    COUNT(*)                                               AS execution_count,
    AVG(DATEDIFF(MILLISECOND, qi.start_time, qi.end_time)) AS avg_duration_ms,
    MAX(DATEDIFF(MILLISECOND, qi.start_time, qi.end_time)) AS max_duration_ms,
    MIN(qi.command)                                        AS sample_command
FROM queryinsights.exec_requests_history AS qi
WHERE qi.start_time >= DATEADD(DAY, -30, SYSUTCDATETIME())
GROUP BY qi.query_hash
ORDER BY execution_count DESC;


/* ----------------------------------------------------------------------------
   15. Summary scorecard input
   One row per severity class. Transcribe into assessment/migration_scorecard.md.
   ---------------------------------------------------------------------------- */
WITH findings AS (
    SELECT 'BLOCKER' AS severity, COUNT(*) AS cnt
    FROM sys.identity_columns

    UNION ALL
    SELECT 'BLOCKER', COUNT(*)
    FROM        sys.procedures  AS p
    INNER JOIN  sys.sql_modules AS m ON m.object_id = p.object_id
    WHERE m.definition LIKE '%INSERT %' OR m.definition LIKE '%UPDATE %'
       OR m.definition LIKE '%DELETE %' OR m.definition LIKE '%MERGE %'
       OR m.definition LIKE '%TRUNCATE %' OR m.definition LIKE '%COPY INTO%'

    UNION ALL
    SELECT 'REWRITE', COUNT(*) FROM sys.foreign_keys

    UNION ALL
    SELECT 'REWRITE', COUNT(*) FROM sys.masked_columns

    UNION ALL
    SELECT 'REWRITE', COUNT(*) FROM sys.computed_columns

    UNION ALL
    SELECT 'BEHAVIOUR_CHANGE', COUNT(*) FROM sys.security_policies

    UNION ALL
    SELECT 'CARRIES_OVER', COUNT(*) FROM sys.views

    UNION ALL
    SELECT 'CARRIES_OVER', COUNT(*)
    FROM        sys.procedures  AS p
    INNER JOIN  sys.sql_modules AS m ON m.object_id = p.object_id
    WHERE NOT (m.definition LIKE '%INSERT %' OR m.definition LIKE '%UPDATE %'
            OR m.definition LIKE '%DELETE %' OR m.definition LIKE '%MERGE %'
            OR m.definition LIKE '%TRUNCATE %' OR m.definition LIKE '%COPY INTO%')
)
SELECT severity, SUM(cnt) AS finding_count
FROM findings
GROUP BY severity
ORDER BY CASE severity
            WHEN 'BLOCKER'          THEN 1
            WHEN 'REWRITE'          THEN 2
            WHEN 'BEHAVIOUR_CHANGE' THEN 3
            ELSE 4
         END;
