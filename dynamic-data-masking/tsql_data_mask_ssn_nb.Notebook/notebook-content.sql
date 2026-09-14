-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "70e18f53-f14f-41bd-b3d0-8060d42c4909",
-- META       "default_lakehouse_name": "health_silver_lh",
-- META       "default_lakehouse_workspace_id": "a8cbda3d-903e-4154-97d9-9a91c95abb42",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "70e18f53-f14f-41bd-b3d0-8060d42c4909"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "bea89836-d75a-4946-b79b-b0e8a10d9c0b",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "bea89836-d75a-4946-b79b-b0e8a10d9c0b",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Conditional Masking — Masked View + Role Pattern
--
-- Implements **row-dependent** masking: the SSN is masked only for employees who
-- are also students. Native Dynamic Data Masking cannot express a row-level
-- condition, so masking is enforced with a **view + role + DENY** pattern.
--
-- > **Security scope:** this pattern is an *obfuscation* control at the SQL
-- > analytics endpoint only. It does **not** cover Spark, OneLake shortcuts, or
-- > Direct Lake semantic models reading the same Delta files. See
-- > `ddm_security_posture.md` before treating it as a protection boundary.
--
-- **Placeholders** — replace before running:
--
-- | Placeholder | Meaning |
-- |---|---|
-- | `<masked-readers-group>` | Entra security group that must see masked values |
--
-- Run order: cells top to bottom. Section 6 verifies, section 7 rolls back.

-- CELL ********************

-- Show all schemas
SELECT *
FROM sys.schemas

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Create sec schema if not exists to apply views to
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    
    WHERE
        name = 'sec'
)
EXEC('CREATE SCHEMA sec')

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Create function to apply masking for social security numbers
CREATE OR ALTER FUNCTION sec.fn_mask_ssn(@ssn NVARCHAR(11))
RETURNS NVARCHAR(11)
AS
BEGIN
    -- If input is NULL, return NULL
    IF @ssn IS NULL
        RETURN NULL;

    -- Ensure SSN has at least 4 characters
    IF LEN(@ssn) < 4
        RETURN @ssn;

    -- Return masked SSN in format XXX-XX-#### (last 4 digits preserved)
    RETURN CONCAT('XXX-XX-', RIGHT(@ssn, 4));
END;
GO


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Exploratory: identify the overlap population (employees who are also students).
-- CAUTION: this returns RAW, unmasked values and joins on the sensitive column
-- itself. Run it only as a privileged identity, and never in a shared session.
SELECT *
FROM employee
INNER JOIN
student
ON
employee.social_security_number = student.social_security_number

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT employee.id,
       employee.first_name,
       CASE 
       WHEN EXISTS (SELECT 1 FROM student 
                            WHERE student.social_security_number = employee.social_security_number)
        THEN CONCAT('XXX-XX-', RIGHT(employee.social_security_number, 4)
        )
        ELSE employee.social_security_number
    END AS social_security_number
FROM employee;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Masked view. The masking format lives in sec.fn_mask_ssn so the mask is defined
-- in exactly one place; the view supplies only the row-level condition.
CREATE OR ALTER VIEW sec.vw_employee_masked AS
    SELECT employee.id,
           employee.first_name,
           employee.last_name,
           CASE
               WHEN EXISTS (SELECT 1 FROM student
                            WHERE student.social_security_number = employee.social_security_number)
                   THEN sec.fn_mask_ssn(employee.social_security_number)
               ELSE employee.social_security_number
           END AS social_security_number
    FROM employee;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT *
FROM sec.vw_employee_masked

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Create the role that masked readers belong to.
-- GRANT reads on the masked view; DENY reads on the base tables so the mask
-- cannot be walked around. DENY always wins over a conflicting GRANT.
-- NOTE: DENY is a T-SQL permission only. It does NOT restrict Spark, OneLake
-- shortcuts, or Direct Lake access to the same Delta files — use OneLake data
-- access roles for those paths. See ddm_security_posture.md.
CREATE ROLE maskedReaders;

GRANT SELECT ON OBJECT::sec.vw_employee_masked TO maskedReaders;
DENY SELECT ON OBJECT::dbo.employee TO maskedReaders;
DENY SELECT ON OBJECT::dbo.student TO maskedReaders;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Add the Entra security GROUP to the role, not individual users, so membership
-- is managed in Entra ID and no T-SQL change is needed when people join or leave.
--
-- Replace <masked-readers-group> with the Entra group display name.
CREATE USER [<masked-readers-group>] FROM EXTERNAL PROVIDER;

ALTER ROLE maskedReaders ADD MEMBER [<masked-readers-group>];

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 6. Verify
--
-- Confirm the role membership and the DENY grants actually landed. Masking
-- **cannot** be verified from an admin/owner session — an admin is not subject
-- to the DENY. Connect to the SQL analytics endpoint as a member of
-- `<masked-readers-group>` and confirm:
--
-- 1. `SELECT * FROM sec.vw_employee_masked` succeeds and SSNs are masked for
--    employees who are also students.
-- 2. `SELECT * FROM dbo.employee` **fails** with a permission error.
-- 3. `SELECT * FROM dbo.student` **fails** with a permission error.
--
-- If step 2 or 3 returns rows, the mask is bypassable and the pattern is not in
-- effect.

-- CELL ********************

-- Effective role membership
SELECT r.name AS role_name,
       m.name AS member_name,
       m.type_desc
FROM sys.database_role_members AS rm
JOIN sys.database_principals AS r ON rm.role_principal_id = r.principal_id
JOIN sys.database_principals AS m ON rm.member_principal_id = m.principal_id
WHERE r.name = 'maskedReaders';

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Effective permissions granted/denied to the role.
-- Expect: GRANT SELECT on sec.vw_employee_masked, DENY SELECT on dbo.employee and dbo.student.
SELECT pr.name           AS principal_name,
       pe.state_desc     AS permission_state,
       pe.permission_name,
       OBJECT_SCHEMA_NAME(pe.major_id) + '.' + OBJECT_NAME(pe.major_id) AS object_name
FROM sys.database_permissions AS pe
JOIN sys.database_principals AS pr ON pe.grantee_principal_id = pr.principal_id
WHERE pr.name = 'maskedReaders';

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 7. Roll back
--
-- Uncomment and run to remove everything this notebook created.

-- CELL ********************

-- ALTER ROLE maskedReaders DROP MEMBER [<masked-readers-group>];
-- DROP ROLE maskedReaders;
-- DROP VIEW sec.vw_employee_masked;
-- DROP FUNCTION sec.fn_mask_ssn;
-- DROP USER [<masked-readers-group>];

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
