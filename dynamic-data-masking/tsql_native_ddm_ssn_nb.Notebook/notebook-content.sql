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

-- # Native Dynamic Data Masking (DDM) for SSN
-- 
-- This notebook implements masking using **Fabric's native Dynamic Data Masking**
-- feature (`ALTER COLUMN ... ADD MASKED WITH (FUNCTION = ...)`) instead of the
-- view + role + DENY pattern in `tsql_data_mask_ssn_nb`.
-- 
-- **Key difference / trade-off:** Native DDM masks the **entire column
-- unconditionally** for every principal except those granted `UNMASK`. It
-- **cannot** express the row-level "only mask SSN for employees who are also
-- students" rule. If that conditional requirement is mandatory, use the
-- view/role pattern instead. This notebook is the lighter-weight choice when the
-- requirement is simply "mask SSN for all non-privileged users."
-- 
-- > **Security scope:** the mask applies to the **SQL analytics endpoint /
-- > Warehouse query path only**. Spark, OneLake shortcuts, and Direct Lake
-- > semantic models read the underlying Delta files and are **not** masked. See
-- > `ddm_security_posture.md` before treating this as a protection boundary.
-- 
-- **Placeholders** — replace before running:
-- 
-- | Placeholder | Meaning |
-- |---|---|
-- | `<unmask-group>` | Entra security group allowed to see unmasked values |

-- CELL ********************

-- Inspect the current SSN values before masking is applied
SELECT TOP 20 id, first_name, last_name, social_security_number
FROM dbo.employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 1. Apply the native mask to the SSN column
-- 
-- `partial(prefix, padding, suffix)` exposes `prefix` leading chars and `suffix`
-- trailing chars, replacing everything in between with the literal `padding`.
-- Using `partial(0, "XXX-XX-", 4)` reveals **no** leading chars, injects the
-- literal `XXX-XX-`, and keeps the **last 4** digits — producing `XXX-XX-6789`,
-- matching the output of the original view-based example.

-- CELL ********************

-- Apply native Dynamic Data Masking to the SSN column on the base table
ALTER TABLE dbo.employee
    ALTER COLUMN social_security_number
    ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)');

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 2. Confirm the column is now masked
-- 
-- `sys.masked_columns` lists every column with a masking function applied,
-- along with the function definition.

-- CELL ********************

SELECT t.name           AS table_name,
       c.name           AS column_name,
       c.is_masked,
       c.masking_function
FROM sys.masked_columns AS c
JOIN sys.tables AS t
    ON c.object_id = t.object_id
WHERE t.name = 'employee';

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 3. Grant UNMASK to a privileged Entra group
-- 
-- Unlike the view/role pattern (which used `DENY SELECT` to block base-table
-- access), native DDM masks by default and you instead **grant `UNMASK`** to the
-- principals who are allowed to see the real values.
-- 
-- Grant to an **Entra security group**, not to individual users, so membership is
-- managed in Entra ID and no T-SQL change is required as people join or leave.
-- 
-- Every other principal querying `dbo.employee` automatically receives the
-- masked value with no view, role membership, or DENY required.
-- 
-- Replace `<unmask-group>` with the Entra group display name.

-- CELL ********************

-- The group must exist as a database principal before it can be granted UNMASK.
CREATE USER [<unmask-group>] FROM EXTERNAL PROVIDER;

-- UNMASK only changes how masked columns render. It does not grant read access
-- and does not override a DENY, so the principal still needs SELECT.
GRANT SELECT ON OBJECT::dbo.employee TO [<unmask-group>];

-- Least privilege: grant UNMASK on the specific column that needs to be readable.
GRANT UNMASK ON dbo.employee(social_security_number) TO [<unmask-group>];

-- Broader alternatives, in increasing order of exposure:
--   GRANT UNMASK ON OBJECT::dbo.employee TO [<unmask-group>];  -- every masked column on the table
--   GRANT UNMASK TO [<unmask-group>];                          -- every masked column in the database
-- Prefer the column-scoped grant above unless a wider scope is explicitly required.

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 4. Verify masking behavior
-- 
-- A principal **without** `UNMASK` sees `XXX-XX-####`; a principal **with**
-- `UNMASK` sees the real value.
-- 
-- **Masking cannot be verified from an admin/owner session.** Workspace admins,
-- the warehouse owner, and `db_owner` members hold implicit `UNMASK` and always
-- see real values. Connect to the SQL analytics endpoint as a non-privileged
-- principal to confirm the mask.
-- 
-- The query below lists who currently holds `UNMASK` so the privileged set can be
-- reviewed without impersonation.

-- CELL ********************

-- Who currently holds UNMASK, at any scope?
SELECT pr.name       AS principal_name,
       pr.type_desc  AS principal_type,
       pe.state_desc AS permission_state,
       pe.permission_name,
       pe.class_desc AS permission_scope
FROM sys.database_permissions AS pe
JOIN sys.database_principals AS pr
    ON pe.grantee_principal_id = pr.principal_id
WHERE pe.permission_name = 'UNMASK';

-- Note: this does NOT list workspace Admin/Member/Contributor members or any
-- principal with CONTROL on the database. They hold UNMASK implicitly and always
-- see real values. Review workspace role membership separately.

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Run this AS a non-privileged principal (not as an admin/owner) to see the mask.
SELECT TOP 20 id, first_name, last_name, social_security_number
FROM dbo.employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 5. (Optional) Revoke or remove the mask
-- 
-- Use these to roll back the changes made by this notebook.

-- CELL ********************

-- Revoke UNMASK from the group (its members will then see masked values)
-- REVOKE UNMASK ON dbo.employee(social_security_number) FROM [<unmask-group>];

-- Drop the mask from the column entirely (all principals see real values again)
-- ALTER TABLE dbo.employee
--     ALTER COLUMN social_security_number DROP MASKED;

-- DROP USER [<unmask-group>];

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
