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

-- ## 3. Grant UNMASK to privileged principals
-- 
-- Unlike the view/role pattern (which used `DENY SELECT` to block base-table
-- access), native DDM masks by default and you instead **grant `UNMASK`** to the
-- principals who are allowed to see the real values — e.g. data engineers.
-- 
-- Every other principal querying `dbo.employee` automatically receives the
-- masked value with no view, role membership, or DENY required.

-- CELL ********************

-- Allow a privileged principal (e.g. data engineer) to see unmasked SSNs.
-- In Fabric, UNMASK is granted at the database scope.
GRANT UNMASK TO [adf_user_2@MngEnvMCAP372892.onmicrosoft.com];

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## 4. Verify masking behavior
-- 
-- A principal **without** `UNMASK` sees `XXX-XX-####`; a principal **with**
-- `UNMASK` sees the real SSN. To test as another user, connect to the SQL
-- analytics endpoint as that principal and run the SELECT below.

-- CELL ********************

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

-- Revoke UNMASK from the principal (they will then see masked values)
-- REVOKE UNMASK FROM [adf_user_2@MngEnvMCAP372892.onmicrosoft.com];

-- Drop the mask from the column entirely (all principals see real values again)
-- ALTER TABLE dbo.employee
--     ALTER COLUMN social_security_number DROP MASKED;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
