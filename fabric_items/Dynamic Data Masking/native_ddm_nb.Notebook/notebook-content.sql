-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
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

-- # Native DDM
-- This notebook implements masking using Fabric's native Dynamic Data Masking feature instead of the conditional
-- pattern applied in the tsql_data_mask_ssn_nb
-- 
-- - This notebook masks the entire column unconditionally whereas in the other notebook it is conditional based on 
-- if the user is a student or not
-- - This is the simpler, mask all SSNs pattern
-- 
-- - When adf_user_2@MngEnvMCAP372892.onmicrosoft.com views this data, it will be masked (XXX-XX-4digits)
-- - When someone with MASKED views the data it will b....

-- CELL ********************

SELECT *
FROM dbo.employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

ALTER TABLE dbo.employee
ALTER COLUMN social_security_number
ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XX-",4)');

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT 
t.name AS table_name,
c.name AS column_name,
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

-- CELL ********************

-- Grant users that can view the raw data  This is database level unmasking so that means all masked columns are unmasked
GRANT UNMASK TO [adf_user_2@MngEnvMCAP372892.onmicrosoft.com];


-- Can also do unmasking on specific tables
-- GRANT UNMASK ON OBJECT::dbo.employee TO [adf_user_2@MngEnvMCAP372892.onmicrosoft.com];

-- Or column level unmasking
-- GRANT UNMASK ON dbo.employee(social_security_number) TO [adf_user_2@MngEnvMCAP372892.onmicrosoft.com];

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP 20 id, first_name, last_name, social_security_number
FROM dbo.employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
