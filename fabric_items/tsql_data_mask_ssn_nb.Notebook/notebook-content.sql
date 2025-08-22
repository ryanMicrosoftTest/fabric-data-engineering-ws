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

-- CELL ********************

SELECT *
FROM sys.schemas

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT TOP (100) [id],
			[first_name],
			[social_security_number]
FROM [health_silver_lh].[sec].[vw_employee]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

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

-- create a view and apply the conditional masking to the view (note this cannot be done on the table itself because DDM only supports static masking functions and doesn't allow conditional expressions or joins)


CREATE VIEW sec.vw_employee AS
SELECT employee.id,
       employee.first_name,
       CASE 
       WHEN EXISTS (SELECT 1 FROM student 
                            WHERE student.id = employee.id)
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
FROM sec.vw_employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE OR ALTER FUNCTION sec.fn_mask_ssn(@ssn NVARCHAR(64))
RETURNS NVARCHAR(11)
AS
BEGIN
    IF @ssn IS NULL 
        RETURN NULL;

    DECLARE @digits NVARCHAR(64) = LTRIM(RTRIM(@ssn));
    -- strip common separators
    SET @digits = REPLACE(REPLACE(REPLACE(REPLACE(@digits, '-', ''), ' ', ''), '.', ''), CHAR(160), '');

    -- require exactly 9 digits; anything else -> fully redacted
    IF LEN(@digits) <> 9 OR @digits LIKE '%[^0-9]%'
        RETURN N'***-**-****';

    RETURN CONCAT(N'XXXX-XX-', RIGHT(@digits, 4));
END;


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

IF OBJECT_ID(N'sec.fn_mask_ssn', N'FN') IS NOT NULL
    DROP FUNCTION sec.fn_mask_ssn
    

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

CREATE FUNCTION sec.fn_mask_ssn(@ssn NVARCHAR(64))
RETURNS NVARCHAR(11)
WITH INLINE = ON
AS
BEGIN
    -- Normalize the input to just digits (rstrip spaces, dashes, dots, NBSP)
    DECLARE @d NVARCHAR(64) = REPLACE(REPLACE(REPLACE(REPLACE(@ssn, '-', ''), ' ', ''), '.', ''), CHAR(160), '');

    -- Single Return: required for inlining eligability in Fabric 
    RETURN CASE
        WHEN @ssn IS NULL THEN NULL
        WHEN LEN(@d) <> 9 OR @d LIKE '%[^0-9]%' THEN N'***-**-****'
        ELSE CONCAT(N'XXX-XX-', RIGHT(@d, 4))
    END;
END;
GO


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Conditional Masking View -> Mask only for Students
CREATE OR ALTER VIEW sec.vw_employee AS
SELECT employee.id,
       employee.first_name,
       CASE 
       WHEN EXISTS (SELECT 1 FROM student 
                            WHERE student.id = employee.id)
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

SELECT TOP (100) [id],
			[first_name],
			[social_security_number]
FROM [health_silver_lh].[sec].[vw_employee]

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

UPDATE STATISTICS sec.vw_employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT name AS ViewName
FROM sys.views
WHERE schema_id = SCHEMA_ID('sec');

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT social_security_number
FROM employee

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

SELECT *
FROM employee

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

-- Masked group
CREATE OR ALTER VIEW sec.vw_employee_masked AS
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

SELECT *
FROM sec.vw_employee_masked

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- Create role for users to 
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

ALTER ROLE maskedReaders ADD MEMBER [adf_user_2@MngEnvMCAP372892.onmicrosoft.com]

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
