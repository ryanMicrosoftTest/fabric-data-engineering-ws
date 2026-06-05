
DECLARE @i INT = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO claims_transactions (id, claim_id, charge_id, patient_id, type, amount, method, from_date, to_date, last_updated_dttm)
    VALUES (
        CONCAT('TX', FORMAT(@i, '000')),
        CONCAT('CLM', FORMAT(1000 + @i, '0000')),
        CONCAT('CHG', FORMAT(2000 + @i, '0000')),
        CONCAT('PAT', FORMAT(3000 + @i, '0000')),
        CASE (@i % 3)
            WHEN 0 THEN 'Consultation'
            WHEN 1 THEN 'Surgery'
            ELSE 'Lab Test'
        END,
        ROUND(RAND() * 1000 + 100, 2),
        CASE (@i % 3)
            WHEN 0 THEN 'Credit Card'
            WHEN 1 THEN 'Insurance'
            ELSE 'Cash'
        END,
        DATEADD(DAY, -(@i % 30 + 1), GETDATE()),
        DATEADD(DAY, -(@i % 30), GETDATE()),
        DATEADD(DAY, -1, GETDATE())
    );
    SET @i = @i + 1;
END;
