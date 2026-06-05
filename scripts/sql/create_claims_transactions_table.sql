CREATE TABLE claims_transactions (
    id VARCHAR(50) PRIMARY KEY,
    claim_id VARCHAR(50),
    charge_id VARCHAR(50),
    patient_id VARCHAR(50),
    type VARCHAR(50),
    amount DECIMAL(10, 2),
    method VARCHAR(50),
    from_date DATE,
    to_date DATE,
    last_updated_dttm DATETIME
);
