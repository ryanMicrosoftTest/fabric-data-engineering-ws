-- Dimension Table: Stores
CREATE TABLE dim_store (
    store_code nchar(10) PRIMARY KEY,
    store_name nvarchar(100),
    store_location GEOGRAPHY
);

-- Dimension Table: Products
CREATE TABLE dim_product (
    product_id int PRIMARY KEY,
    product_name nvarchar(100),
    restock_date smalldatetime,
    restock_priority_level tinyint
);

-- Fact Table: Sales
CREATE TABLE fact_sales (
    bigint_transaction_id bigint IDENTITY(1,1) PRIMARY KEY,
    sale money,
    unit_price smallmoney,
    sale_date DATETIME2(7),             -- For 7 decimal places of precision in the date and time
    sale_timestamp DATETIMEOFFSET(7),   -- For 7 decimal places of precision in the date and time with timezone offset
    store_id nchar(10),
    product_id int,
    FOREIGN KEY (store_id) REFERENCES dim_store(store_code),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);