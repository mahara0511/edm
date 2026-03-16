-- ============================================================
-- FILE: 02_ddl_datawarehouse.sql
-- PURPOSE: Star Schema for Debt Status Data Warehouse
-- TARGET: PostgreSQL
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ==================== DIMENSION TABLES ====================

-- DIM_CUSTOMER
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key        SERIAL          PRIMARY KEY,
    cif_number          VARCHAR(20)     NOT NULL,
    full_name           VARCHAR(100),
    national_id         VARCHAR(20),
    phone_number        VARCHAR(15),
    current_address     VARCHAR(255),
    email               VARCHAR(100),
    -- SCD Type 2 fields
    effective_date      DATE            NOT NULL DEFAULT CURRENT_DATE,
    expiry_date         DATE,
    is_current          BOOLEAN         DEFAULT TRUE
);

-- DIM_BRANCH
CREATE TABLE IF NOT EXISTS dw.dim_branch (
    branch_key          SERIAL          PRIMARY KEY,
    branch_code         VARCHAR(10)     NOT NULL,
    branch_name         VARCHAR(100),
    region_name         VARCHAR(100)
);

-- DIM_PRODUCT
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key         SERIAL          PRIMARY KEY,
    contract_number     VARCHAR(30)     NOT NULL,
    product_type        VARCHAR(50),    -- MORTGAGE, HOME_EQUITY, VISA_PLATINUM, etc.
    interest_rate       NUMERIC(5,4),
    maturity_date       DATE
);

-- DIM_TIME
CREATE TABLE IF NOT EXISTS dw.dim_time (
    time_key            SERIAL          PRIMARY KEY,
    full_date           DATE            NOT NULL UNIQUE,
    day                 INT             NOT NULL,
    month               INT             NOT NULL,
    quarter             INT             NOT NULL,
    year                INT             NOT NULL,
    day_name            VARCHAR(10),
    month_name          VARCHAR(10),
    is_weekend          VARCHAR(5)      -- 'true'/'false'
);

-- ==================== FACT TABLE ====================

-- FACT_DEBT_STATUS
CREATE TABLE IF NOT EXISTS dw.fact_debt_status (
    fact_id             SERIAL          PRIMARY KEY,
    customer_key        INT             NOT NULL REFERENCES dw.dim_customer(customer_key),
    branch_key          INT             NOT NULL REFERENCES dw.dim_branch(branch_key),
    product_key         INT             NOT NULL REFERENCES dw.dim_product(product_key),
    time_key            INT             NOT NULL REFERENCES dw.dim_time(time_key),
    -- Measures
    principal_amount    NUMERIC(18,2),
    interest_amount     NUMERIC(18,2),
    total_outstanding   NUMERIC(18,2),
    days_past_due       INT             DEFAULT 0,
    risk_bucket         VARCHAR(20)     -- 'CURRENT','1-30 DPD','31-60 DPD','61-90 DPD','NPL'
);

-- ==================== INDEXES ====================
CREATE INDEX IF NOT EXISTS idx_fact_customer    ON dw.fact_debt_status(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_branch      ON dw.fact_debt_status(branch_key);
CREATE INDEX IF NOT EXISTS idx_fact_product     ON dw.fact_debt_status(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_time        ON dw.fact_debt_status(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_risk        ON dw.fact_debt_status(risk_bucket);
CREATE INDEX IF NOT EXISTS idx_dim_time_date    ON dw.dim_time(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_cust_cif     ON dw.dim_customer(cif_number);
CREATE INDEX IF NOT EXISTS idx_dim_branch_code  ON dw.dim_branch(branch_code);

-- ==================== ANALYTIC VIEWS ====================

-- View: NPL Summary by Branch and Month
CREATE OR REPLACE VIEW dw.v_npl_by_branch_month AS
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    db.branch_name,
    db.region_name,
    COUNT(*)                            AS total_loans,
    SUM(f.total_outstanding)            AS total_outstanding,
    SUM(CASE WHEN f.risk_bucket = 'NPL' THEN f.total_outstanding ELSE 0 END) AS npl_amount,
    ROUND(
        SUM(CASE WHEN f.risk_bucket = 'NPL' THEN f.total_outstanding ELSE 0 END)
        / NULLIF(SUM(f.total_outstanding), 0) * 100, 2
    )                                   AS npl_ratio_pct
FROM dw.fact_debt_status f
JOIN dw.dim_time     dt ON f.time_key     = dt.time_key
JOIN dw.dim_branch   db ON f.branch_key   = db.branch_key
GROUP BY dt.year, dt.month, dt.month_name, db.branch_name, db.region_name
ORDER BY dt.year, dt.month, db.branch_name;

-- View: Customer Debt Summary (current snapshot)
CREATE OR REPLACE VIEW dw.v_customer_debt_summary AS
SELECT
    dc.cif_number,
    dc.full_name,
    db.branch_name,
    dp.product_type,
    f.total_outstanding,
    f.days_past_due,
    f.risk_bucket,
    dt.full_date AS snapshot_date
FROM dw.fact_debt_status f
JOIN dw.dim_customer dc ON f.customer_key = dc.customer_key
JOIN dw.dim_branch   db ON f.branch_key   = db.branch_key
JOIN dw.dim_product  dp ON f.product_key  = dp.product_key
JOIN dw.dim_time     dt ON f.time_key     = dt.time_key
WHERE dc.is_current = TRUE;

-- View: Risk Bucket Distribution
CREATE OR REPLACE VIEW dw.v_risk_distribution AS
SELECT
    dt.year,
    dt.quarter,
    f.risk_bucket,
    dp.product_type,
    COUNT(*)                            AS loan_count,
    SUM(f.total_outstanding)            AS total_outstanding,
    AVG(f.days_past_due)                AS avg_days_past_due
FROM dw.fact_debt_status f
JOIN dw.dim_time    dt ON f.time_key    = dt.time_key
JOIN dw.dim_product dp ON f.product_key = dp.product_key
GROUP BY dt.year, dt.quarter, f.risk_bucket, dp.product_type
ORDER BY dt.year, dt.quarter, f.risk_bucket;
