-- ============================================================
-- FILE: 06_ddl_dw_ewallet_payment.sql
-- PURPOSE: Extend Star Schema in dw schema for E-Wallet & Payment
-- TARGET: PostgreSQL / DW_EDM
-- ============================================================

-- ============================================================
-- NEW DIMENSION: DIM_MERCHANT (dùng cho Payment)
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_merchant (
    merchant_key        SERIAL          PRIMARY KEY,
    merchant_code       VARCHAR(20)     NOT NULL,
    merchant_name       VARCHAR(100),
    merchant_category   VARCHAR(50),
    region              VARCHAR(50),
    branch_code         VARCHAR(10),
    status              VARCHAR(20)
);

-- NEW DIMENSION: DIM_CHANNEL (dùng cho cả E-wallet & Payment)
CREATE TABLE IF NOT EXISTS dw.dim_channel (
    channel_key         SERIAL          PRIMARY KEY,
    channel_code        VARCHAR(30)     NOT NULL,   -- EWALLET, CARD, QR_CODE, BANK_TRANSFER, ATM
    channel_name        VARCHAR(50),
    channel_type        VARCHAR(30),    -- DIGITAL, PHYSICAL
    product_line        VARCHAR(20)     -- EWALLET, PAYMENT, LENDING
);

-- ============================================================
-- FACT TABLE: FACT_EWALLET_TRANSACTION
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.fact_ewallet_transaction (
    fact_id             SERIAL          PRIMARY KEY,
    customer_key        INT             NOT NULL REFERENCES dw.dim_customer(customer_key),
    branch_key          INT             NOT NULL REFERENCES dw.dim_branch(branch_key),
    time_key            INT             NOT NULL REFERENCES dw.dim_time(time_key),
    channel_key         INT             NOT NULL REFERENCES dw.dim_channel(channel_key),
    -- Measures
    txn_count           INT             DEFAULT 1,
    txn_amount          NUMERIC(18,2),
    fee_amount          NUMERIC(18,2),
    net_amount          NUMERIC(18,2),
    -- Degenerate dimensions (attributes on fact)
    txn_type            VARCHAR(30),    -- TOP_UP, TRANSFER, WITHDRAW, PAYMENT, REFUND
    txn_status          VARCHAR(20),    -- SUCCESS, FAILED, PENDING
    -- Balance snapshot
    closing_balance     NUMERIC(18,2)
);

-- ============================================================
-- FACT TABLE: FACT_PAYMENT_TRANSACTION
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.fact_payment_transaction (
    fact_id             SERIAL          PRIMARY KEY,
    customer_key        INT             NOT NULL REFERENCES dw.dim_customer(customer_key),
    merchant_key        INT             NOT NULL REFERENCES dw.dim_merchant(merchant_key),
    branch_key          INT             NOT NULL REFERENCES dw.dim_branch(branch_key),
    time_key            INT             NOT NULL REFERENCES dw.dim_time(time_key),
    channel_key         INT             NOT NULL REFERENCES dw.dim_channel(channel_key),
    -- Measures
    txn_count           INT             DEFAULT 1,
    txn_amount          NUMERIC(18,2),
    fee_amount          NUMERIC(18,2),
    net_amount          NUMERIC(18,2),
    refund_amount       NUMERIC(18,2)   DEFAULT 0,
    -- Degenerate dimensions
    payment_method      VARCHAR(30),
    txn_status          VARCHAR(20),
    merchant_category   VARCHAR(50)
);

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_fact_ew_customer  ON dw.fact_ewallet_transaction(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_ew_time      ON dw.fact_ewallet_transaction(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_ew_branch    ON dw.fact_ewallet_transaction(branch_key);
CREATE INDEX IF NOT EXISTS idx_fact_ew_channel   ON dw.fact_ewallet_transaction(channel_key);
CREATE INDEX IF NOT EXISTS idx_fact_ew_type      ON dw.fact_ewallet_transaction(txn_type);

CREATE INDEX IF NOT EXISTS idx_fact_pay_customer ON dw.fact_payment_transaction(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_pay_merchant ON dw.fact_payment_transaction(merchant_key);
CREATE INDEX IF NOT EXISTS idx_fact_pay_time     ON dw.fact_payment_transaction(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_pay_channel  ON dw.fact_payment_transaction(channel_key);

-- ============================================================
-- ANALYTIC VIEWS
-- ============================================================

-- View: E-wallet transaction volume by month & type
CREATE OR REPLACE VIEW dw.v_ewallet_monthly_summary AS
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    db.branch_name,
    db.region_name,
    f.txn_type,
    f.txn_status,
    COUNT(*)                            AS txn_count,
    SUM(f.txn_amount)                   AS total_amount,
    SUM(f.fee_amount)                   AS total_fee,
    AVG(f.txn_amount)                   AS avg_txn_amount
FROM dw.fact_ewallet_transaction f
JOIN dw.dim_time   dt ON f.time_key   = dt.time_key
JOIN dw.dim_branch db ON f.branch_key = db.branch_key
GROUP BY dt.year, dt.month, dt.month_name, db.branch_name, db.region_name, f.txn_type, f.txn_status
ORDER BY dt.year, dt.month;

-- View: Payment revenue by merchant category & channel
CREATE OR REPLACE VIEW dw.v_payment_revenue_summary AS
SELECT
    dt.year,
    dt.quarter,
    dt.month,
    dm.merchant_category,
    dc.channel_name,
    db.region_name,
    COUNT(*)                            AS txn_count,
    SUM(f.txn_amount)                   AS gross_amount,
    SUM(f.fee_amount)                   AS total_fee,
    SUM(f.refund_amount)                AS total_refund,
    SUM(f.net_amount)                   AS net_revenue,
    ROUND(SUM(f.fee_amount) / NULLIF(SUM(f.txn_amount),0) * 100, 3) AS fee_rate_pct
FROM dw.fact_payment_transaction f
JOIN dw.dim_time     dt ON f.time_key     = dt.time_key
JOIN dw.dim_merchant dm ON f.merchant_key = dm.merchant_key
JOIN dw.dim_channel  dc ON f.channel_key  = dc.channel_key
JOIN dw.dim_branch   db ON f.branch_key   = db.branch_key
GROUP BY dt.year, dt.quarter, dt.month, dm.merchant_category, dc.channel_name, db.region_name
ORDER BY dt.year, dt.month;

-- View: Cross-product customer activity (Lending + E-wallet + Payment)
CREATE OR REPLACE VIEW dw.v_customer_product_activity AS
SELECT
    dc.cif_number,
    dc.full_name,
    COALESCE(l.loan_count, 0)       AS loan_count,
    COALESCE(l.total_outstanding, 0) AS total_loan_outstanding,
    COALESCE(e.ew_txn_count, 0)     AS ewallet_txn_count,
    COALESCE(e.ew_total_amount, 0)  AS ewallet_total_amount,
    COALESCE(p.pay_txn_count, 0)    AS payment_txn_count,
    COALESCE(p.pay_total_amount, 0) AS payment_total_amount,
    CASE
        WHEN COALESCE(l.loan_count,0)>0 AND COALESCE(e.ew_txn_count,0)>0 AND COALESCE(p.pay_txn_count,0)>0 THEN '3-Product'
        WHEN (COALESCE(l.loan_count,0)>0)::INT + (COALESCE(e.ew_txn_count,0)>0)::INT + (COALESCE(p.pay_txn_count,0)>0)::INT = 2 THEN '2-Product'
        ELSE '1-Product'
    END AS customer_segment
FROM dw.dim_customer dc
LEFT JOIN (
    SELECT customer_key, COUNT(*) AS loan_count, SUM(total_outstanding) AS total_outstanding
    FROM dw.fact_debt_status GROUP BY customer_key
) l ON dc.customer_key = l.customer_key
LEFT JOIN (
    SELECT customer_key, COUNT(*) AS ew_txn_count, SUM(txn_amount) AS ew_total_amount
    FROM dw.fact_ewallet_transaction GROUP BY customer_key
) e ON dc.customer_key = e.customer_key
LEFT JOIN (
    SELECT customer_key, COUNT(*) AS pay_txn_count, SUM(txn_amount) AS pay_total_amount
    FROM dw.fact_payment_transaction GROUP BY customer_key
) p ON dc.customer_key = p.customer_key
WHERE dc.is_current = TRUE;
