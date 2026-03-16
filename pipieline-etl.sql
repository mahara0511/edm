-- ============================================================
-- FILE: 07_full_dw_load.sql
-- PURPOSE: Tổng hợp toàn bộ DDL + hướng dẫn load cho DW_EDM
--          3 product lines: Lending | E-Wallet | Payment
-- TARGET: PostgreSQL / DBeaver
-- ============================================================

-- ===========================================================
-- BƯỚC 1: CHẠY CÁC FILE DDL THEO THỨ TỰ
-- ===========================================================
-- 1. 01_ddl_source_tables.sql        → schemas corebank, card
-- 2. 02_ddl_datawarehouse.sql        → schema dw (lending star schema)
-- 3. 05_ddl_ewallet_payment_source.sql → schemas ewallet, payment
-- 4. 06_ddl_dw_ewallet_payment.sql   → extend dw (ewallet + payment)

-- ===========================================================
-- BƯỚC 2: KIỂM TRA CÁC TABLES ĐÃ TẠO
-- ===========================================================

SELECT
    table_schema    AS schema,
    table_name,
    CASE
        WHEN table_name LIKE 'dim_%'  THEN 'Dimension'
        WHEN table_name LIKE 'fact_%' THEN 'Fact'
        WHEN table_name LIKE 'v_%'    THEN 'View'
        ELSE 'Source'   
    END AS table_type
FROM information_schema.tables
WHERE table_schema IN ('dw','corebank','card','ewallet','payment')
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;

-- ===========================================================
-- BƯỚC 3: SAU KHI LOAD DATA — VERIFY ROW COUNTS
-- ===========================================================

SELECT 'DIMENSIONS' AS section, '' AS table_name, 0 AS row_count WHERE FALSE
UNION ALL
SELECT '---', 'DIM_CUSTOMER',    COUNT(*)::INT FROM dw.dim_customer
UNION ALL
SELECT '---', 'DIM_BRANCH',      COUNT(*)::INT FROM dw.dim_branch
UNION ALL
SELECT '---', 'DIM_TIME',        COUNT(*)::INT FROM dw.dim_time
UNION ALL
SELECT '---', 'DIM_PRODUCT',     COUNT(*)::INT FROM dw.dim_product
UNION ALL
SELECT '---', 'DIM_MERCHANT',    COUNT(*)::INT FROM dw.dim_merchant
UNION ALL
SELECT '---', 'DIM_CHANNEL',     COUNT(*)::INT FROM dw.dim_channel
UNION ALL
SELECT 'FACTS', '', 0 WHERE FALSE
UNION ALL
SELECT '===', 'FACT_DEBT_STATUS',         COUNT(*)::INT FROM dw.fact_debt_status
UNION ALL
SELECT '===', 'FACT_EWALLET_TRANSACTION', COUNT(*)::INT FROM dw.fact_ewallet_transaction
UNION ALL
SELECT '===', 'FACT_PAYMENT_TRANSACTION', COUNT(*)::INT FROM dw.fact_payment_transaction;

-- ===========================================================
-- BƯỚC 4: ANALYTIC QUERIES TỔNG HỢP 3 PRODUCT LINES
-- ===========================================================

-- 4A. Tổng doanh thu / volume theo product line
SELECT
    'Lending'   AS product_line,
    COUNT(*)    AS record_count,
    SUM(total_outstanding)      AS total_value,
    NULL::NUMERIC               AS total_fee
FROM dw.fact_debt_status
UNION ALL
SELECT
    'E-Wallet',
    COUNT(*),
    SUM(txn_amount),
    SUM(fee_amount)
FROM dw.fact_ewallet_transaction
WHERE txn_status = 'SUCCESS'
UNION ALL
SELECT
    'Payment',
    COUNT(*),
    SUM(txn_amount),
    SUM(fee_amount)
FROM dw.fact_payment_transaction
WHERE txn_status = 'SUCCESS';

-- 4B. Hoạt động theo chi nhánh — cả 3 product lines
SELECT
    db.branch_name,
    db.region_name,
    COALESCE(l.loan_count,  0)  AS lending_records,
    COALESCE(e.ew_count,    0)  AS ewallet_txns,
    COALESCE(p.pay_count,   0)  AS payment_txns,
    COALESCE(l.loan_count,0) + COALESCE(e.ew_count,0) + COALESCE(p.pay_count,0) AS total_activity
FROM dw.dim_branch db
LEFT JOIN (SELECT branch_key, COUNT(*) AS loan_count FROM dw.fact_debt_status         GROUP BY branch_key) l ON db.branch_key = l.branch_key
LEFT JOIN (SELECT branch_key, COUNT(*) AS ew_count   FROM dw.fact_ewallet_transaction  GROUP BY branch_key) e ON db.branch_key = e.branch_key
LEFT JOIN (SELECT branch_key, COUNT(*) AS pay_count  FROM dw.fact_payment_transaction  GROUP BY branch_key) p ON db.branch_key = p.branch_key
ORDER BY total_activity DESC;

-- 4C. Phân tích customer theo số sản phẩm sử dụng
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM dw.v_customer_product_activity
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- 4D. E-wallet: top 5 loại giao dịch theo volume
SELECT
    txn_type,
    COUNT(*)            AS txn_count,
    SUM(txn_amount)     AS total_amount,
    ROUND(AVG(txn_amount)::NUMERIC, 0) AS avg_amount,
    SUM(fee_amount)     AS total_fee
FROM dw.fact_ewallet_transaction
WHERE txn_status = 'SUCCESS'
GROUP BY txn_type
ORDER BY total_amount DESC;

-- 4E. Payment: doanh thu theo merchant category
SELECT
    merchant_category,
    COUNT(*)                        AS txn_count,
    SUM(txn_amount)                 AS gross_amount,
    SUM(fee_amount)                 AS total_fee,
    SUM(refund_amount)              AS total_refund,
    ROUND(SUM(fee_amount)*100.0/NULLIF(SUM(txn_amount),0), 3) AS fee_rate_pct
FROM dw.fact_payment_transaction
WHERE txn_status IN ('SUCCESS','REFUNDED')
GROUP BY merchant_category
ORDER BY gross_amount DESC;

-- 4F. Lending: NPL ratio theo region
SELECT * FROM dw.v_npl_by_branch_month
WHERE year = 2024
ORDER BY npl_ratio_pct DESC
LIMIT 10;

-- 4G. Monthly trend — tất cả 3 product lines
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    COALESCE(l.outstanding,   0) AS lending_outstanding,
    COALESCE(e.ew_volume,     0) AS ewallet_volume,
    COALESCE(p.pay_volume,    0) AS payment_volume
FROM dw.dim_time dt
LEFT JOIN (
    SELECT time_key, SUM(total_outstanding) AS outstanding
    FROM dw.fact_debt_status GROUP BY time_key
) l ON dt.time_key = l.time_key
LEFT JOIN (
    SELECT time_key, SUM(txn_amount) AS ew_volume
    FROM dw.fact_ewallet_transaction WHERE txn_status='SUCCESS' GROUP BY time_key
) e ON dt.time_key = e.time_key
LEFT JOIN (
    SELECT time_key, SUM(txn_amount) AS pay_volume
    FROM dw.fact_payment_transaction WHERE txn_status='SUCCESS' GROUP BY time_key
) p ON dt.time_key = p.time_key
WHERE dt.year = 2024
  AND (l.outstanding IS NOT NULL OR e.ew_volume IS NOT NULL OR p.pay_volume IS NOT NULL)
ORDER BY dt.month;
