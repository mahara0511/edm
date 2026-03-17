-- ============================================================
-- FILE: 05_ddl_ewallet_payment_source.sql
-- PURPOSE: Source (OLTP) tables for E-Wallet & Payment Gateway
-- TARGET: PostgreSQL / DW_EDM
-- ============================================================

-- ==================== SCHEMA SETUP ====================
CREATE SCHEMA IF NOT EXISTS ewallet;
CREATE SCHEMA IF NOT EXISTS payment;

-- ============================================================
-- SCHEMA: ewallet  (Ví điện tử)
-- ============================================================

-- Tài khoản ví
CREATE TABLE IF NOT EXISTS ewallet.account (
    account_id          SERIAL          PRIMARY KEY,
    user_id             VARCHAR(20)     NOT NULL,           -- link to corebank CIF hoặc card national_id
    phone_number        VARCHAR(15)     UNIQUE NOT NULL,
    full_name           VARCHAR(100),
    national_id         VARCHAR(20),
    email               VARCHAR(100),
    account_status      VARCHAR(20)     DEFAULT 'ACTIVE',   -- ACTIVE, SUSPENDED, CLOSED
    kyc_level           VARCHAR(10)     DEFAULT 'BASIC',    -- BASIC, STANDARD, ADVANCED
    branch_code         VARCHAR(10),
    created_at          TIMESTAMP       DEFAULT NOW()
);

-- Số dư ví
CREATE TABLE IF NOT EXISTS ewallet.balance (
    balance_id          SERIAL          PRIMARY KEY,
    account_id          INT             NOT NULL REFERENCES ewallet.account(account_id),
    current_balance     NUMERIC(18,2)   DEFAULT 0,
    available_balance   NUMERIC(18,2)   DEFAULT 0,
    frozen_amount       NUMERIC(18,2)   DEFAULT 0,
    last_updated        TIMESTAMP       DEFAULT NOW()
);

-- Giao dịch ví
CREATE TABLE IF NOT EXISTS ewallet.transaction (
    txn_id              SERIAL          PRIMARY KEY,
    account_id          INT             NOT NULL REFERENCES ewallet.account(account_id),
    txn_type            VARCHAR(30)     NOT NULL,   -- TOP_UP, TRANSFER, WITHDRAW, PAYMENT, REFUND
    txn_amount          NUMERIC(18,2)   NOT NULL,
    fee_amount          NUMERIC(18,2)   DEFAULT 0,
    txn_status          VARCHAR(20)     DEFAULT 'SUCCESS',  -- SUCCESS, FAILED, PENDING
    source_channel      VARCHAR(30),    -- ATM, BANK_TRANSFER, CARD
    receiver_account    VARCHAR(20),    -- nếu là TRANSFER
    description         VARCHAR(255),
    txn_date            TIMESTAMP       DEFAULT NOW(),
    branch_code         VARCHAR(10)
);

-- Nạp tiền (Top-up)
CREATE TABLE IF NOT EXISTS ewallet.topup (
    topup_id            SERIAL          PRIMARY KEY,
    account_id          INT             NOT NULL REFERENCES ewallet.account(account_id),
    amount              NUMERIC(18,2)   NOT NULL,
    source_type         VARCHAR(30),    -- BANK_TRANSFER, ATM, CARD
    source_ref          VARCHAR(50),    -- mã tham chiếu ngân hàng
    status              VARCHAR(20)     DEFAULT 'SUCCESS',
    topup_date          TIMESTAMP       DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ewallet_txn_account   ON ewallet.transaction(account_id);
CREATE INDEX IF NOT EXISTS idx_ewallet_txn_date      ON ewallet.transaction(txn_date);
CREATE INDEX IF NOT EXISTS idx_ewallet_txn_type      ON ewallet.transaction(txn_type);
CREATE INDEX IF NOT EXISTS idx_ewallet_balance_acct  ON ewallet.balance(account_id);


-- ============================================================
-- SCHEMA: payment  (Cổng thanh toán)
-- ============================================================

-- Merchant (đơn vị chấp nhận thanh toán)
CREATE TABLE IF NOT EXISTS payment.merchant (
    merchant_id         SERIAL          PRIMARY KEY,
    merchant_code       VARCHAR(20)     UNIQUE NOT NULL,
    merchant_name       VARCHAR(100)    NOT NULL,
    merchant_category   VARCHAR(50),    -- RETAIL, FOOD, TRAVEL, EDUCATION, HEALTHCARE, etc.
    branch_code         VARCHAR(10),
    region              VARCHAR(50),
    status              VARCHAR(20)     DEFAULT 'ACTIVE',
    registered_at       DATE
);

-- Giao dịch thanh toán
CREATE TABLE IF NOT EXISTS payment.transaction (
    txn_id              SERIAL          PRIMARY KEY,
    merchant_id         INT             NOT NULL REFERENCES payment.merchant(merchant_id),
    customer_ref        VARCHAR(20),    -- CIF hoặc phone/account_id
    txn_amount          NUMERIC(18,2)   NOT NULL,
    fee_amount          NUMERIC(18,2)   DEFAULT 0,
    payment_method      VARCHAR(30)     NOT NULL,   -- EWALLET, CARD, QR_CODE, BANK_TRANSFER
    txn_status          VARCHAR(20)     DEFAULT 'SUCCESS',  -- SUCCESS, FAILED, REFUNDED, DISPUTED
    currency            VARCHAR(5)      DEFAULT 'VND',
    description         VARCHAR(255),
    txn_date            TIMESTAMP       DEFAULT NOW(),
    settlement_date     DATE,
    branch_code         VARCHAR(10)
);

-- Hoàn tiền (Refund)
CREATE TABLE IF NOT EXISTS payment.refund (
    refund_id           SERIAL          PRIMARY KEY,
    original_txn_id     INT             NOT NULL REFERENCES payment.transaction(txn_id),
    refund_amount       NUMERIC(18,2)   NOT NULL,
    refund_reason       VARCHAR(100),
    refund_status       VARCHAR(20)     DEFAULT 'COMPLETED',
    refund_date         TIMESTAMP       DEFAULT NOW()
);

-- Quyết toán (Settlement)
CREATE TABLE IF NOT EXISTS payment.settlement (
    settlement_id       SERIAL          PRIMARY KEY,
    merchant_id         INT             NOT NULL REFERENCES payment.merchant(merchant_id),
    settlement_date     DATE            NOT NULL,
    total_txn_count     INT             DEFAULT 0,
    gross_amount        NUMERIC(18,2)   DEFAULT 0,
    total_fee           NUMERIC(18,2)   DEFAULT 0,
    net_amount          NUMERIC(18,2)   DEFAULT 0,
    status              VARCHAR(20)     DEFAULT 'SETTLED'
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_payment_txn_merchant  ON payment.transaction(merchant_id);
CREATE INDEX IF NOT EXISTS idx_payment_txn_date      ON payment.transaction(txn_date);
CREATE INDEX IF NOT EXISTS idx_payment_txn_method    ON payment.transaction(payment_method);
CREATE INDEX IF NOT EXISTS idx_payment_txn_status    ON payment.transaction(txn_status);
CREATE INDEX IF NOT EXISTS idx_payment_merchant_cat  ON payment.merchant(merchant_category);
