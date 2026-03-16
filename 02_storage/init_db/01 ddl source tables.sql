-- ============================================================
-- FILE: 01_ddl_source_tables.sql
-- PURPOSE: Create source (OLTP) tables for Core Banking & Core Card
-- TARGET: PostgreSQL
-- ============================================================

-- ==================== SCHEMA SETUP ====================
CREATE SCHEMA IF NOT EXISTS corebank;
CREATE SCHEMA IF NOT EXISTS card;

-- ==================== CORE BANKING ====================

CREATE TABLE IF NOT EXISTS corebank.customer (
    cif             VARCHAR(20)     PRIMARY KEY,
    full_name       VARCHAR(100)    NOT NULL,
    national_id     VARCHAR(20)     UNIQUE NOT NULL,
    phone           VARCHAR(15),
    email           VARCHAR(100),
    current_address VARCHAR(255),
    created_at      TIMESTAMP       DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS corebank.mortgage_loan (
    contract_no         VARCHAR(30)     PRIMARY KEY,
    cif                 VARCHAR(20)     NOT NULL REFERENCES corebank.customer(cif),
    branch_code         VARCHAR(10)     NOT NULL,
    product_type        VARCHAR(50)     NOT NULL,  -- e.g. 'MORTGAGE', 'HOME_EQUITY'
    principal_amount    NUMERIC(18,2)   NOT NULL,
    interest_rate       NUMERIC(5,4)    NOT NULL,  -- e.g. 0.0875 = 8.75%
    total_outstanding   NUMERIC(18,2),
    disbursement_date   DATE            NOT NULL,
    maturity_date       DATE            NOT NULL,
    status              VARCHAR(20)     DEFAULT 'ACTIVE'  -- ACTIVE, CLOSED, DEFAULT
);

CREATE TABLE IF NOT EXISTS corebank.loan_detail (
    id                      SERIAL          PRIMARY KEY,
    contract_no             VARCHAR(30)     NOT NULL REFERENCES corebank.mortgage_loan(contract_no),
    installment_due_date    DATE            NOT NULL,
    installment_amount      NUMERIC(18,2)   NOT NULL,
    principal_component     NUMERIC(18,2),
    interest_component      NUMERIC(18,2),
    last_payment_date       DATE,
    last_payment_amount     NUMERIC(18,2),
    status                  VARCHAR(20)     DEFAULT 'PENDING'  -- PENDING, PAID, OVERDUE
);

-- ==================== CORE CARD ====================

CREATE TABLE IF NOT EXISTS card.user (
    user_id         SERIAL          PRIMARY KEY,
    national_id     VARCHAR(20)     UNIQUE NOT NULL,
    mobile_number   VARCHAR(15),
    email_address   VARCHAR(100),
    full_name       VARCHAR(100),
    created_at      TIMESTAMP       DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS card.card_account (
    card_no             VARCHAR(20)     PRIMARY KEY,
    user_id             INT             NOT NULL REFERENCES card.user(user_id),
    branch_code         VARCHAR(10)     NOT NULL,
    product_type        VARCHAR(50)     NOT NULL,  -- e.g. 'VISA_PLATINUM', 'MASTERCARD_GOLD'
    credit_limit        NUMERIC(18,2)   NOT NULL,
    current_balance     NUMERIC(18,2)   DEFAULT 0,
    available_credit    NUMERIC(18,2),
    open_date           DATE,
    status              VARCHAR(20)     DEFAULT 'ACTIVE'
);

CREATE TABLE IF NOT EXISTS card.statement (
    statement_id        SERIAL          PRIMARY KEY,
    card_no             VARCHAR(20)     NOT NULL REFERENCES card.card_account(card_no),
    statement_date      DATE            NOT NULL,
    payment_due_date    DATE            NOT NULL,
    closing_balance     NUMERIC(18,2),
    minimum_amount      NUMERIC(18,2),
    last_payment_date   DATE,
    last_payment_amount NUMERIC(18,2),
    days_past_due       INT             DEFAULT 0
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_mortgage_loan_cif         ON corebank.mortgage_loan(cif);
CREATE INDEX IF NOT EXISTS idx_mortgage_loan_branch      ON corebank.mortgage_loan(branch_code);
CREATE INDEX IF NOT EXISTS idx_loan_detail_contract      ON corebank.loan_detail(contract_no);
CREATE INDEX IF NOT EXISTS idx_card_account_user         ON card.card_account(user_id);
CREATE INDEX IF NOT EXISTS idx_card_account_branch       ON card.card_account(branch_code);
CREATE INDEX IF NOT EXISTS idx_statement_card            ON card.statement(card_no);
CREATE INDEX IF NOT EXISTS idx_statement_date            ON card.statement(statement_date);
