from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'enterprise_admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

"""
FINAL ENTERPRISE LAKEHOUSE PIPELINE (CLEANED VERSION):
- Real-time: PAYMENT + EWALLET (Kafka -> Spark -> DW).
- Batch: LENDING (Corebank -> DW).
- No more Batch E-wallet (Removed to avoid duplication).
"""

with DAG(
    dag_id='enterprise_edw_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['edw', 'enterprise', 'realtime_vs_batch'],
) as dag:
    
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # 1. マスターデータ (MASTER DATA)
    load_dim_customer = PostgresOperator(
        task_id='load_dim_customer',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            TRUNCATE dw.dim_customer RESTART IDENTITY CASCADE;
            TRUNCATE dw.mdm_deduplication_log RESTART IDENTITY CASCADE;

            -- 1. Unknown record với SK=1
            INSERT INTO dw.dim_customer (customer_sk, national_id, phone_number, full_name, is_wallet_user, is_borrower, kyc_level)
            VALUES (1, 'N/A', 'N/A', 'General/Unknown/N/A', false, false, 'TIER_0')
            ON CONFLICT (customer_sk) DO NOTHING;

            -- Reset sequence để ID tiếp theo là 2
            SELECT setval(pg_get_serial_sequence('dw.dim_customer', 'customer_sk'), 1, true);

            WITH 
            cleansed_bank AS (
                SELECT 
                    cif, 
                    national_id, 
                    REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as clean_phone, 
                    UPPER(TRIM(full_name)) as full_name,
                    'TIER_2' as kyc_level  -- Bank mặc định là TIER_2
                FROM corebank.customer
            ),
            -- 2. Cleanse Wallet Data
            cleansed_wallet AS (
                SELECT 
                    user_id, 
                    national_id, 
                    REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g') as clean_phone, 
                    UPPER(TRIM(full_name)) as full_name,
                    kyc_level
                FROM ewallet.account
            ),
            -- 3. Identity Resolution (Dùng COALESCE để làm Hash-Joinable)
            identities AS (
                SELECT 
                    COALESCE(b.national_id, w.national_id, 'PH_' || w.clean_phone) as master_id,
                    COALESCE(b.clean_phone, w.clean_phone) as master_phone,
                    COALESCE(b.full_name, w.full_name) as master_name,
                    b.cif as bank_cif,
                    w.user_id as wallet_user_id,
                    (w.user_id IS NOT NULL) as is_wallet,
                    (b.cif IS NOT NULL) as is_borrower,
                    COALESCE(b.kyc_level, w.kyc_level) as master_kyc
                FROM cleansed_bank b
                FULL OUTER JOIN cleansed_wallet w 
                    ON COALESCE(b.national_id, 'P_' || b.clean_phone) = COALESCE(w.national_id, 'P_' || w.clean_phone)
            )
            INSERT INTO dw.dim_customer (national_id, phone_number, full_name, is_wallet_user, is_borrower, kyc_level, bank_cif, wallet_user_id)
            SELECT 
                master_id, master_phone, master_name, is_wallet, is_borrower, master_kyc,
                bank_cif, wallet_user_id
            FROM identities;

            -- 5. MDM Log
            INSERT INTO dw.mdm_deduplication_log (surviving_customer_sk, merged_source_id, confidence_score)
            SELECT dc.customer_sk, dc.wallet_user_id, 0.95
            FROM dw.dim_customer dc
            WHERE dc.bank_cif IS NOT NULL AND dc.wallet_user_id IS NOT NULL;
        """
    )
    
    load_dim_merchant = PostgresOperator(
        task_id='load_dim_merchant',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            TRUNCATE dw.dim_merchant RESTART IDENTITY CASCADE;
            INSERT INTO dw.dim_merchant (merchant_sk, source_merchant_id, merchant_name) 
            VALUES (1, 'NA', 'General/Unknown') ON CONFLICT DO NOTHING;
            
            -- Reset sequence để ID tiếp theo là 2
            SELECT setval(pg_get_serial_sequence('dw.dim_merchant', 'merchant_sk'), 1, true);

            INSERT INTO dw.dim_merchant (source_merchant_id, merchant_name)
            SELECT DISTINCT merchant_name, merchant_name FROM public.realtime_transactions
            ON CONFLICT DO NOTHING;
        """
    )

    load_dim_product_account = PostgresOperator(
        task_id='load_dim_product_account',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            TRUNCATE dw.dim_product_account RESTART IDENTITY CASCADE;
            INSERT INTO dw.dim_product_account (product_sk, customer_sk, product_type, source_account_id, status)
            VALUES (1, 1, 'N/A', 'N/A', 'N/A') ON CONFLICT DO NOTHING;
            
            -- Reset sequence để ID tiếp theo là 2
            SELECT setval(pg_get_serial_sequence('dw.dim_product_account', 'product_sk'), 1, true);

            INSERT INTO dw.dim_product_account (customer_sk, product_type, source_account_id, status)
            SELECT dc.customer_sk, 'WALLET_ACCOUNT', acc.account_id, acc.account_status
            FROM ewallet.account acc JOIN dw.dim_customer dc ON dc.wallet_user_id = acc.user_id;

            INSERT INTO dw.dim_product_account (customer_sk, product_type, source_account_id, status)
            SELECT dc.customer_sk, 'MORTGAGE_LOAN', ml.branch_code || '_' || ml.cif, 'ACTIVE'
            FROM corebank.mortgage_loan ml JOIN dw.dim_customer dc ON dc.bank_cif = ml.cif;
        """
    )

    # 2. EVENTS (UNIFIED TRANSACTIONS)
    clear_fact = PostgresOperator(task_id='clear_fact_today', postgres_conn_id='postgres_lakehouse', sql="DELETE FROM dw.fact_enterprise_transaction WHERE DATE(trans_timestamp) = '{{ ds }}';")

    # Kafka nạp Real-time (PAYMENT & EWALLET)
    load_fact_kafka = PostgresOperator(
        task_id='load_fact_kafka',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            INSERT INTO dw.fact_enterprise_transaction (customer_sk, merchant_sk, product_sk, source_system, source_trans_id, base_amount, currency_code, trans_timestamp, status)
            SELECT 
                COALESCE(dc.customer_sk, 1),
                COALESCE(dm.merchant_sk, 1),
                1 as product_sk, -- Gán tạm 1 để đảm bảo nạp thành công
                rt.source_system, rt.event_id, rt.base_amount, rt.currency, rt.timestamp::TIMESTAMP, 'COMPLETED'
            FROM public.realtime_transactions rt
            LEFT JOIN dw.dim_customer dc ON (
                (rt.source_system = 'PAYMENT' AND dc.bank_cif = rt.cif) OR 
                (rt.source_system = 'EWALLET' AND dc.wallet_user_id = rt.wallet_id)
            )
            LEFT JOIN dw.dim_merchant dm ON (rt.merchant_name = dm.merchant_name);
        """
    )

    # Corebank (LENDING - Batch only)
    load_fact_lending = PostgresOperator(
        task_id='load_fact_lending',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            INSERT INTO dw.fact_enterprise_transaction (customer_sk, product_sk, source_system, source_trans_id, base_amount, currency_code, trans_timestamp, status)
            SELECT dc.customer_sk, pa.product_sk, 'COREBANK', 'LND_' || ml.branch_code, ml.principal_amount, 'VND', ml.disbursement_date::TIMESTAMP, 'DISBURSED'
            FROM corebank.mortgage_loan ml JOIN dw.dim_customer dc ON dc.bank_cif = ml.cif
            JOIN dw.dim_product_account pa ON pa.customer_sk = dc.customer_sk AND pa.product_type = 'LOAN_CONTRACT'
            WHERE ml.disbursement_date = '{{ ds }}';
        """
    )

    # 3. DQ MONITORING
    dq_check = PostgresOperator(
        task_id='dq_monitor',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            INSERT INTO dw.dq_quarantine_log (source_system, raw_payload, error_type)
            SELECT source_system, row_to_json(f), 'INVALID_AMOUNT'
            FROM dw.fact_enterprise_transaction f
            WHERE base_amount <= 0;
        """
    )

    # Workflow
    start_pipeline >> [load_dim_customer, load_dim_merchant] >> load_dim_product_account >> clear_fact
    clear_fact >> [load_fact_kafka, load_fact_lending] >> dq_check
