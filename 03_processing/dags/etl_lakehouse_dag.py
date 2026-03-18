from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

"""
PIPELINE ETL CHUẨN (STANDARD ETL PIPELINE):
- Đảm bảo tính Idempotent (Chạy lại DAG không bị lặp dữ liệu bằng DELETE-INSERT theo Date).
- Workflow: Load Dimensions -> Load Facts -> Kiểm thử chất lượng (Data Quality).
"""

with DAG(
    dag_id='lakehouse_daily_etl_standard',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'elt', 'dw'],
) as dag:
    
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # ==========================================
    # 1. LOAD DIMENSIONS (Nạp danh mục)
    # ==========================================
    
    # Sinh Dim Time tự động nếu chưa có
    load_dim_time = PostgresOperator(
        task_id='load_dim_time',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            INSERT INTO dw.dim_time (full_date, day, month, quarter, year, day_name, month_name, is_weekend)
            SELECT 
                datum_date, EXTRACT(DAY FROM datum_date), EXTRACT(MONTH FROM datum_date), 
                EXTRACT(QUARTER FROM datum_date), EXTRACT(YEAR FROM datum_date),
                TO_CHAR(datum_date, 'Day'), TO_CHAR(datum_date, 'Month'),
                CASE WHEN EXTRACT(ISODOW FROM datum_date) IN (6, 7) THEN 'true' ELSE 'false' END
            FROM (SELECT generate_series(DATE('2023-01-01'), DATE('2026-12-31'), '1 day'::interval) AS datum_date) AS t
            WHERE NOT EXISTS (SELECT 1 FROM dw.dim_time WHERE full_date = t.datum_date);
        """
    )

    load_dim_customer = PostgresOperator(
        task_id='load_dim_customer',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            -- 1. Nạp từ Corebank
            INSERT INTO dw.dim_customer (cif_number, full_name, national_id, phone_number, current_address, effective_date, is_current)
            SELECT c.cif, c.full_name, c.national_id, c.phone, c.current_address, CURRENT_DATE, TRUE
            FROM corebank.customer c
            WHERE NOT EXISTS (SELECT 1 FROM dw.dim_customer WHERE cif_number = c.cif);

            -- 2. Bổ sung từ Realtime nếu chưa có
            INSERT INTO dw.dim_customer (cif_number, full_name, effective_date, is_current)
            SELECT DISTINCT CAST(account_id AS VARCHAR), 'Customer ' || account_id, CURRENT_DATE, TRUE
            FROM public.realtime_transactions t
            WHERE NOT EXISTS (SELECT 1 FROM dw.dim_customer WHERE cif_number = CAST(t.account_id AS VARCHAR));
        """
    )
    
    load_dim_branch = PostgresOperator(
        task_id='load_dim_branch',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            INSERT INTO dw.dim_branch (branch_code, branch_name)
            SELECT DISTINCT branch_code, 'Branch ' || branch_code
            FROM corebank.mortgage_loan ml
            WHERE NOT EXISTS (SELECT 1 FROM dw.dim_branch WHERE branch_code = ml.branch_code);

            -- Mặc định cho Realtime
            INSERT INTO dw.dim_branch (branch_code, branch_name)
            SELECT 'MAIN', 'Main Office'
            WHERE NOT EXISTS (SELECT 1 FROM dw.dim_branch WHERE branch_code = 'MAIN');
        """
    )

    load_dim_merchant = PostgresOperator(
        task_id='load_dim_merchant',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            INSERT INTO dw.dim_merchant (merchant_code, merchant_name, merchant_category)
            SELECT DISTINCT 
                SUBSTRING(UPPER(REPLACE(merchant, ' ', '_')) FROM 1 FOR 20),
                merchant, 
                'General'
            FROM public.realtime_transactions t
            WHERE NOT EXISTS (SELECT 1 FROM dw.dim_merchant WHERE merchant_name = t.merchant);
        """
    )

    # ==========================================
    # 2. LOAD FACTS (Nạp dữ liệu giao dịch phát sinh)
    # ==========================================
    
    load_fact_lending = PostgresOperator(
        task_id='load_fact_lending',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            DELETE FROM dw.fact_debt_status f
            USING dw.dim_time dt
            WHERE f.time_key = dt.time_key AND dt.full_date = '{{ ds }}';

            INSERT INTO dw.fact_debt_status (customer_key, branch_key, product_key, time_key, principal_amount, total_outstanding, risk_bucket)
            SELECT 
                COALESCE(dim_c.customer_key, 1), 
                COALESCE(dim_b.branch_key, 1), 
                1, dim_t.time_key, ml.principal_amount, ml.total_outstanding, 'CURRENT'
            FROM corebank.mortgage_loan ml
            LEFT JOIN dw.dim_customer dim_c ON ml.cif = dim_c.cif_number
            LEFT JOIN dw.dim_branch dim_b ON ml.branch_code = dim_b.branch_code
            INNER JOIN dw.dim_time dim_t ON ml.disbursement_date = dim_t.full_date
            WHERE ml.disbursement_date = '{{ ds }}';
        """
    )

    load_fact_ewallet = PostgresOperator(
        task_id='load_fact_ewallet',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            DELETE FROM dw.fact_ewallet_transaction f
            USING dw.dim_time dt
            WHERE f.time_key = dt.time_key AND dt.full_date = '{{ ds }}';

            INSERT INTO dw.fact_ewallet_transaction (customer_key, branch_key, time_key, channel_key, txn_amount, fee_amount, txn_type, txn_status)
            SELECT 
                COALESCE(dim_c.customer_key, 1), 1, dim_t.time_key, 1, t.txn_amount, t.fee_amount, t.txn_type, t.txn_status
            FROM ewallet.transaction t
            LEFT JOIN ewallet.account acc ON t.account_id = acc.account_id
            LEFT JOIN dw.dim_customer dim_c ON acc.user_id = dim_c.cif_number
            INNER JOIN dw.dim_time dim_t ON DATE(t.txn_date) = dim_t.full_date
            WHERE DATE(t.txn_date) = '{{ ds }}';
        """
    )

    load_fact_realtime_payments = PostgresOperator(
        task_id='load_fact_realtime_payments',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            -- Idempotent: Xóa sạch dữ liệu của ngày chạy để nạp lại
            DELETE FROM dw.fact_payment_transaction f
            USING dw.dim_time dt
            WHERE f.time_key = dt.time_key AND dt.full_date = '{{ ds }}';

            -- Nạp từ public.realtime_transactions (Bronze Layer) sang dw schema (Gold Layer)
            INSERT INTO dw.fact_payment_transaction (customer_key, merchant_key, branch_key, time_key, channel_key, txn_count, txn_amount, net_amount, txn_status)
            SELECT 
                COALESCE(dc.customer_key, 1),
                COALESCE(dm.merchant_key, 1),
                COALESCE(db.branch_key, 1),
                dt.time_key,
                1, -- Kênh Mobile mặc định
                1, -- Số lượng transaction
                rt.amount,
                rt.amount, -- Giả sử không có phí
                'SUCCESS'
            FROM public.realtime_transactions rt
            JOIN dw.dim_customer dc ON dc.cif_number = CAST(rt.account_id AS VARCHAR)
            JOIN dw.dim_time dt ON dt.full_date = DATE(rt.timestamp)
            LEFT JOIN dw.dim_merchant dm ON dm.merchant_name = rt.merchant
            LEFT JOIN dw.dim_branch db ON db.branch_code = 'MAIN'
            WHERE DATE(rt.timestamp) = '{{ ds }}';
        """
    )

    # ==========================================
    # 3. DATA QUALITY CHECKS (Kiểm định)
    # ==========================================
    
    dq_check = PostgresOperator(
        task_id='dq_checks',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            -- Đảm bảo không có data nào bị NULL key quan trọng
            SELECT 
                1 / CASE WHEN count(*) = 0 THEN 1 ELSE 0 END
            FROM (
                SELECT 1 FROM dw.fact_payment_transaction WHERE customer_key IS NULL
                UNION ALL
                SELECT 1 FROM dw.fact_debt_status WHERE customer_key IS NULL
                UNION ALL
                SELECT 1 FROM dw.fact_ewallet_transaction WHERE customer_key IS NULL
            ) AS failures;
        """
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Dependency Flow
    start_pipeline >> [load_dim_time, load_dim_customer, load_dim_branch, load_dim_merchant]
    [load_dim_time, load_dim_customer, load_dim_branch, load_dim_merchant] >> load_fact_lending
    [load_dim_time, load_dim_customer, load_dim_branch, load_dim_merchant] >> load_fact_ewallet
    [load_dim_time, load_dim_customer, load_dim_branch, load_dim_merchant] >> load_fact_realtime_payments
    [load_fact_lending, load_fact_ewallet, load_fact_realtime_payments] >> dq_check >> end_pipeline
