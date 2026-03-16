from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse_admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_start_process():
    print("Bắt đầu quá trình ETL: Đọc dữ liệu từ Schema [Corebank, EWallet, Payment] -> Nạp vào Schema [DW]")

with DAG(
    dag_id='lakehouse_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'elt'],
    description='DAG mô phỏng quá trình Extract-Load-Transform đưa từ Raw Storage sang DW'
) as dag:
    
    start_pipeline = PythonOperator(
        task_id='start_pipeline',
        python_callable=log_start_process
    )
    
    # Minh hoạ bước Transform bằng SQL (Sau này bạn sẽ viết logic Join thật tại đây)
    transform_lending_data = PostgresOperator(
        task_id='transform_lending_data',
        postgres_conn_id='postgres_lakehouse', # Connection ID này cần được cấu hình trong Airflow UI
        sql="""
            -- CHÚ Ý: Đây là mã giả (mock query) minh hoạ cách Airflow gọi CSDL.
            -- Logic thật: INSERT INTO dw.fact_debt_status SELECT ... FROM corebank.mortgage_loan;
            SELECT count(*) as count_loans FROM corebank.mortgage_loan;
        """
    )

    transform_ewallet_payment = PostgresOperator(
        task_id='transform_ewallet_payment',
        postgres_conn_id='postgres_lakehouse',
        sql="""
            -- Minh hoạ logic map data từ ewallet sang DW
            SELECT count(*) as total_ewallet FROM ewallet.transaction;
        """
    )
    
    verify_data_quality = EmptyOperator(
        task_id='verify_data_quality'
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline'
    )

    # Dependency Flow (Quy định luồng chạy DAG)
    start_pipeline >> [transform_lending_data, transform_ewallet_payment] >> verify_data_quality >> end_pipeline
