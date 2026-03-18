# Script to launch the Real-Time Streaming & Batch Lakehouse architecture according to Concept Image.

$ErrorActionPreference = 'Stop'

Write-Host "=========================================================="
Write-Host "STEP 1: START REAL-TIME STREAMING SYSTEM (KAFKA)"
Write-Host "=========================================================="
cd 02_ingestion
docker compose up -d
cd ..

Write-Host "`n=========================================================="
Write-Host "STEP 2: START WARM/COLD STORAGE SYSTEM (Postgres, MinIO)"
Write-Host "=========================================================="
cd 04_storage
docker compose up -d
cd ..

Write-Host "`n[Waiting 15s for Database and Kafka Broker to stabilize...]`n"
Start-Sleep -Seconds 15

Write-Host "=========================================================="
Write-Host "STEP 2.5: START PROCESSING LAYER (Airflow & Spark)"
Write-Host "=========================================================="
cd 03_processing
docker compose up -d
cd ..

Write-Host "=========================================================="
Write-Host "STEP 3: START SERVING LAYER (Metabase & Jupyter Notebook)"
Write-Host "=========================================================="
cd 05_serving
docker compose up -d
cd ..

Write-Host "`n=========================================================="
Write-Host "                 SUPER DEMO READY!"
Write-Host "=========================================================="
Write-Host "Services exposed on Localhost:"
Write-Host "1. Kafka (Real-time Broker)       : Port 29092"
Write-Host "2. Kafka Debezium CDC (Connector) : Port 8083"
Write-Host "3. Postgres (Warm Storage DB)     : Port 5433"
Write-Host "4. Data Lake MinIO (Cold Storage) : Port 9001"
Write-Host "5. Metabase (BI Dashboard)        : Port 3000"
Write-Host "6. Jupyter Notebook (Jupyter ML)  : Port 8888 (Password: admin)"
Write-Host "7. FastAPI (Model Serving)        : Port 8000"
Write-Host "8. Streamlit (End-to-End Demo)    : Port 8501"
Write-Host "9. Keycloak (Auth/JWT)            : Port 8081 (Admin: admin/admin)"
Write-Host ""
Write-Host "--> TO DEMO STREAMING HOT DATA (Manual):"
Write-Host "1) Open Terminal 1: cd 01_data_sources; .\venv\Scripts\Activate.ps1; pip install kafka-python faker; python generate_realtime_events.py"
Write-Host "2) Open Terminal 2: docker exec lakehouse-spark-worker /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/jobs/streaming_job.py"
Write-Host "=========================================================="
