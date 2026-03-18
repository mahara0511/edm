# Lakehouse Architecture Demo - Full Flow Guide

Welcome to the end-to-end Local Lakehouse Data Engineering Project! This repository simulates a real-world Big Data & Machine Learning Pipeline transitioning streaming/batch data into a Star Schema Data Warehouse and Serving Layers.

## System Architecture

The project is structured according to the **Medallion Architecture**, divided closely by responsibility:

- `01_data_sources`: Where "Fake" bank and e-wallet events are injected.
- `02_ingestion`: Kafka Event Streaming Bus.
- `03_processing`: Apache Spark (Real-time stream) and Apache Airflow (Daily ETL Batch load).
- `04_storage`: MinIO (Cold Data Lake S3) and PostgreSQL (Hot Data Warehouse).
- `05_serving`: Metabase (BI Dashboard), Jupyter Notebook (ML Engine), FastAPI (Machine Learning Serving Endpoint).

---

## Prerequisite
- **Windows / Linux OS** with **Docker & Docker Compose** installed.
- **Python 3.10+** (Installed locally on your host OS) to generate fake streaming events.
- JDK 11+ and Apache Spark configured to run `spark-submit`.

---

## How to run the Full Flow

### Step 1: Initialize The Platform Infrastructure
To easily wake up the entire Data Platform, open your terminal (PowerShell) and run:
```powershell
.\start_realtime_demo.ps1
```
This script will sequentially boot up Kafka, PostgreSQL, MinIO, Metabase, Jupyter, and FastAPI.

### Step 2: Stream Data to Storage (Lake & DW)
You will now spin up an Apache Spark engine inside our worker container to consume Kafka events in real-time.
Open a **new terminal window** and run:
```powershell
docker exec lakehouse-spark-worker /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/spark/jobs/streaming_job.py
```
*(Spark will start connecting to S3 MinIO and wait for Kafka streams).*

### Step 3: Start Generating Bank Events (Source)
Open a **third terminal** inside the root directory and start the data spigot:
```powershell
cd 01_data_sources
.\venv\Scripts\Activate.ps1
pip install kafka-python faker
python generate_realtime_events.py
```
> The generator will start pushing JSON events representing payments, transfers, and high-risk bank withdrawals into the pipeline.

---

## Step 4: Interact with The Serving Layer

Now that data is flowing seamlessly from End to End, you can access the powerful services deployed:

### 1. BI Dashboard - Metabase (`http://localhost:3000`)
- Use Metabase to create live visual analytics.
- **Connect DB:** Type `PostgreSQL` | Host `host.docker.internal` | Port `5433` | Database `dw_edm` | User `edm_user` | Pass `edm_pass`.
- View dynamic SQL charts updating as new Kafka transactions sink in.

### 2. Machine Learning Engine - Jupyter Notebook (`http://localhost:8888`)
- The primary code-base for Data Scientists.
- Password is: `admin`
- Write Python code here to load the massive history volumes spanning S3 `s3a://data-lake/raw/transactions/`.

### 3. AI Serving & Live Monitor - Streamlit App (`http://localhost:8501`)
- Open **http://localhost:8501** in your web browser.
- A beautiful, interactive **End-to-End Streamlit Dashboard** lets you:
  - View the Real-time Database Sink updating live from Kafka/Spark.
  - Test the FastAPI Fraud Detection Neural Network by filling out a payment form and evaluating the Real-time Risk Score Percentage computed by our Random Forest model.

### 4. Model Serving API - FastAPI (`http://localhost:8000`)
- The backend Machine Learning API that powers the Streamlit inference.
- Head to **http://localhost:8000/docs** to see the Swagger UI.

### 5. Run Batch Data Warehouse Pipelines (Airflow)
- Access **http://localhost:8080** (admin/admin).
- Turn on the `.dag` file called `lakehouse_daily_etl_standard`.
- It executes a standard batch process mimicking a robust corporate data pipeline mapping raw Sources directly into the structured Data Warehouse Star Schema!

---

## Troubleshooting

**Download failed java.lang.RuntimeException for Spark Packages**:
- **Why it happens**: This occurs if the `spark-submit` command is forcefully terminated (e.g. by pressing `Ctrl + C`) while it is actively downloading jars from the Maven repository (the first ~20 seconds). The half-downloaded jars corrupt the Ivy cache.
- **How to fix it**: Run the following command to clear the broken cache inside the Spark container before trying again:
  ```powershell
  docker exec lakehouse-spark-worker rm -rf /root/.ivy2
  ```
- **How to prevent it**: Resist the urge to press `Ctrl + C` during the download phase. You should only terminate the streaming process AFTER the text `Bắt đầu lắng nghe Spark Streaming từ Kafka topic...` appears on the console. At this point, the download is 100% complete and safe to exit anytime.

---
*Happy Engineering!*
