# EDM-ETL: Enterprise Data Lakehouse Pipeline

Du an mo phong mot **Enterprise Data Warehouse (EDW)** theo kien truc Medallion, tich hop luong du lieu thoi gian thuc (Kafka + Spark Streaming) va xu ly Batch (Apache Airflow) vao mot kho du lieu trung tam duy nhat (PostgreSQL).

---

## Kien truc tong the

```
[Corebank / Ewallet OLTP]
        |
        v
[01] Kafka Producer (generate_realtime_events.py)
        |  topic: realtime_transactions
        v
[02] Kafka Broker (localhost:29092)
        |
        v
[03] Spark Streaming (streaming_job.py)
        |  -> public.realtime_transactions (Landing Zone)
        v
[03] Apache Airflow DAG: enterprise_edw_etl_pipeline
        |  -> dw.dim_customer  (MDM Identity Resolution)
        |  -> dw.dim_merchant
        |  -> dw.dim_product_account
        |  -> dw.fact_enterprise_transaction
        v
[05] Serving Layer
        |  -> Metabase   (BI Dashboard)     :3000
        |  -> Jupyter    (ML Notebook)      :8888
        |  -> Streamlit  (Live Dashboard)   :8501
        |  -> FastAPI    (ML API)           :8000
```

---

## Yeu cau cai dat (Prerequisites)

- Docker Desktop (da bat WSL2 tren Windows)
- Python 3.10+ (cai tren may host, khong can trong Docker)
- Thu vien Python: `kafka-python`, `faker`

```powershell
pip install kafka-python faker
```

---

## CHAY LAN DAU (First-Time Setup)

Luu y: Thuc hien dung thu tu tu Buoc 1 den Buoc 5. Khong bo qua buoc nao.

### Buoc 1: Khoi dong ha tang Kafka + Zookeeper

```powershell
cd 02_ingestion
docker-compose up -d
```

Kiem tra Kafka da san sang:
```powershell
docker logs lakehouse-kafka | Select-String "started"
```

### Buoc 2: Khoi dong Postgres + Spark + Airflow

```powershell
cd 03_processing
docker-compose up -d
```

Cho khoang 30 giay de Airflow khoi dong hoan toan, sau do kiem tra:
```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Buoc 3: Khoi tao Database Schema (Chi chay MOT LAN duy nhat)

**Buoc 3.1** - Tao cac bang Source (Corebank, Ewallet, Landing Zone):
```powershell
Get-Content "04_storage\init_db\01 ddl source tables.sql" | docker exec -i lakehouse-postgres psql -U edm_user -d dw_edm
```

**Buoc 3.2** - Tao cac bang E-wallet va Payment Source:
```powershell
Get-Content "04_storage\init_db\05 ddl ewallet payment source.sql" | docker exec -i lakehouse-postgres psql -U edm_user -d dw_edm
```

**Buoc 3.3** - Tao Schema Warehouse EDM (Dimension + Fact):
```powershell
$sql = @"
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    national_id     VARCHAR(20),
    phone_number    VARCHAR(20),
    full_name       VARCHAR(100),
    is_wallet_user  BOOLEAN DEFAULT false,
    is_borrower     BOOLEAN DEFAULT false,
    kyc_level       VARCHAR(20),
    bank_cif        VARCHAR(20),
    wallet_user_id  VARCHAR(50),
    CONSTRAINT uq_dim_customer_sk UNIQUE (customer_sk)
);

CREATE TABLE IF NOT EXISTS dw.dim_merchant (
    merchant_sk         SERIAL PRIMARY KEY,
    source_merchant_id  VARCHAR(50) UNIQUE,
    merchant_name       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dw.dim_product_account (
    product_sk          SERIAL PRIMARY KEY,
    customer_sk         INT,
    product_type        VARCHAR(50),
    source_account_id   VARCHAR(50),
    status              VARCHAR(20),
    CONSTRAINT uq_product_sk UNIQUE (product_sk)
);

CREATE TABLE IF NOT EXISTS dw.fact_enterprise_transaction (
    fact_id         SERIAL PRIMARY KEY,
    customer_sk     INT  DEFAULT 1,
    merchant_sk     INT  DEFAULT 1,
    product_sk      INT  DEFAULT 1,
    source_system   VARCHAR(20),
    source_trans_id VARCHAR(50),
    base_amount     NUMERIC(18,2),
    currency_code   VARCHAR(10),
    trans_timestamp TIMESTAMP,
    status          VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dw.mdm_deduplication_log (
    log_id                  SERIAL PRIMARY KEY,
    surviving_customer_sk   INT,
    merged_source_id        VARCHAR(50),
    confidence_score        NUMERIC(5,2),
    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw.dq_quarantine_log (
    log_id          SERIAL PRIMARY KEY,
    source_system   VARCHAR(20),
    raw_payload     TEXT,
    error_type      VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

ALTER TABLE public.realtime_transactions
    ADD COLUMN IF NOT EXISTS source_system  VARCHAR(20),
    ADD COLUMN IF NOT EXISTS event_id       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS cif            VARCHAR(20),
    ADD COLUMN IF NOT EXISTS wallet_id      VARCHAR(50),
    ADD COLUMN IF NOT EXISTS merchant_name  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS base_amount    NUMERIC(18,2),
    ADD COLUMN IF NOT EXISTS currency       VARCHAR(10);
"@
echo $sql | docker exec -i lakehouse-postgres psql -U edm_user -d dw_edm
```

**Buoc 3.4** - Cau hinh ket noi Airflow toi Postgres (chay mot lan):
```powershell
docker exec airflow_scheduler airflow connections add postgres_lakehouse `
  --conn-type postgres `
  --conn-host lakehouse-postgres `
  --conn-port 5432 `
  --conn-schema dw_edm `
  --conn-login edm_user `
  --conn-password edm_pass
```

### Buoc 4: Nap du lieu mau (Corebank va Ewallet)

```powershell
$seed = @"
INSERT INTO corebank.customer (cif, full_name, national_id, phone)
SELECT
    'CIF' || LPAD(gs::text, 6, '0'),
    'Customer ' || gs,
    '0' || LPAD(gs::text, 9, '0'),
    '09' || LPAD(gs::text, 8, '0')
FROM generate_series(1, 200) gs
ON CONFLICT DO NOTHING;

INSERT INTO ewallet.account (user_id, phone_number, full_name, national_id, kyc_level)
SELECT
    'W' || LPAD((gs + 500)::text, 6, '0'),
    '08' || LPAD(gs::text, 8, '0'),
    'Wallet User ' || gs,
    '1' || LPAD(gs::text, 9, '0'),
    CASE WHEN gs % 3 = 0 THEN 'ADVANCED' WHEN gs % 2 = 0 THEN 'STANDARD' ELSE 'BASIC' END
FROM generate_series(1, 250) gs
ON CONFLICT DO NOTHING;
"@
echo $seed | docker exec -i lakehouse-postgres psql -U edm_user -d dw_edm
```

---

## CHAY CAC LAN SAU (Daily Run)

### Buoc 1: Dam bao Docker dang chay

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Neu container nao bi dung, khoi dong lai:
```powershell
cd 02_ingestion
docker-compose up -d

cd ../03_processing
docker-compose up -d
```

### Buoc 2: Chay Kafka Producer (Terminal 1)

Mo terminal rieng va chay:
```powershell
cd c:\Users\ADMIN\Desktop\edm-etl
python 01_data_sources\generate_realtime_events.py
```

Producer se gui giao dich PAYMENT va EWALLET lien tuc vao Kafka moi 0.1-1 giay. Nhan Ctrl+C de dung.

### Buoc 3: Chay Spark Streaming Job (Terminal 2)

Mo terminal khac va chay:
```powershell
docker exec lakehouse-spark-worker /opt/spark/bin/spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.4 `
  /opt/spark/jobs/streaming_job.py
```

Luu y: Cho den khi thay dong `>>> Spark Ingestion Engine Started. Listening to Kafka...` moi dung (Ctrl+C).
Khong dung trong qua trinh tai JAR (20-30 giay dau) de tranh corrupt cache.

### Buoc 4: Kich hoat Airflow DAG

Cho it nhat 30 giay de Spark nap du du lieu vao Landing Zone.

Cach 1 - Giao dien Web (khuyen dung):
- Truy cap: http://localhost:8080
- Tai khoan: admin / admin
- Tim DAG `enterprise_edw_etl_pipeline` va nhan Trigger DAG

Cach 2 - Dong lenh:
```powershell
docker exec airflow_scheduler airflow dags trigger enterprise_edw_etl_pipeline
```

### Buoc 5: Kiem tra ket qua

```powershell
# So giao dich trong Fact Table
docker exec lakehouse-postgres psql -U edm_user -d dw_edm -c "SELECT count(*) FROM dw.fact_enterprise_transaction;"

# So khach hang da duoc resolve (MDM)
docker exec lakehouse-postgres psql -U edm_user -d dw_edm -c "SELECT count(*), is_wallet_user, is_borrower FROM dw.dim_customer GROUP BY 2,3;"

# 10 giao dich moi nhat
docker exec lakehouse-postgres psql -U edm_user -d dw_edm -c "SELECT source_system, base_amount, currency_code, trans_timestamp FROM dw.fact_enterprise_transaction ORDER BY trans_timestamp DESC LIMIT 10;"
```

---

## Cac dich vu Serving Layer (Tuy chon)

```powershell
cd 05_serving
docker-compose up -d
```

| Dich vu              | URL                        | Dang nhap           |
|----------------------|----------------------------|---------------------|
| Airflow (ETL)        | http://localhost:8080       | admin / admin       |
| Metabase (BI)        | http://localhost:3000       | Thiet lap lan dau   |
| Jupyter Notebook     | http://localhost:8888       | Token: admin        |
| Streamlit Dashboard  | http://localhost:8501       | Khong can           |
| FastAPI (ML API)     | http://localhost:8000/docs  | Khong can           |
| Spark Master UI      | http://localhost:8089       | Khong can           |

Ket noi Metabase toi Postgres:
- Type: PostgreSQL
- Host: host.docker.internal
- Port: 5432
- Database: dw_edm
- User: edm_user / Pass: edm_pass

---

## Xu ly su co thuong gap

### Loi: The '<' operator is reserved for future use (PowerShell)

PowerShell khong ho tro chuyen huong < nhu Linux. Thay the bang:
```powershell
# Sai
docker exec -i lakehouse-postgres psql ... < file.sql

# Dung
Get-Content "file.sql" | docker exec -i lakehouse-postgres psql -U edm_user -d dw_edm
```

### Loi: relation "identities" does not exist (Airflow)

Task load_dim_customer bi do vi lenh SQL bi tach roi. Fix da duoc ap dung vao DAG.
Khoi WITH ... AS phai nam ngay truoc lenh INSERT INTO tuong ung, khong co cau lenh SQL nao xen giua.

### Loi: duplicate key value violates unique constraint (Airflow)

Cac bang Dimension co ban ghi Unknown (SK=1) va chuoi SERIAL phai bat dau tu 2.
Fix da duoc ap dung: RESTART IDENTITY + setval() trong moi Task Dimension.

### Loi: Spark khong tim thay file streaming_job.py

Code duoc mount vao Spark Worker (khong phai Master):
```powershell
docker exec lakehouse-spark-worker /opt/spark/bin/spark-submit ...
```

### Loi: Corrupt Ivy Cache (Spark JAR download bi gian doan)

```powershell
docker exec lakehouse-spark-worker rm -rf /root/.ivy2
```

### Loi: lakehouse-postgres container khong chay

Postgres nam trong stack 04_storage:
```powershell
cd 04_storage
docker-compose up -d
```

---

Happy Engineering!
