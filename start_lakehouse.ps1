# Yêu cầu chạy trên Windows - PowerShell
$ErrorActionPreference = 'Stop'

Write-Host "=========================================================="
Write-Host "BƯỚC 1: KHỞI ĐỘNG LỚP STORAGE (PostgreSQL, MinIO DataLake)"
Write-Host "=========================================================="
cd 02_storage
docker compose up -d
cd ..

Write-Host "`n[Đang chờ 10 giây để PostgreSQL Database tạo sẵn các Schema, Bảng...]`n"
Start-Sleep -Seconds 10

Write-Host "=========================================================="
Write-Host "BƯỚC 2: MỒI DỮ LIỆU TỪ EXCEL VÀO STORAGE (RAW ODS)"
Write-Host "=========================================================="
cd 01_data_sources
py -3 load_data_to_source_db.py
cd ..

Write-Host "`n=========================================================="
Write-Host "BƯỚC 3: KHỞI ĐỘNG LỚP PROCESSING (Apache Airflow)"
Write-Host "=========================================================="
cd 03_processing
Write-Host "[Đang khởi tạo Database cho Airflow...]"
docker compose run --rm airflow-init
Write-Host "[Khởi động Airflow Services...]"
docker compose up -d
cd ..

Write-Host "`n=========================================================="
Write-Host "BƯỚC 4: KHỞI ĐỘNG LỚP SERVING (Metabase BI)"
Write-Host "=========================================================="
cd 04_serving
docker compose up -d
cd ..

Write-Host "`n=========================================================="
Write-Host "                 🚀 TRIỂN KHAI THÀNH CÔNG 🚀"
Write-Host "=========================================================="
Write-Host "Các service đã mở tại:"
Write-Host "- Cơ sở dữ liệu DW (Postgres): localhost:5433 (user: edm_user / pass: edm_pass)"
Write-Host "- Quản lý SQL Web (Adminer)  : http://localhost:8082"
Write-Host "- Giao diện Data Lake (MinIO): http://localhost:9001 (user: admin / pass: password)"
Write-Host "- Pipeline ETL (Airflow UI)  : http://localhost:8080 (user: admin / pass: admin)"
Write-Host "- Báo cáo BI (Metabase UI)   : http://localhost:3000"
Write-Host "=========================================================="
