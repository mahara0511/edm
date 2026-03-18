"""
PySpark Structured Streaming Job mô phỏng Processing Layer
Đọc Real-time data từ Kafka -> Ghi xuống Data Lake (MinIO S3) & Data Warehouse (Postgres)
Cần chạy bằng: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 streaming_job.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Define Kafka Schema tương ứng với dữ liệu giả lập từ thư mục 01
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("txn_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("is_fraud_flag", IntegerType(), True)
])

def create_spark_session():
    # Khởi tạo Spark kết nối tới Spark Master trong Docker và cấu hình S3 MinIO
    spark = SparkSession.builder \
        .appName("Lakehouse-Realtime-ETL") \
        .master("spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream():
    spark = create_spark_session()
    print(">>> Bắt đầu lắng nghe Spark Streaming từ Kafka topic 'realtime_transactions'...")

    # 1. Đọc stream từ Kafka (Dùng tên container kafka thay vì localhost)
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "realtime_transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Transform JSON (Chuyển byte sang JSON format)
    df_json = df_kafka.selectExpr("CAST(value AS STRING)")
    df_parsed = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")
    df_parsed = df_parsed.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))

    # 3. Ghi song song: 
    #   - Ghi xuống Cold Storage (Data Lake: MinIO S3 Object Storage)
    query_lake = df_parsed.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "s3a://data-lake/raw/transactions") \
        .option("checkpointLocation", "s3a://data-lake/checkpoints/data_lake") \
        .start()

    #   - Ghi xuống Hot/Warm Storage qua Console/Postgres (Ở local demo ghi console check đã)
    def write_to_postgres_and_console(batch_df, batch_id):
        # 1. Ghi ra console để debug
        batch_df.show(truncate=False)
        # 2. Ghi vào PostgreSQL (Operational DB)
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://lakehouse-postgres:5432/dw_edm") \
            .option("dbtable", "public.realtime_transactions") \
            .option("user", "edm_user") \
            .option("password", "edm_pass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    query_postgres = df_parsed.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://data-lake/checkpoints/postgres") \
        .foreachBatch(write_to_postgres_and_console) \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
