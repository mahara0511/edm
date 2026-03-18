import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Định nghĩa Schema Enterprise (Khớp với Kafka Generator)
schema = StructType([
    StructField("source_system", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("cif", StringType(), True),
    StructField("wallet_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("base_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True)
])

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Enterprise-Lakehouse-Ingestion") \
        .master("local[*]") \
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
    print(">>> Spark Ingestion Engine Started. Listening to Kafka...")

    # Đọc từ Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "realtime_transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON & Cast Types
    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) # Phải cast sang Timestamp cho Postgres

    # Hàm nạp Batch vào Postgres
    def foreach_batch_function(batch_df, batch_id):
        if batch_df.count() > 0:
            print(f">>> Writing Batch {batch_id} to Landing Zone...")
            # Đảm bảo types khớp PostgreSQL
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://lakehouse-postgres:5432/dw_edm") \
                .option("dbtable", "public.realtime_transactions") \
                .option("user", "edm_user") \
                .option("password", "edm_pass") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()

    # Chạy stream
    query = df_parsed.writeStream \
        .foreachBatch(foreach_batch_function) \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()
