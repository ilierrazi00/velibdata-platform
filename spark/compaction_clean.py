import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

spark = (
    SparkSession.builder.appName("VelibCompactionClean")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SRC = "s3a://clean/station_status"
DST = "s3a://clean-compacted/station_status"

df = spark.read.format("parquet").load(SRC)

(
    df.repartition(4)
    .write.format("parquet")
    .mode("overwrite")
    .option("compression", "snappy")
    .save(DST)
)

print("=" * 55)
print("COMPACTION TERMINEE - voir clean-compacted/station_status")
print("=" * 55)
spark.stop()
