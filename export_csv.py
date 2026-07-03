import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("ExportCSV")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT","http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
    .getOrCreate())

df = spark.read.parquet("s3a://curated/stations_enrichies")
df.coalesce(1).write.mode("overwrite").option("header","true").csv("/tmp/export_curated")
print("LIGNES EXPORTEES:", df.count())
spark.stop()
