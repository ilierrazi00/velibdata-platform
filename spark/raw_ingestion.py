import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

spark = (
    SparkSession.builder.appName("VelibRawIngestion")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def stream_topic_to_raw(topic, bucket_path):
    """Lit un topic Kafka et écrit le brut en Parquet dans MinIO (zone RAW)."""
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    # On garde le message brut (clé + valeur JSON) + horodatage d'ingestion
    out = df.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "timestamp AS kafka_timestamp",
    ).withColumn("ingestion_ts", current_timestamp())

    return (
        out.writeStream.format("parquet")
        .option("path", bucket_path)
        .option("checkpointLocation", bucket_path + "/_checkpoint")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


# Une stream par source, écriture dans la zone RAW
q1 = stream_topic_to_raw("velib.station_status", "s3a://raw/station_status")
q2 = stream_topic_to_raw("velib.station_information", "s3a://raw/station_information")
q3 = stream_topic_to_raw("velib.weather", "s3a://raw/weather")

spark.streams.awaitAnyTermination()