import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

spark = (
    SparkSession.builder.appName("VelibCleanProcessing")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

RAW_META = "key STRING, value STRING, topic STRING, kafka_timestamp TIMESTAMP, ingestion_ts TIMESTAMP"

# --- Schémas ---
status_schema = StructType([
    StructField("stationcode", StringType()),
    StructField("name", StringType()),
    StructField("is_installed", StringType()),
    StructField("is_renting", StringType()),
    StructField("is_returning", StringType()),
    StructField("numbikesavailable", IntegerType()),
    StructField("numdocksavailable", IntegerType()),
    StructField("mechanical", IntegerType()),
    StructField("ebike", IntegerType()),
    StructField("capacity", IntegerType()),
    StructField("nom_arrondissement_communes", StringType()),
])

info_schema = StructType([
    StructField("stationcode", StringType()),
    StructField("name", StringType()),
    StructField("capacity", IntegerType()),
    StructField("coordonnees_geo", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType()),
    ])),
])

weather_schema = StructType([
    StructField("name", StringType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType()),
    ])),
    StructField("dt", IntegerType()),
])


def clean_stream(raw_path, schema, clean_path, dedup_keys, select_expr):
    raw = spark.readStream.format("parquet").schema(RAW_META).load(raw_path)
    parsed = (
        raw.select(from_json(col("value"), schema).alias("d"), col("ingestion_ts"))
        .selectExpr(*select_expr, "ingestion_ts")
        .dropDuplicates(dedup_keys)
    )
    return (
        parsed.writeStream.format("parquet")
        .option("path", clean_path)
        .option("checkpointLocation", clean_path + "/_checkpoint")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


# Disponibilité
clean_stream(
    "s3a://raw/station_status", status_schema, "s3a://clean/station_status",
    ["stationcode", "ingestion_ts"],
    ["d.stationcode", "d.numbikesavailable", "d.numdocksavailable",
     "d.mechanical", "d.ebike", "d.capacity", "d.nom_arrondissement_communes"],
)

# Référentiel stations
clean_stream(
    "s3a://raw/station_information", info_schema, "s3a://clean/station_information",
    ["stationcode"],
    ["d.stationcode", "d.name", "d.capacity",
     "d.coordonnees_geo.lat AS latitude", "d.coordonnees_geo.lon AS longitude"],
)

# Météo
clean_stream(
    "s3a://raw/weather", weather_schema, "s3a://clean/weather",
    ["weather_ts"],
    ["d.main.temp AS temperature", "d.main.humidity AS humidity",
     "d.wind.speed AS wind_speed", "d.dt AS weather_ts"],
)

spark.streams.awaitAnyTermination()