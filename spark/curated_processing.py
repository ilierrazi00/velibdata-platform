import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, round as spark_round, current_timestamp, lit, least, row_number
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", "120"))  # toutes les 2 min

spark = (
    SparkSession.builder.appName("VelibCuratedProcessing")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def build_curated():
    # Lecture batch des zones CLEAN
    status = spark.read.parquet("s3a://clean/station_status")
    info = spark.read.parquet("s3a://clean/station_information")
    weather = spark.read.parquet("s3a://clean/weather")

    # Dernier relevé météo (le plus récent)
    last_weather = weather.orderBy(col("weather_ts").desc()).limit(1)
    w = last_weather.select(
        col("temperature").alias("meteo_temp"),
        col("wind_speed").alias("meteo_vent"),
        col("humidity").alias("meteo_humidite"),
    )

    # Dernier état de disponibilité par station (déduplication sur le plus récent)
    w_spec = Window.partitionBy("stationcode").orderBy(col("ingestion_ts").desc())
    latest_status = (
        status.withColumn("rn", row_number().over(w_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Référentiel stations mis en cache (réutilisé à chaque batch)
    # -> équivalent "mise en cache des fichiers avant insertion" (comp. 5)
    ref_stations = info.select("stationcode", "name", "latitude", "longitude").cache()

    # Jointure disponibilité + référentiel stations
    joined = latest_status.join(
        ref_stations,
        on="stationcode", how="left",
    )

    # Taux d'occupation + enrichissement météo (cross join sur 1 ligne météo)
    curated = (
        joined.crossJoin(w)
        .withColumn(
            "taux_occupation",
            spark_round(col("numbikesavailable") / col("capacity") * 100, 1),
        )
        .withColumn("date_calcul", current_timestamp())
        .select(
            "stationcode", "name", "latitude", "longitude",
            "numbikesavailable", "mechanical", "ebike",
            "numdocksavailable", "capacity", "taux_occupation",
            "nom_arrondissement_communes",
            "meteo_temp", "meteo_vent", "meteo_humidite",
            "date_calcul",
        )
    )
    return curated


print("Démarrage du traitement CURATED (batch toutes les %ds)" % BATCH_INTERVAL)
while True:
    try:
        curated = build_curated()
        # Nettoyage qualité : on fiabilise la donnée métier
        curated = (
            curated
            .filter(col("stationcode").isNotNull())                     # pas de station sans code
            .filter((col("capacity") > 0) & (col("capacity") <= 100))   # capacité plausible
            .withColumn(                                                 # taux plafonné à 100
                "taux_occupation",
                least(col("taux_occupation"), lit(100.0))
            )
        )
        # Comptage AVANT écriture (référence d'intégrité)
        nb_ecrit = curated.count()

        (
            curated.write.mode("overwrite")
            .parquet("s3a://curated/stations_enrichies")
        )

        # Vérification d'intégrité post-écriture (équivalent "somme de contrôle", comp. 5)
        # On relit ce qui vient d'être écrit et on compare le nombre de lignes.
        nb_relu = spark.read.parquet("s3a://curated/stations_enrichies").count()
        if nb_ecrit == nb_relu:
            print("CURATED mis à jour : %d stations - integrite OK (ecrit=%d, relu=%d)"
                  % (nb_relu, nb_ecrit, nb_relu))
        else:
            print("ALERTE INTEGRITE CURATED : ecart detecte (ecrit=%d, relu=%d)"
                  % (nb_ecrit, nb_relu))
    except Exception as e:
        print("Erreur CURATED : %s" % e)
    time.sleep(BATCH_INTERVAL)