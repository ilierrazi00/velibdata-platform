"""
Test 1/3 — Validation du schéma CURATED (compétence 3 : tests automatisés).

Vérifie que le contrat de la table stations_enrichies est respecté : un
DataFrame conforme est accepté, un DataFrame malformé (colonne manquante ou
mauvais type) est rejeté.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DoubleType)

from spark.velib_rules import validate_curated_schema, CURATED_REQUIRED_SCHEMA


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_schema_curated")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


def _schema_from(required):
    return StructType([StructField(n, t, True) for n, t in required.items()])


def test_schema_conforme_est_accepte(spark):
    """Un DataFrame respectant le contrat CURATED passe la validation."""
    schema = _schema_from(CURATED_REQUIRED_SCHEMA)
    df = spark.createDataFrame(
        [("16107", 35, 12, 23, 34.60),
         ("32017", 20, 0, 20, 0.0)],
        schema=schema,
    )
    ok, manquantes, types_incorrects = validate_curated_schema(df)
    assert ok, f"manquantes={manquantes}, types_incorrects={types_incorrects}"


def test_colonne_manquante_est_rejetee(spark):
    """L'absence d'une colonne obligatoire fait échouer la validation."""
    schema = StructType([
        StructField("stationcode", StringType(), True),
        StructField("capacity", LongType(), True),
        # numbikesavailable / numdocksavailable / taux_occupation absents
    ])
    df = spark.createDataFrame([("16107", 35)], schema=schema)
    ok, manquantes, _ = validate_curated_schema(df)
    assert not ok
    assert "taux_occupation" in manquantes


def test_type_incorrect_est_rejete(spark):
    """Un mauvais type sur une colonne obligatoire est détecté."""
    schema = StructType([
        StructField("stationcode", StringType(), True),
        StructField("capacity", LongType(), True),
        StructField("numbikesavailable", LongType(), True),
        StructField("numdocksavailable", LongType(), True),
        StructField("taux_occupation", StringType(), True),  # devrait être double
    ])
    df = spark.createDataFrame([("16107", 35, 12, 23, "34.60")], schema=schema)
    ok, _, types_incorrects = validate_curated_schema(df)
    assert not ok
    noms_en_erreur = [t[0] for t in types_incorrects]
    assert "taux_occupation" in noms_en_erreur
