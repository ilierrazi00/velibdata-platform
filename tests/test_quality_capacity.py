"""
Test 3/3 — Qualité des données : capacity conservée entre 1 et 100
(compétence 3 : tests automatisés).

Vérifie que le filtre qualité écarte les valeurs aberrantes (ex. 1613 issue
d'un artefact de jointure) et les valeurs nulles, tout en gardant les bornes.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

from spark.velib_rules import filter_valid_capacity


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_quality_capacity")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def df_capacity(spark):
    schema = StructType([
        StructField("stationcode", StringType(), True),
        StructField("capacity", LongType(), True),
    ])
    return spark.createDataFrame(
        [("A", 1),       # borne basse valide
         ("B", 35),      # valide
         ("C", 100),     # borne haute valide
         ("D", 0),       # invalide (< 1)
         ("E", 1613),    # aberrant (artefact de jointure)
         ("F", None)],   # nulle
        schema=schema,
    )


def test_valeurs_aberrantes_ecartees(spark, df_capacity):
    codes = {r["stationcode"] for r in filter_valid_capacity(df_capacity).collect()}
    assert "E" not in codes   # 1613 écartée
    assert "D" not in codes   # 0 écartée
    assert "F" not in codes   # nulle écartée


def test_bornes_conservees(spark, df_capacity):
    codes = {r["stationcode"] for r in filter_valid_capacity(df_capacity).collect()}
    assert {"A", "B", "C"}.issubset(codes)   # 1, 35, 100 gardées


def test_toutes_les_lignes_restantes_sont_valides(spark, df_capacity):
    restantes = filter_valid_capacity(df_capacity).collect()
    assert len(restantes) == 3
    for r in restantes:
        assert 1 <= r["capacity"] <= 100
