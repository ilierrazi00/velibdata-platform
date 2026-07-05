"""
Test 2/3 — Transformation : taux_occupation plafonné à 100.0
(compétence 3 : tests automatisés).

Vérifie que la règle least(taux_occupation, 100.0) écrête bien les valeurs
supérieures à 100 et laisse les autres intactes.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from spark.velib_rules import cap_taux_occupation


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_transformation_taux")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def df_taux(spark):
    schema = StructType([
        StructField("stationcode", StringType(), True),
        StructField("taux_occupation", DoubleType(), True),
    ])
    return spark.createDataFrame(
        [("A", 34.60),    # normal, inchangé
         ("B", 100.0),    # borne exacte, inchangé
         ("C", 137.5),    # aberrant, doit être ramené à 100.0
         ("D", 0.0)],     # station vide, inchangé
        schema=schema,
    )


def test_valeur_superieure_a_100_est_plafonnee(spark, df_taux):
    res = {r["stationcode"]: r["taux_occupation"]
           for r in cap_taux_occupation(df_taux).collect()}
    assert res["C"] == 100.0


def test_valeurs_normales_inchangees(spark, df_taux):
    res = {r["stationcode"]: r["taux_occupation"]
           for r in cap_taux_occupation(df_taux).collect()}
    assert res["A"] == 34.60
    assert res["B"] == 100.0
    assert res["D"] == 0.0


def test_aucune_valeur_ne_depasse_100(spark, df_taux):
    """Après transformation, plus aucune ligne au-dessus de 100."""
    depassements = (cap_taux_occupation(df_taux)
                    .filter("taux_occupation > 100.0")
                    .count())
    assert depassements == 0
