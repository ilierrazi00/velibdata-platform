"""
velib_rules.py
--------------
Règles métier de la zone CURATED (table stations_enrichies), isolées ici
comme *source unique de vérité* : le pipeline (curated_processing.py) et les
tests automatiques (tests/) s'appuient sur les mêmes fonctions.

Trois règles couvertes :
  1. Schéma CURATED    -> colonnes/types attendus de stations_enrichies
  2. Transformation    -> taux_occupation plafonné à 100.0
  3. Qualité données   -> capacity conservée uniquement entre 1 et 100

Les fonctions sont pures (DataFrame -> DataFrame) et n'ont aucune dépendance
à MinIO/Kafka, ce qui les rend exécutables en intégration continue.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, least, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


# --------------------------------------------------------------------------
# 1. SCHÉMA CURATED — contrat minimal de la table stations_enrichies
# --------------------------------------------------------------------------
# Colonnes essentielles produites en sortie de la zone CURATED. Le test de
# schéma vérifie que ces colonnes existent avec le bon type. Ajuste cette
# liste si le contrat de stations_enrichies évolue.
CURATED_REQUIRED_SCHEMA = {
    "stationcode": StringType(),
    "capacity": LongType(),
    "numbikesavailable": LongType(),
    "numdocksavailable": LongType(),
    "taux_occupation": DoubleType(),
}


def validate_curated_schema(df: DataFrame, required=None):
    """
    Vérifie que le DataFrame respecte le contrat CURATED.

    Retourne (ok: bool, manquantes: list, types_incorrects: list).
    - manquantes         : colonnes attendues absentes
    - types_incorrects   : (colonne, type_attendu, type_reel)
    """
    if required is None:
        required = CURATED_REQUIRED_SCHEMA

    present = dict(df.dtypes)  # {nom_colonne: type_simple_string}
    manquantes = []
    types_incorrects = []

    for name, expected_type in required.items():
        if name not in present:
            manquantes.append(name)
            continue
        attendu = expected_type.simpleString()
        reel = present[name]
        if reel != attendu:
            types_incorrects.append((name, attendu, reel))

    ok = (len(manquantes) == 0) and (len(types_incorrects) == 0)
    return ok, manquantes, types_incorrects


# --------------------------------------------------------------------------
# 2. TRANSFORMATION — taux_occupation plafonné à 100.0
# --------------------------------------------------------------------------
def cap_taux_occupation(df: DataFrame, col_name: str = "taux_occupation") -> DataFrame:
    """
    Plafonne taux_occupation à 100.0 : least(col, 100.0).

    Reproduit la règle du pipeline CURATED, où un artefact de jointure pouvait
    produire un taux > 100 %. Les valeurs <= 100 sont inchangées.
    """
    return df.withColumn(col_name, least(col(col_name), lit(100.0)))


# --------------------------------------------------------------------------
# 3. QUALITÉ — capacity conservée uniquement entre 1 et 100
# --------------------------------------------------------------------------
def filter_valid_capacity(df: DataFrame, col_name: str = "capacity",
                          lo: int = 1, hi: int = 100) -> DataFrame:
    """
    Ne conserve que les lignes dont capacity est comprise entre lo et hi (inclus).

    Écarte les valeurs aberrantes (ex. la capacité 1613 issue d'un artefact de
    jointure détectée sur les données brutes) et les valeurs nulles.
    """
    return df.filter(col(col_name).isNotNull() &
                     (col(col_name) >= lo) &
                     (col(col_name) <= hi))
