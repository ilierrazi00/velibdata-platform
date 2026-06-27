import os
import sys
import s3fs
import pandas as pd
import great_expectations as gx

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
CURATED_PATH = "curated/stations_enrichies"

print("=== Validation qualité des données — VélibData ===")

# Connexion S3 (MinIO)
fs = s3fs.S3FileSystem(
    key=MINIO_USER,
    secret=MINIO_PASSWORD,
    client_kwargs={"endpoint_url": MINIO_ENDPOINT},
)

# Lecture des Parquet CURATED
files = [f for f in fs.ls(CURATED_PATH) if f.endswith(".parquet")]
if not files:
    print("ERREUR : aucun fichier Parquet trouvé dans la zone CURATED.")
    sys.exit(1)

dfs = [pd.read_parquet(f"s3://{f}", filesystem=fs) for f in files]
df = pd.concat(dfs, ignore_index=True)
print(f"{len(df)} lignes chargées depuis CURATED ({len(files)} fichiers Parquet)")

# Création du validateur GX
gxdf = gx.from_pandas(df)

# --- Règles de qualité (Expectations) ---
results = []
results.append(gxdf.expect_table_row_count_to_be_between(min_value=1, max_value=100000))
results.append(gxdf.expect_column_values_to_not_be_null("stationcode"))
results.append(gxdf.expect_column_values_to_be_between("taux_occupation", min_value=0, max_value=100))
results.append(gxdf.expect_column_values_to_be_between("capacity", min_value=0, max_value=200))
results.append(gxdf.expect_column_values_to_be_between("numbikesavailable", min_value=0, max_value=200))
results.append(gxdf.expect_column_values_to_be_between("meteo_temp", min_value=-30, max_value=50))

# --- Synthèse ---
total = len(results)
passed = sum(1 for r in results if r["success"])
print("\n=== Résultats ===")
for r in results:
    exp = r["expectation_config"]["expectation_type"]
    col = r["expectation_config"]["kwargs"].get("column", "table")
    status = "✓ PASS" if r["success"] else "✗ FAIL"
    print(f"  {status}  {exp} ({col})")

print(f"\nBilan : {passed}/{total} tests réussis")

if passed == total:
    print("✅ QUALITÉ VALIDÉE — toutes les règles sont respectées.")
    sys.exit(0)
else:
    print("⚠️  QUALITÉ DÉGRADÉE — certaines règles ont échoué.")
    sys.exit(1)