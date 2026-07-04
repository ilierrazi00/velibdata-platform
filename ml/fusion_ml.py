import glob, os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

FOLDER = "ml_parquet"
files = glob.glob(os.path.join(FOLDER, "*.snappy.parquet"))
print(f"Fichiers Parquet trouves : {len(files)}")

tables = []
for i, f in enumerate(files):
    try:
        tables.append(pq.read_table(f))
    except Exception as e:
        print("skip", f, e)
    if (i+1) % 1000 == 0:
        print(f"  {i+1}/{len(files)} lus")

df = pa.concat_tables(tables, promote_options="default").to_pandas()
print("Lignes brutes :", len(df))

df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"])
df = df.drop_duplicates(["stationcode", "ingestion_ts"])
print("Apres dedup   :", len(df))
print("Periode       :", df["ingestion_ts"].min(), "->", df["ingestion_ts"].max())

df["ts_15min"] = df["ingestion_ts"].dt.floor("15min")
agg = (
    df.groupby(["stationcode", "ts_15min"], as_index=False)
      .agg(
          capacity=("capacity", "max"),
          nom_arrondissement_communes=("nom_arrondissement_communes", "first"),
          numbikesavailable=("numbikesavailable", "mean"),
          numdocksavailable=("numdocksavailable", "mean"),
          mechanical=("mechanical", "mean"),
          ebike=("ebike", "mean"),
      )
)
for c in ["numbikesavailable", "numdocksavailable", "mechanical", "ebike"]:
    agg[c] = agg[c].round().astype("Int64")

agg.to_csv("velib_ml.csv", index=False)
print("=== OK ===")
print("Lignes finales :", len(agg), "| Stations :", agg["stationcode"].nunique())
