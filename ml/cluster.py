# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

df = pd.read_csv("velib_ml.csv", parse_dates=["ts_15min"])
df["hour"]=df.ts_15min.dt.hour
df["capacity"]=df["capacity"].fillna(df["capacity"].median())
df["taux_occ"]=np.where(df.capacity>0, df.numbikesavailable/df.capacity, np.nan)

prof = df.pivot_table(index="stationcode", columns="hour", values="taux_occ", aggfunc="mean")
prof = prof.dropna(thresh=18).interpolate(axis=1, limit_direction="both")
print("Stations profilees:", len(prof))

X = StandardScaler().fit_transform(prof.values)
K=4
labels = KMeans(n_clusters=K, n_init=10, random_state=42).fit_predict(X)
prof_hours = prof.copy()
prof["cluster"]=labels

colors=["#2e8b57","#e07b39","#3b6fb0","#b0433b"]
fig,ax=plt.subplots(figsize=(8,4.6))
for c in range(K):
    sub=prof_hours[labels==c]
    ax.plot(sub.columns, sub.mean(axis=0).values, "-o", ms=3, color=colors[c],
            label=f"Cluster {c} (n={ (labels==c).sum() })")
ax.set_xlabel("Heure"); ax.set_ylabel("Taux d'occupation moyen")
ax.set_title(f"Profils journaliers d'occupation - {K} clusters de stations (K-means)")
ax.legend(); ax.grid(alpha=.3); ax.set_xticks(range(0,24,2))
fig.tight_layout(); fig.savefig("g5_clusters.png"); plt.close()

out = pd.DataFrame({"stationcode":prof.index, "cluster":labels})
out.to_csv("station_clusters.csv", index=False)

# resume interpretatif
m = df.merge(out, on="stationcode")
g = m.groupby("cluster").agg(n_stations=("stationcode","nunique"),
                             occ_moy=("taux_occ","mean"),
                             capacite_moy=("capacity","mean")).round(3)
print(g.to_string())
print("Clustering OK")
