import pandas as pd, numpy as np, json
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

df = pd.read_csv("velib_ml.csv", parse_dates=["ts_15min"]).sort_values(["stationcode","ts_15min"]).reset_index(drop=True)
key = df.set_index(["stationcode","ts_15min"])["numbikesavailable"]
def lookup(delta):
    ts = df["ts_15min"] + pd.Timedelta(minutes=delta)
    return key.reindex(pd.MultiIndex.from_arrays([df["stationcode"], ts])).values
for m in [15,30,45,60]:
    df[f"lag_{m}"]=lookup(-m)
df["hour"]=df.ts_15min.dt.hour; df["dow"]=df.ts_15min.dt.dayofweek
df["weekend"]=(df.dow>=5).astype(int)
mod=df.hour*60+df.ts_15min.dt.minute
df["sin_t"]=np.sin(2*np.pi*mod/1440); df["cos_t"]=np.cos(2*np.pi*mod/1440)
df["capacity"]=df["capacity"].fillna(df["capacity"].median())
df["taux_occ"]=np.where(df.capacity>0, df.numbikesavailable/df.capacity, np.nan)
df["is_paris"]=(df.nom_arrondissement_communes=="Paris").astype(int)
df["roll_mean_lag"]=df[["lag_15","lag_30","lag_45","lag_60"]].mean(axis=1)
FEAT=["numbikesavailable","numdocksavailable","mechanical","ebike","capacity","taux_occ",
      "lag_15","lag_30","lag_45","lag_60","roll_mean_lag","hour","dow","weekend","sin_t","cos_t","is_paris"]

def run(h):
    d=df.copy(); d["tgt"]=lookup(h); d=d[d.tgt.notna()].copy()
    cut=d.ts_15min.max()-pd.Timedelta(days=2)
    tr=d[d.ts_15min<=cut]; te=d[d.ts_15min>cut]
    m=XGBRegressor(n_estimators=400,max_depth=6,learning_rate=0.05,subsample=0.8,
                   colsample_bytree=0.8,min_child_weight=5,n_jobs=4,random_state=42)
    m.fit(tr[FEAT],(tr.tgt-tr.numbikesavailable).values)
    pred=np.clip(te.numbikesavailable.values+m.predict(te[FEAT]),0,None)
    true=te.tgt.values; base=te.numbikesavailable.values
    return dict(mae=mean_absolute_error(true,pred),rmse=np.sqrt(mean_squared_error(true,pred)),
                r2=r2_score(true,pred),mae_b=mean_absolute_error(true,base),
                rmse_b=np.sqrt(mean_squared_error(true,base)),n_tr=len(tr),n_te=len(te))

res={}
print(f"{'Horizon':<8}{'Modele':<14}{'MAE':>7}{'RMSE':>8}{'R2':>7}")
print("-"*44)
for h,lbl in [(60,"H+1"),(180,"H+3"),(1440,"H+24")]:
    r=run(h); res[lbl]=r
    print(f"{lbl:<8}{'Persistance':<14}{r['mae_b']:>7.2f}{r['rmse_b']:>8.2f}{'':>7}")
    print(f"{'':<8}{'XGBoost':<14}{r['mae']:>7.2f}{r['rmse']:>8.2f}{r['r2']:>7.3f}")
    print(f"{'':<8}(n_test={r['n_te']})")
    print()
json.dump(res, open("metrics.json","w"), indent=2)

# graphe 3 horizons
labels=["H+1","H+3","H+24"]; x=np.arange(3); w=.35
b=[res[l]["rmse_b"] for l in labels]; g=[res[l]["rmse"] for l in labels]
fig,ax=plt.subplots(figsize=(6.5,4))
ax.bar(x-w/2,b,w,label="Persistance",color="#8a8a8a")
ax.bar(x+w/2,g,w,label="XGBoost",color="#2e8b57")
ax.set_xticks(x); ax.set_xticklabels(labels); ax.set_ylabel("RMSE (velos)")
ax.set_title("RMSE : XGBoost vs persistance selon l'horizon")
for i,(bb,gg) in enumerate(zip(b,g)):
    ax.text(i-w/2,bb+.05,f"{bb:.2f}",ha="center",fontsize=8)
    ax.text(i+w/2,gg+.05,f"{gg:.2f}",ha="center",fontsize=8)
ax.legend(); fig.tight_layout(); fig.savefig("g4_horizons.png"); plt.close()
print("Graphe 3 horizons OK")
