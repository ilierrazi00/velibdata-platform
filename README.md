# VélibData — Plateforme Big Data (Blocs 2 & 4)

Plateforme d'ingestion, stockage et traitement temps réel des données Vélib' Métropole.
MSPR RNCP36921 — EPSI — Équipe : Ilias Errazi, Manal Jawhar, Issmail Khouyi.

## Architecture

API Open Data Paris + OpenWeatherMap → Kafka → Spark Structured Streaming
→ Data Lake MinIO (zones RAW / CLEAN / CURATED, format Parquet) → Power BI

## Sources de données

| Source | Dataset | Topic Kafka | Fréquence |
|---|---|---|---|
| Vélib disponibilité | velib-disponibilite-en-temps-reel | velib.station_status | 60 s |
| Vélib stations | velib-emplacement-des-stations | velib.station_information | 10 min |
| Météo | OpenWeatherMap (Paris) | velib.weather | 10 min |

## Prérequis

- Docker Desktop (backend WSL2)
- Fichier `.env` à créer à partir de `.env.example`

## Démarrage

```bash
cp .env.example .env   # puis renseigner les valeurs
docker compose up -d --build
```

## Interfaces

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Console MinIO | http://localhost:9001 |
| Spark UI | http://localhost:4040 |

## Structure

- `producers/` — producers Python (3 sources)
- `spark/` — jobs Spark (ingestion RAW, puis CLEAN/CURATED)
- `docs/` — documentation et captures