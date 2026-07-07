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
## Démonstration Kubernetes sécurisée et test de panne

Les identifiants MinIO ne sont pas stockés dans les manifestes Kubernetes.

```powershell
# 1. Créer le secret depuis le .env local et déployer MinIO
powershell -ExecutionPolicy Bypass -File scripts/deploy-minio-k8s.ps1

# 2. Tester lecture, écriture et SHA-256 avec un pod indisponible
powershell -ExecutionPolicy Bypass -File scripts/test-minio-resilience.ps1
```

Une preuve horodatée est créée dans `evidence/`.

## CI et smoke tests automatiques

Le workflow `.github/workflows/ci.yml` exécute : validation → tests unitaires → build Docker → déploiement temporaire → smoke tests → publication des preuves.

Test local Windows :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\cicd-local.ps1
```

La stack de test est isolée dans `docker-compose.smoke.yml` et produit un rapport horodaté dans `evidence/`.

## Monitoring complet et alertes automatiques

Le monitoring couvre Kafka, MinIO, CPU/mémoire des conteneurs, Prometheus et Alertmanager.

```powershell
# Démarrer les composants de supervision

docker compose up -d minio kafka kafka-exporter cadvisor alert-webhook alertmanager prometheus grafana

# Exécuter le test automatique FIRING puis RESOLVED
powershell -ExecutionPolicy Bypass -File .\scripts\test-monitoring-alerts.ps1
```

Interfaces : Grafana `http://localhost:3000`, Prometheus `http://localhost:9090`, Alertmanager `http://localhost:9093`, cAdvisor `http://localhost:8081`.
