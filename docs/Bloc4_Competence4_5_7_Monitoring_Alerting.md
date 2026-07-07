# Monitoring complet et alertes automatiques - VélibData

## Objectif

Superviser en temps réel la disponibilité des composants, les ressources CPU/mémoire des conteneurs, la santé et la capacité MinIO, puis notifier automatiquement les incidents.

## Architecture de supervision

- **Prometheus** collecte les métriques Kafka, MinIO, cAdvisor, Alertmanager et ses propres métriques.
- **cAdvisor** expose les métriques CPU, mémoire, réseau et fichiers des conteneurs Docker.
- **MinIO** expose ses métriques Prometheus de cluster et de nœud.
- **Grafana** affiche un tableau de bord unique avec Kafka, MinIO, CPU, mémoire, cibles et alertes actives.
- **Alertmanager** groupe, déduplique et route les alertes.
- **Webhook local** conserve chaque notification `firing` ou `resolved` dans `evidence/monitoring-alerts.jsonl`.

## Alertes configurées

- ingestion Vélib figée ;
- Kafka Exporter indisponible ;
- aucun broker Kafka actif ;
- MinIO indisponible ;
- nœud ou disque MinIO hors ligne ;
- capacité MinIO supérieure à 80 % ;
- cAdvisor indisponible ;
- CPU conteneur supérieur à 85 % pendant 3 minutes ;
- mémoire conteneur supérieure à 1,5 Gio pendant 5 minutes ;
- Alertmanager indisponible ;
- erreur d'évaluation d'une règle Prometheus.

## Test reproductible

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-monitoring-alerts.ps1
```

Le script vérifie les endpoints, confirme la présence des métriques MinIO/CPU/mémoire, arrête volontairement cAdvisor, attend une notification automatique `FIRING`, redémarre cAdvisor, puis attend la notification `RESOLVED`.

## Preuves

- `evidence/monitoring-YYYYMMDD-HHMMSS.txt`
- `evidence/monitoring-alerts.jsonl`
- dashboard Grafana : `http://localhost:3000/d/velib-main`
- Alertmanager : `http://localhost:9093`
- cAdvisor : `http://localhost:8081`

## Limite

La supervision est exécutée sur Docker Desktop en local. Elle démontre les mécanismes de collecte, d'alerte, de routage et de résolution, mais ne constitue pas une supervision multi-machine de production.
