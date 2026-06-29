# Compétence 8 — Guide utilisateur, guide métier et schéma d'architecture

**Projet VélibData — Plateforme Big Data**
**MSPR RNCP36921 — Bloc 4 « Administration et supervision d'une plateforme Big Data »**
**Équipe :** Ilias Errazi (Data Engineer / chef de projet) · Manal Jawhar (Data Analyst / sécurité-DLP) · Issmail Khouyi (Data Science / veille / Green IT)
**Version :** 1.0 — 29 juin 2026

---

## 1. Objet du livrable

La grille d'évaluation de la compétence 8 attend, en complément du protocole de maintenance, que l'équipe *« rédige un guide utilisateur, un guide métier ainsi que des schémas d'architectures clairs et détaillés »*.

Ce document fournit ces trois éléments :

1. un **schéma d'architecture opérationnel** décrivant la plateforme telle qu'elle est réellement déployée ;
2. un **guide utilisateur** (à destination de l'exploitant technique) décrivant comment démarrer, vérifier, superviser et arrêter la plateforme ;
3. un **guide métier** (à destination des profils non techniques) expliquant ce que produit la plateforme et comment lire les indicateurs.

Il complète le protocole de maintenance (même compétence) et le reporting coûts/performances (compétence 7).

---

## 2. Schéma d'architecture opérationnel

Le schéma ci-dessous représente l'**état déployé** de la plateforme (à distinguer de l'architecture *cible* proposée au Bloc 1). Il met en évidence deux environnements complémentaires :

- le **pipeline de données de production**, exécuté sous **Docker Compose**, qui fait circuler la donnée des sources jusqu'à Power BI ;
- les **briques d'administration et de supervision**, démontrées sur **Kubernetes** (stockage distribué, self-healing, autoscaling, rétention) et couvertes par la supervision Prometheus/Grafana.

![Schéma d'architecture opérationnel VélibData](Bloc4_Schema_Architecture_Operationnel.svg)

*Figure — Architecture opérationnelle VélibData. Flux pleins : circulation de la donnée. Flux pointillés : administration et supervision transverses.*

**Lecture du flux principal :**

| Étape | Composant | Rôle |
|---|---|---|
| 1. Sources | APIs Vélib' (disponibilité + stations), OpenWeatherMap | Données ouvertes temps réel et météo |
| 2. Ingestion | Producers Python → Apache Kafka 3.9 (KRaft) | Collecte et mise en file découplée |
| 3. Traitement | Apache Spark 3.5 Structured Streaming | RAW→CLEAN (streaming), CLEAN→CURATED (batch 120 s) |
| 4. Qualité | Great Expectations | Validation 6/6 règles sur la zone CURATED |
| 5. Stockage | MinIO — zones RAW / CLEAN / CURATED | Data Lake objet S3-compatible, Parquet+Snappy |
| 6. Restitution | Power BI | Tableaux de bord métier sur la zone CURATED |

**Briques d'administration (Kubernetes) :** MinIO en StatefulSet 4 réplicas (stockage distribué + self-healing), KEDA (autoscaling des workers Spark 1→4), CronJob de rétention RAW > 30 j, CI/CD GitHub Actions. **Supervision transverse :** Prometheus + Kafka exporter + Grafana.

---

## 3. Guide utilisateur (exploitant technique)

Ce guide s'adresse à toute personne devant **démarrer, vérifier ou superviser** la plateforme. Il suppose un poste Windows avec **Docker Desktop** (backend WSL2) installé et le dépôt cloné dans `velibdata-platform`.

### 3.1 Prérequis

- Docker Desktop démarré (vérifier l'icône active).
- Le fichier `.env` présent à la racine (copié depuis `.env.example` et renseigné : identifiants MinIO, clé API OpenWeatherMap). **Ce fichier n'est jamais versionné.**
- Pour les démonstrations d'administration : Kubernetes activé dans Docker Desktop.

### 3.2 Démarrer la plateforme

Depuis la racine du projet :

```sh
docker compose up -d
```

Cette commande lance l'ensemble de la pile : Kafka, Spark, MinIO, les producers, Prometheus et Grafana.

### 3.3 Vérifier que tout fonctionne

```sh
docker compose ps
```

Tous les services doivent être à l'état `running` / `healthy`. Pour suivre le traitement en direct :

```sh
docker compose logs -f spark
```

### 3.4 Accéder aux interfaces

| Interface | URL | Usage |
|---|---|---|
| Console MinIO | `http://localhost:9001` | Explorer les zones RAW / CLEAN / CURATED |
| Grafana | `http://localhost:3000` | Dashboard de supervision (débit Kafka, volumes, alertes) |
| Prometheus | `http://localhost:9090` | Métriques brutes |

Les identifiants de connexion sont définis dans le fichier `.env` (non communiqués ici pour des raisons de sécurité).

### 3.5 Superviser le stockage (client `mc`)

Le client MinIO `mc` permet d'inspecter le Data Lake en ligne de commande. Configuration de l'alias (à faire une seule fois) :

```sh
mc alias set velibdc http://localhost:9000 <ACCESS_KEY> <SECRET_KEY>
```

> **Convention :** l'alias `velibdc` est utilisé de manière uniforme dans toute la documentation VélibData (reporting compétence 7, protocole de maintenance compétence 8). Les identifiants proviennent du fichier `.env` / du secret Kubernetes `minio-creds`.

Commandes de supervision courantes :

```sh
mc du velibdc/raw velibdc/clean velibdc/curated      # volumétrie par zone
mc ls --recursive --summarize velibdc/clean          # nombre d'objets (détection small files)
```

### 3.6 Démontrer les compétences d'administration (Kubernetes)

```sh
kubectl get pods -n velibdata                         # état des pods (MinIO, Spark)
kubectl delete pod <pod-minio> -n velibdata           # self-healing : recréation auto ~3 s
kubectl get hpa -n velibdata                          # suivi de l'autoscaling KEDA
```

### 3.7 Arrêter la plateforme

```sh
docker compose down          # arrêt en conservant les volumes (données préservées)
docker compose down -v       # arrêt + suppression des volumes (remise à zéro complète)
```

> **Attention :** `down -v` efface les données du Data Lake. À n'utiliser que pour repartir d'un environnement vierge.

---

## 4. Guide métier (profils non techniques)

Ce guide s'adresse aux **directions métier** (Data Analysts, décideurs, exploitation Vélib') qui consultent les résultats sans manipuler l'infrastructure.

### 4.1 À quoi sert la plateforme

VélibData transforme les données ouvertes du service Vélib' (disponibilité des vélos par station, mise à jour chaque minute) et la météo en **indicateurs exploitables** pour optimiser la répartition des vélos : repérer les stations vides ou saturées, comprendre les pics d'usage, anticiper la demande.

### 4.2 La donnée mise à disposition

La couche directement consultable (zone CURATED, table `stations_enrichies`) contient une ligne par station, enrichie. En langage métier :

| Donnée | Signification métier |
|---|---|
| Station | Nom et identifiant de la station Vélib' |
| Capacité | Nombre total de bornes de la station |
| Taux d'occupation | Part des bornes occupées par un vélo (0 % = station vide, 100 % = station pleine) |
| Vélos disponibles | Nombre de vélos prêts à être empruntés |
| Bornes libres | Nombre d'emplacements pour reposer un vélo |
| Météo | Température et conditions au moment du relevé (facteur explicatif de la demande) |
| Date du calcul | Horodatage du relevé |

### 4.3 Comment lire les tableaux de bord

Les tableaux de bord Power BI s'appuient sur ces données pour répondre à des questions métier concrètes :

- **Quelles stations sont en tension ?** Une station à **taux d'occupation proche de 100 %** ne peut plus accueillir de vélos rapportés ; une station **proche de 0 %** n'a plus de vélos à emprunter. Ce sont les deux situations à corriger par les équipes de régulation.
- **Quand a lieu la demande ?** Les pics typiques se situent aux heures de pointe (8 h–9 h, 18 h–19 h) ; le croisement avec la météo aide à expliquer les variations.
- **Quelles zones optimiser ?** La comparaison entre arrondissements / quartiers identifie les déséquilibres récurrents.

### 4.4 Fraîcheur et limites des données

- **Fraîcheur :** les données de disponibilité sont rafraîchies à la minute côté source ; la plateforme les traite en continu, avec une consolidation métier toutes les 2 minutes.
- **Périmètre :** les données sont **agrégées par station**, sans information individuelle sur les usagers (conformité RGPD : aucune donnée personnelle n'est traitée).
- **Dépendance :** la disponibilité dépend des APIs publiques Vélib' ; en cas d'indisponibilité de la source, le pipeline reprend automatiquement à la remise en service (cf. protocole de maintenance).

---

## 5. Rattachement aux autres livrables

| Élément | Document |
|---|---|
| Procédures de maintenance préventive / corrective | Compétence 8 — Protocole de maintenance |
| Mesures de coûts et performances de stockage | Compétence 7 — Reporting coûts/performances |
| Architecture cible et choix stratégiques d'origine | Bloc 1 — Rapport stratégie Big Data |

---

*Document à versionner dans le dépôt (`docs/`) et à présenter en soutenance comme livrable de la compétence 8 (volet documentation).*
