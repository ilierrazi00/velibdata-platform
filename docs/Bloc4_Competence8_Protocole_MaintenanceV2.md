# Compétence 8 — Protocole de maintenance et préservation de la documentation technique

**Projet VélibData — Plateforme Big Data**
**MSPR RNCP36921 — Bloc 4 « Administration et supervision d'une plateforme Big Data »**
**Équipe :** Ilias Errazi (Data Engineer / chef de projet) · Manal Jawhar (Data Analyst / sécurité-DLP) · Issmail Khouyi (Data Science / veille / Green IT)
**Version :** 1.0 — 29 juin 2026

---

## 1. Objet et portée

L'énoncé attend la rédaction d'un *« protocole de maintenance de la documentation technique afin de consigner et assurer la préservation de la solution choisie »*.

Ce document définit le protocole de maintenance de la plateforme VélibData dans son ensemble : il décrit les opérations de maintenance **préventive**, **corrective** et **évolutive**, leur fréquence, leur responsable et leur procédure d'exécution, ainsi que les modalités de **préservation de la documentation technique**. Il garantit la fiabilité de la solution tout au long de son cycle de vie et sa reprise en main par un tiers.

## 2. Périmètre : composants sous maintenance

| Couche | Composant | Rôle |
|---|---|---|
| Messagerie | Apache Kafka 3.9.0 (KRaft) | Bus d'ingestion temps réel |
| Traitement | Apache Spark 3.5.3 (streaming + batch) | RAW→CLEAN→CURATED |
| Stockage | MinIO (zones RAW / CLEAN / CURATED) | Data Lake objet S3-compatible |
| Orchestration | Kubernetes + KEDA | Distribution, self-healing, autoscaling |
| Qualité | Great Expectations | Validation des règles de données |
| Supervision | Prometheus + Kafka exporter + Grafana | Métriques et alertes |
| Sources | Producers Python (Vélib' dispo, stations, météo) | Collecte API |
| Restitution | Power BI | Tableaux de bord métier |

## 3. Typologie de la maintenance

Le protocole distingue trois types de maintenance, selon la norme usuelle de gestion de service :

- **Préventive** : opérations planifiées visant à éviter l'apparition de pannes ou la dégradation des performances (purges, compaction, sauvegardes, contrôles).
- **Corrective** : actions de rétablissement déclenchées par un incident (panne de composant, arrêt de pipeline, échec de qualité).
- **Évolutive** : mises à jour de versions, ajout de capacité, amélioration continue de l'architecture.

## 4. Maintenance préventive (planning)

Le tableau ci-dessous constitue le calendrier de maintenance préventive. Les opérations marquées « automatisée » s'exécutent sans intervention humaine.

| Opération | Fréquence | Mode | Responsable |
|---|---|---|---|
| Purge des données RAW > 30 jours | Quotidien (02:00) | **Automatisée** (CronJob `velib-retention-raw`) | Data Engineer |
| Validation qualité des données (6 règles) | À chaque batch | **Automatisée** (Great Expectations) | Data Analyst |
| Contrôle des dashboards de supervision et alertes | Quotidien | Manuel (visuel Grafana) | Data Engineer |
| Compaction des petits fichiers (zones CLEAN/CURATED) | Hebdomadaire | Semi-automatisée (job Spark dédié) | Data Engineer |
| Nettoyage des checkpoints Spark obsolètes | Mensuel | Semi-automatisée | Data Engineer |
| Suppression des répertoires `_temporary` orphelins | Hebdomadaire | Manuel / script | Data Engineer |
| Vérification du taux de remplissage des volumes (PVC / disque) | Hebdomadaire | Manuel (alerte Prometheus) | Data Engineer |
| Sauvegarde de la configuration (manifests, compose, IaC) | À chaque modification | **Automatisée** (commit Git) | Toute l'équipe |
| Veille CVE et mise à jour des images de base | Trimestriel | Manuel | Veille (Issmail) |

### 4.1 Procédures de maintenance préventive

**Purge de rétention RAW (automatisée).** Une CronJob Kubernetes purge chaque nuit les objets RAW de plus de 30 jours, conformément à la politique de conservation :

```sh
mc rm --recursive --force --older-than 30d velibdc/raw
```

**Compaction des petits fichiers (correction de l'anomalie n°1 du rapport Comp 7).** Le streaming Spark génère un grand nombre de petits fichiers Parquet (≈ 45 000 fichiers en zone CLEAN au 29/06/2026). Un job de compaction hebdomadaire relit chaque partition et la réécrit en un nombre réduit de fichiers, via un `coalesce`/`repartition` avant écriture, ce qui restaure les performances de lecture.

**Purge des checkpoints (correction de l'anomalie n°2).** Les fichiers de checkpoint de Spark Structured Streaming (≈ 35 000 fichiers) s'accumulent indéfiniment ; une purge mensuelle des commits/offsets obsolètes est planifiée, en conservant les checkpoints actifs nécessaires à la reprise.

**Nettoyage des répertoires de staging (correction de l'anomalie n°3).** Les répertoires `_temporary/0/` résiduels laissés par des jobs interrompus sont supprimés chaque semaine.

## 5. Maintenance corrective (runbook d'incidents)

Procédures de rétablissement, par type d'incident. Chaque entrée indique le symptôme, le diagnostic et l'action.

| Incident | Diagnostic | Action corrective |
|---|---|---|
| **Panne d'un nœud de stockage** (pod MinIO arrêté) | `kubectl get pods -n velibdata` montre un pod absent/`CrashLoop` | **Self-healing automatique** : Kubernetes recrée le pod (≈ 3 s, cf. compétence 2). Vérifier le retour à `Running`. Aucune perte (StatefulSet + PVC). |
| **Pipeline d'ingestion arrêté** (après reboot machine) | Aucun conteneur `velib-*` dans `docker ps` | Relancer la stack : `docker compose up -d`, puis vérifier `docker compose ps` (états `healthy`) et les logs Spark. |
| **Échec de validation qualité** | Great Expectations remonte < 6/6 règles | Identifier la règle en échec, inspecter la zone CLEAN, corriger la logique de nettoyage dans `clean_processing.py`, relancer le batch. |
| **Saturation d'un volume** | Alerte Prometheus sur taux de remplissage PVC/disque | Lancer la purge de rétention, supprimer les fichiers techniques obsolètes, et redimensionner le volume si nécessaire. |
| **Lag Kafka élevé** (retard de traitement) | Lag visible dans Grafana (Kafka exporter) | KEDA met automatiquement à l'échelle les workers Spark (1→4, cf. compétence 6). Si le lag persiste, vérifier la santé des producers. |
| **Connecteurs Spark indisponibles** | Erreur de téléchargement de packages au démarrage Spark | Le volume `spark-ivy` met en cache les connecteurs ; vérifier sa présence. En dernier recours, re-télécharger les packages référencés dans le `spark-submit`. |

## 5bis. Plan de récupération des données (Disaster Recovery)

Le runbook d'incidents (§5) couvre les pannes où la donnée reste intègre (pod redémarré, pipeline relancé). Cette section couvre le scénario distinct où la **donnée elle-même est perdue ou corrompue** — perte de volume, suppression accidentelle d'un bucket, corruption disque — conformément à l'exigence de la grille sur les *« plans de récupération des données »*.

### 5bis.1 Objectifs de récupération (RPO / RTO)

| Zone | RPO (perte de données maximale acceptable) | RTO (temps de restauration cible) | Justification |
|---|---|---|---|
| RAW | 24 h | 2 h | Rejouable depuis les APIs sources si la fenêtre de rétention le permet ; criticité moindre |
| CLEAN | 24 h | 2 h | Reconstructible par re-traitement Spark depuis RAW |
| CURATED | 1 h | 30 min | Zone directement exposée à Power BI ; criticité opérationnelle la plus forte |

### 5bis.2 Stratégie de sauvegarde

- **Réplication interne (première ligne de défense)** : MinIO est déployé en `StatefulSet` à 4 réplicas sur Kubernetes, ce qui protège déjà contre la perte d'un pod ou d'un nœud unique (cf. compétence 2). Cela ne protège **pas** contre une suppression logique (bucket supprimé par erreur, script `mc rm` mal ciblé) ni contre une corruption du volume `hostpath` sous-jacent.
- **Sauvegarde externe (miroir)** : une synchronisation périodique des buckets `raw`, `clean` et `curated` vers un second emplacement de stockage (second volume Docker, disque externe, ou bucket S3 secondaire en cas de passage cloud) via :

```sh
mc mirror --overwrite velibdc/curated backup-target/curated
mc mirror --overwrite velibdc/clean backup-target/clean
mc mirror --overwrite velibdc/raw backup-target/raw
```

- **Fréquence de sauvegarde** : quotidienne pour RAW et CLEAN (alignée sur la purge de rétention, §4), horaire pour CURATED (zone la plus critique et la plus petite en volume, donc peu coûteuse à sauvegarder fréquemment).
- **Rétention des sauvegardes** : 7 jours glissants, pour permettre un retour arrière en cas de corruption détectée tardivement (ex. bug de qualité de données non capté immédiatement par Great Expectations).

### 5bis.3 Procédure de restauration

En cas de perte ou corruption confirmée d'une zone :

1. **Isoler** : arrêter les jobs Spark écrivant vers la zone affectée, pour éviter d'écraser une sauvegarde valide par des données déjà corrompues.
2. **Diagnostiquer l'étendue** : comparer `mc ls --recursive --summarize` entre la zone affectée et son miroir de sauvegarde pour identifier la fenêtre de perte.
3. **Restaurer** :
```sh
mc mirror --overwrite backup-target/curated velibdc/curated
```
4. **Revalider** : relancer les règles Great Expectations sur la zone restaurée avant de reprendre l'ingestion.
5. **Reprendre** : redémarrer les jobs Spark ; pour CLEAN et CURATED, un re-traitement complet depuis RAW reste possible en dernier recours, RAW faisant foi comme source de vérité rejouable.

### 5bis.4 Cas particulier — RAW comme source de rejeu

La zone RAW conservant les données brutes non transformées, elle constitue elle-même un mécanisme de récupération pour CLEAN et CURATED : en cas de perte de ces deux zones et d'absence de sauvegarde à jour, un re-traitement Spark complet depuis RAW permet de reconstruire l'état de la plateforme, au prix d'un RTO plus long (durée du batch complet plutôt que quelques minutes).

*Piste d'évolution : automatiser le `mc mirror` via une CronJob Kubernetes dédiée (sur le modèle de `velib-retention-raw`), et tester la procédure de restauration au moins une fois avant la soutenance pour pouvoir en attester devant le jury.*

## 6. Rôles et responsabilités

| Membre | Rôle projet | Responsabilité maintenance |
|---|---|---|
| Ilias Errazi | Data Engineer / chef de projet | Maintenance technique (stockage, pipelines, Kubernetes), runbook d'incidents |
| Manal Jawhar | Data Analyst / sécurité-DLP | Qualité des données, conformité RGPD, sécurité des accès (NeuVector) |
| Issmail Khouyi | Data Science / veille / Green IT | Veille technologique, mises à jour de versions, optimisation Green IT |

La maintenance de la **configuration** (manifests, `docker-compose.yml`, IaC) est une responsabilité partagée, formalisée par le versionnement Git (§7).

## 7. Préservation de la documentation technique

La pérennité de la solution repose sur une documentation versionnée et reproductible, hébergée sur le dépôt Git du projet (`github.com/ilierrazi00/velibdata-platform`).

**Principes de préservation :**

- **Versionnement Git** : tout changement de code, de configuration ou de documentation est tracé par commit, ce qui garantit l'historique et la possibilité de revenir à un état antérieur.
- **Infrastructure as Code** : les manifestes Kubernetes (`k8s/` — namespace, MinIO, démonstration d'autoscaling, CronJob de rétention) et le `docker-compose.yml` décrivent l'infrastructure de manière déclarative et reproductible : la plateforme peut être reconstruite à l'identique à partir du dépôt. *Piste d'évolution : industrialiser ce déploiement via des charts Helm (paramétrage par environnement) et/ou du provisioning Terraform pour un passage en production multi-environnements.*
- **Documentation projet** : le `README.md` (procédure de démarrage) et le dossier `docs/` consignent l'architecture, les choix techniques et les procédures.
- **Validation continue** : le workflow GitHub Actions (`.github/workflows/`) valide le code à chaque `push` (cf. compétence 3), empêchant la dérive de la documentation par rapport au code réel.
- **Secrets** : les valeurs sensibles ne sont pas versionnées ; seul le modèle `.env.example` l'est. *Recommandation d'amélioration : chiffrer les secrets Kubernetes au repos (chiffrement etcd, ou solution type Sealed Secrets / Vault), le simple encodage base64 actuel n'étant pas un mécanisme de protection.*

**Convention de mise à jour :** toute opération de maintenance significative (corrective ou évolutive) donne lieu à une mise à jour de la documentation concernée dans le même commit, afin de maintenir la cohérence entre la solution et sa description.

## 8. Indicateurs de suivi de la maintenance

| Indicateur | Cible | Source |
|---|---|---|
| Disponibilité des pods de stockage | ≥ 99 % | Kubernetes / Grafana |
| Temps de rétablissement après panne de nœud | ≤ quelques secondes (self-healing) | Compétence 2 |
| Taux de réussite des règles qualité | 6/6 | Great Expectations |
| Nombre de fichiers par zone (détection du *small files*) | Stable après compaction | `mc ls --recursive` |
| Taux de remplissage des volumes | < 80 % | Prometheus |

## 9. Conclusion

Ce protocole couvre l'ensemble du cycle de vie de la plateforme VélibData : prévention planifiée (purges, compaction, contrôles), correction outillée par un runbook d'incidents, et préservation de la documentation par le versionnement et l'Infrastructure as Code. Il s'appuie sur les mécanismes déjà en place — self-healing Kubernetes, autoscaling KEDA, supervision Prometheus/Grafana, validation Great Expectations, CI GitHub Actions — et intègre les actions correctives issues du contrôle de stockage (compétence 7). La solution est ainsi maintenable, reproductible et pérenne.

---

*Document de référence à versionner dans le dépôt (`docs/`) et à présenter en soutenance comme livrable de la compétence 8.*
