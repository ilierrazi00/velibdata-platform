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
| Orchestration | Kubernetes + KEDA | Distribution, self-healing ; démonstration d'autoscaling sur déploiement représentatif (cf. §4ter) |
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

## 4bis. Justification du calibrage de l'infrastructure distribuée

Conformément à l'exigence de justification de la compétence 2, les choix de configuration du cluster de stockage sont argumentés ci-dessous.

**Nombre de réplicas (4).** MinIO est déployé à 4 réplicas plutôt que le minimum de 3 généralement recommandé pour un quorum distribué. Ce choix conserve une marge de tolérance : avec 4 nœuds, la perte simultanée de 2 réplicas laisse encore un quorum fonctionnel (contre un seul nœud de marge avec 3), ce qui sécurise la démonstration de tolérance de panne en contexte pédagogique où plusieurs tests peuvent être exécutés à la suite. Un cluster à 3 réplicas aurait suffi pour la seule tolérance zéro panne testée (perte d'un pod), mais 4 nœuds illustrent mieux le principe d'*auto-balancing* (répartition sur un nombre pair, cf. compétence 5) sans complexité de déploiement supplémentaire sur Docker Desktop.

**Taux de réplication.** Le facteur de réplication effectif est celui du `StatefulSet` (4 copies de chaque objet réparties sur les réplicas), sans réplication supplémentaire au niveau applicatif : au vu du volume de données du projet (quelques centaines de Mio), une réplication à ce niveau est largement suffisante et évite la complexité d'un facteur de réplication configurable par bucket, pertinent seulement à plus grande échelle.

**Absence de machines virtuelles dédiées.** Le choix a été fait de conteneuriser directement (Docker Desktop + Kubernetes intégré) plutôt que de provisionner des VM séparées par nœud. Justification : à l'échelle de la maquette académique, des VM ajouteraient une couche de virtualisation redondante avec l'isolation déjà apportée par les conteneurs, sans bénéfice de sécurité ou de performance mesurable, tout en consommant davantage de ressources sur une machine de développement (32 Go de RAM partagés). Ce choix reste documenté comme un arbitrage : en environnement de production réel, un provisioning de VM (ou de nœuds cloud managés) par souci d'isolation renforcée resterait une évolution pertinente.

## 4ter. Intégrité des données lors de la réplication des services (compétence 6)

L'autoscaling KEDA (1→4 pods) porte exclusivement sur les **workers Spark**, qui sont des composants de calcul *stateless* : ils ne persistent aucune donnée localement, et leur duplication ou destruction ne pose donc aucun risque d'incohérence — chaque nouveau pod reprend simplement sa part de traitement depuis Kafka (offsets) ou depuis le Data Lake, sans état à synchroniser entre instances.

La donnée elle-même n'est jamais répliquée par l'autoscaling : elle réside exclusivement dans MinIO, dont la réplication est gérée séparément et en continu par le `StatefulSet` à 4 réplicas fixes (cf. compétences 1 et 2), avec intégrité garantie par les mécanismes S3 natifs (checksums/ETag, cf. §4.5.1). Ce découplage est un choix d'architecture délibéré : **ne jamais scaler la couche de stockage à la volée** évite précisément les problèmes classiques de cohérence qu'impliquerait la réplication dynamique d'une base de données pendant un événement de charge (fenêtres d'incohérence, conflits d'écriture concurrente). La compétence de « maîtrise du partage de la donnée » exigée par la grille est ainsi assurée en amont, au niveau du stockage stable, plutôt qu'au niveau des workers éphémères qui, eux, n'ont par nature aucune donnée à protéger.

*Précision de périmètre : la démonstration d'autoscaling KEDA (1→4 pods) s'effectue sur un déploiement Kubernetes représentatif (`velib-worker`), distinct du job Spark réel qui tourne en Docker Compose (cf. §2). Ce choix isole la preuve du mécanisme d'orchestration de l'infrastructure de production, sans coupler cette dernière à l'environnement de démonstration K8s. En production, le même mécanisme s'appliquerait au job réel, avec le lag Kafka comme déclencheur plutôt que le CPU générique utilisé ici.*

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

### 5.1 Cas réel documenté — Blocage silencieux d'écriture et reprise sur incident

Le tableau ci-dessus décrit les procédures génériques par type d'incident. Cette section documente un **incident réel survenu et résolu** pendant l'exploitation de la plateforme, illustrant concrètement la réexécution des tâches (cf. 5bis.5) au-delà du cas théorique.

**Chronologie de l'incident :**

| Horodatage | Événement |
|---|---|
| 2026-06-29 22:41:29 | Dernier commit de checkpoint sain sur le flux `station_status` (n° 411) |
| 2026-07-02 → 2026-07-03 | Déconnexions réseau répétées entre Spark et Kafka (`DisconnectException: node 0 being disconnected`), provoquant des micro-batches anormalement longs (jusqu'à 2h07 au lieu de 30 s) |
| 2026-07-03 22:25 | Un premier redémarrage isolé (`docker compose restart spark`) relance le process, visible comme `RUNNING` dans la Spark UI (`/StreamingQuery/`) avec un débit apparent élevé, mais **aucune nouvelle écriture n'atteint MinIO** — aucun nouveau commit de checkpoint après plus de 35 minutes |
| 2026-07-04 00:58 | Diagnostic écarté : ni panne réseau (test `/dev/tcp` réussi), ni Kafka en erreur (logs Kafka propres), ni exception explicite dans les logs Spark — le blocage reste silencieux |
| 2026-07-04 00:58 | Action corrective : redémarrage complet du stack (`docker compose down && docker compose up -d`) plutôt qu'un redémarrage isolé, pour repartir sur un état cohérent de l'ensemble des composants |
| 2026-07-04 01:01:35 | **Reprise confirmée** : nouveau commit de checkpoint n° 412, soit exactement la suite du n° 411 — aucune perte, aucun retraitement complet depuis l'origine |

**Enseignement retenu :** un redémarrage isolé d'un seul composant (`restart spark`) peut laisser le système dans un état incohérent lorsque l'incident touche plusieurs composants interdépendants (ici, une coupure réseau ayant affecté à la fois Kafka et Spark). Dans ce cas, un redémarrage complet et ordonné de l'ensemble de la stack (`docker compose down` puis `up -d`, qui respecte l'ordre de démarrage défini par les `depends_on`) s'est révélé plus fiable qu'un redémarrage ciblé. Cette observation est intégrée au runbook : **en cas de blocage silencieux persistant après un redémarrage isolé (absence de progression du checkpoint malgré un statut `RUNNING`), l'action de second niveau recommandée est un redémarrage complet de la stack plutôt qu'une investigation prolongée**, le coût d'un redémarrage complet étant faible comparé au temps de diagnostic d'un blocage sans erreur explicite.

Cet incident constitue une preuve opérationnelle, non simulée, du mécanisme de réexécution des tâches décrit en 5bis.5 : le checkpoint Spark a permis une reprise exacte au point d'interruption, sur un cas réel de production plutôt qu'un test provoqué.

**Effet de bord détecté et corrigé — cohérence checkpoint/topic après redémarrage complet.**

L'action corrective décrite ci-dessus (`docker compose down && up -d`) a résolu le blocage d'écriture sur `station_status`/CLEAN, mais a eu un effet de bord non anticipé : elle a réinitialisé les topics Kafka sous-jacents (les offsets disponibles sont repartis d'une valeur basse), rendant incohérents les checkpoints du job RAW (`velib-spark`), qui référençaient encore les anciens offsets.

**Chronologie du second épisode :**

| Horodatage | Événement |
|---|---|
| 2026-07-04 01:01 | Redémarrage complet effectué (cf. 5.1), job CLEAN/CURATED repris avec succès |
| 2026-07-04 ~01:05 | Contrôle de routine : le conteneur `velib-spark` (job RAW) est constaté `Exited (1)` |
| 2026-07-04 ~01:06 | Diagnostic : `docker logs velib-spark` révèle une `StreamingQueryException` — `Partition velib.station_information-0's offset was changed from 272492 to 1500, some data may have been missed` |
| 2026-07-04 ~01:14 | Correction ciblée du topic `station_information` (purge du checkpoint), mais **récidive** sur `station_status` au redémarrage (`offset changed from 2752695 to 4500`) — preuve que l'incohérence touchait l'ensemble des topics, pas un seul |
| 2026-07-04 ~01:18 | Purge complète des checkpoints RAW pour les trois topics (`station_information`, `station_status`, `weather`) |
| 2026-07-04 01:20:26 | Redémarrage de `velib-spark` : les trois requêtes de streaming repartent sans exception |
| 2026-07-04 01:21:05 | Premier commit de checkpoint confirmé (`0`), pipeline RAW pleinement opérationnel |

**Enseignement retenu :** Spark Structured Streaming refuse par défaut de continuer un flux si l'offset

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

### 5bis.5 Réexécution des tâches (reprise sur incident)

Au-delà de la restauration de données (5bis.1 à 5bis.4), la plateforme assure la **réexécution des tâches interrompues**, exigence distincte de la grille d'évaluation. Ce mécanisme repose sur les checkpoints de Spark Structured Streaming : à chaque micro-batch, Spark persiste dans la zone CLEAN (`_checkpoint/commits`, `_checkpoint/offsets`) l'état d'avancement de la consommation Kafka.

En cas d'interruption d'un job (crash du conteneur Spark, redémarrage machine), le redémarrage du job (`docker compose up -d`) fait reprendre Spark **exactement à l'offset Kafka du dernier micro-batch validé**, sans retraitement ni perte des messages déjà consommés — la réexécution repart du point d'interruption plutôt que depuis zéro. C'est ce même mécanisme qui explique la présence des ≈ 35 000 fichiers de checkpoint identifiés en compétence 7, dont la purge périodique (§4) est calibrée pour conserver uniquement les checkpoints nécessaires à cette reprise, sans accumulation indéfinie.

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
