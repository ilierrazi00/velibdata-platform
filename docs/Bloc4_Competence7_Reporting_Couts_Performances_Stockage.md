# Compétence 7 — Reporting des coûts et performances de stockage

**Projet VélibData — Plateforme Big Data**
**MSPR RNCP36921 — Bloc 4 « Administration et supervision d'une plateforme Big Data »**
**Équipe :** Ilias Errazi (Data Engineer / chef de projet) · Manal Jawhar (Data Analyst / sécurité-DLP) · Issmail Khouyi (Data Science / veille / Green IT)
**Date des mesures :** 29 juin 2026

---

## 1. Objet du livrable

L'énoncé attend que l'équipe *« contrôle la bonne application de la politique des données en analysant, traitant et reportant les coûts et les performances de stockage selon les différents critères (licences, supports, évolutivité, performance…) afin de s'assurer de la pérennité de la solution »*.

Ce document répond à cette compétence en quatre temps : la méthodologie de mesure employée, l'état des lieux chiffré du Data Lake, l'analyse selon les quatre critères imposés (licences, supports, évolutivité, performance), puis le contrôle de conformité, qui identifie les anomalies de stockage détectées et le plan d'action associé. Toutes les valeurs présentées sont **mesurées sur la plateforme en fonctionnement**, et non estimées.

## 2. Méthodologie de mesure

Les relevés ont été effectués pendant que le pipeline (Kafka → Spark → MinIO) tournait, sur l'instance MinIO de production (Docker Compose), à l'aide des outils suivants :

- **Volumétrie par zone** : client officiel `mc` (MinIO Client), commandes `mc du` et `mc ls --recursive --summarize` sur les trois buckets `raw`, `clean`, `curated`.
- **Composition des objets** : comptage par motif (`_checkpoint`, `_spark_metadata`, `.parquet`) sur la liste récursive de la zone CLEAN.
- **Taux de compression réel** : lecture des métadonnées des fichiers Parquet de la zone CURATED (bibliothèque PyArrow), puis comparaison de la même donnée réécrite en JSON et en CSV.

Cette traçabilité méthodologique garantit la reproductibilité des chiffres devant le jury.

## 3. État des lieux chiffré du Data Lake

Le Data Lake suit le patron médaillon en trois zones. Relevé du 29 juin 2026 :

| Zone | Volume | Nombre d'objets | Nature du contenu |
|---|---|---|---|
| RAW | 102 Mio | 1 443 | Données brutes ingérées (JSON), avant nettoyage |
| CLEAN | 229 Mio | ≈ 80 565 | Données nettoyées + fichiers techniques Spark (voir §5.4) |
| CURATED | 108 Kio | 6–7 | Table analytique `stations_enrichies` (1 511 lignes) |
| **Total** | **≈ 331 Mio** | **≈ 82 000** | — |

La zone CURATED contient la table `stations_enrichies`, qui joint les données de disponibilité Vélib' et les données météo (colonnes `stationcode`, `capacity`, `taux_occupation`, `meteo_temp`, `date_calcul`, etc.). Cette table dénormalisée est la couche directement exploitée par Power BI.

## 4. Analyse selon les quatre critères de l'énoncé

### 4.1 Licences

L'intégralité de la chaîne technologique repose sur des logiciels **open source**, ce qui ramène le coût de licence à **0 €** :

| Composant | Licence | Coût |
|---|---|---|
| MinIO (stockage objet) | GNU AGPLv3 | 0 € |
| Apache Kafka, Apache Spark | Apache 2.0 | 0 € |
| Format Parquet, codec Snappy | Apache 2.0 (formats ouverts) | 0 € |
| Prometheus, Grafana | Apache 2.0 / AGPLv3 | 0 € |
| Great Expectations | Apache 2.0 | 0 € |

Face à une pile équivalente en services managés propriétaires (Confluent Cloud pour Kafka, entrepôt type Snowflake, stockage objet facturé), ce choix supprime tout abonnement récurrent et tout risque de dépendance commerciale (*vendor lock-in*). Le format Parquet étant ouvert, les données restent lisibles par n'importe quel moteur (Spark, DuckDB, Trino, Pandas) sans licence.

### 4.2 Supports

Le stockage repose sur du **stockage objet S3-compatible hébergé localement** (volume Docker pour le pipeline de production, et `PersistentVolumeClaim` de type `hostpath` côté Kubernetes pour la démonstration distribuée). Les implications :

- **Coût de support quasi nul** : la donnée réside sur le disque d'une machine déjà possédée, sans facturation au Go.
- **Localisation des données en France** : le stockage étant local, la donnée ne quitte pas le territoire, ce qui satisfait directement la contrainte RGPD de l'énoncé (localisation UE) — un point de conformité à mettre en avant.
- **Limite identifiée** : les volumes Kubernetes sont dimensionnés à 1 Gio chacun (`hostpath`), valeur volontairement modeste pour la maquette académique mais à requalifier en cas de passage en production (voir §5.4).

### 4.3 Évolutivité

L'architecture est conçue pour absorber la montée en charge sans refonte :

- **Stockage distribué** : MinIO est déployé en `StatefulSet` à 4 réplicas sur Kubernetes (compétence 1). L'ajout de capacité se fait par ajout de nœuds/réplicas, sans migration de format.
- **Élasticité du traitement** : KEDA met à l'échelle automatiquement les workers Spark de 1 à 4 pods selon la charge (compétence 6), garantissant que l'ingestion suit la croissance du volume.
- **Format adapté à l'analytique à grande échelle** : Parquet est colonnaire et partitionnable, ce qui permet le *partition pruning* (lecture sélective) à mesure que les volumes augmentent.

### 4.4 Performance

Le format de stockage retenu (Parquet compressé en Snappy) a été comparé aux formats bruts sur la table CURATED réelle (1 511 lignes). **Gains de stockage mesurés :**

| Format | Taille | Gain vs JSON brut | Octets / ligne |
|---|---|---|---|
| JSON brut (NDJSON) | 521,8 Kio | référence | 354 o |
| CSV | 191,9 Kio | −63,2 % | 130 o |
| **Parquet + Snappy (solution retenue)** | **107,7 Kio** | **−79,4 %** | **73 o** |

**Le format Parquet+Snappy est 4,8× plus compact que le JSON brut (−79,4 %) et −43,9 % plus compact que le CSV.** À noter : l'étude stratégique du Bloc 1 anticipait un gain d'environ −40 % vs JSON ; la mesure terrain (−79 %) **dépasse cet objectif**, ce qui valide a posteriori le choix d'architecture.

> Précision technique : la compression Snappy seule (Parquet compressé vs Parquet non compressé) apporte ici un gain marginal, car le volume CURATED est faible et les colonnes peu redondantes. L'essentiel du gain provient du **format colonnaire Parquet** lui-même, pas uniquement du codec.

### 4.5 Supervision (monitoring) de l'espace de stockage et des performances

Le suivi continu des performances et de l'occupation du stockage est assuré par la pile **Prometheus + Grafana**, alimentée par le Kafka exporter et les métriques des conteneurs. Cette solution joue, dans l'architecture VélibData, le rôle que l'énoncé illustre par ElasticELK / APM : elle permet de suivre le débit du pipeline, de surveiller le taux de remplissage des volumes de stockage et de détecter les anomalies (retard de traitement, saturation, indisponibilité d'un composant) via des seuils d'alerte. Un dashboard de supervision dédié (« Supervision Pipeline ») est provisionné automatiquement au démarrage. Le stockage objet (MinIO) et le traitement (Spark) sont ainsi observables en continu, ce qui permet de remonter les problèmes de performance ou d'espace **avant** qu'ils n'affectent la pérennité de la solution.

## 5. Contrôle de conformité : anomalies détectées

Le rôle de cette compétence est aussi de *contrôler la bonne application de la politique des données*. L'analyse a révélé trois anomalies de stockage à corriger. Elles ne remettent pas en cause le fonctionnement de la plateforme, mais affectent sa performance et sa pérennité, et alimentent directement le protocole de maintenance (compétence 8).

### 5.1 Problème des « petits fichiers » (small files problem)

La zone CLEAN contient **44 757 fichiers Parquet** pour un volume de 229 Mio, soit une taille moyenne de l'ordre de quelques kilo-octets par fichier. De même, la table CURATED de 1 511 lignes est éclatée en 6 fichiers alors qu'un seul suffirait. Ce profil est la signature du *small files problem* de Spark Structured Streaming : à chaque micro-batch, de nouveaux petits fichiers sont écrits sans jamais être compactés.

**Impact :** ralentissement des opérations de listing objet, surcoût de métadonnées Parquet, dégradation des temps de requête analytique.

### 5.2 Accumulation des fichiers de checkpoint

La zone CLEAN comporte **35 651 fichiers de checkpoint** (`_checkpoint/commits`, `_checkpoint/offsets`), de 29 à 656 octets chacun, accumulés au fil des jours de streaming. Ces fichiers sont nécessaires à la reprise sur incident de Spark, mais leur nombre croît indéfiniment sans purge.

**Impact :** gonflement du nombre d'objets, complexité de sauvegarde et de supervision.

### 5.3 Répertoire de staging orphelin

La zone CURATED contient un répertoire `stations_enrichies/_temporary/0/` vide. Ce dossier est la zone de transit de Spark pendant l'écriture ; sa persistance indique un résidu d'un job interrompu, à nettoyer.

### 5.4 Sous-dimensionnement des volumes

Les `PersistentVolumeClaim` MinIO sur Kubernetes sont fixés à 1 Gio. Avec un débit RAW mesuré à environ 17 Mio/jour et une rétention de 30 jours, le besoin steady-state avoisine déjà 0,5 Gio pour la seule zone RAW ; le dimensionnement actuel laisse peu de marge en cas de montée en charge.

## 6. Reporting des coûts

Le coût direct de la solution est dominé par l'absence de licence et l'hébergement local. À titre de référence, voici l'estimation du coût de stockage si l'empreinte actuelle et ses projections étaient hébergées sur un stockage objet managé (AWS S3 Standard, région Paris `eu-west-3`, ≈ 0,024 USD/Go/mois) :

| Scénario | Volume | Coût stockage cloud estimé |
|---|---|---|
| Empreinte actuelle | ≈ 0,32 Go | ≈ 0,01 USD/mois |
| Steady-state rétention 30 j | ≈ 0,5 Go | ≈ 0,01 USD/mois |
| Montée en charge ×20 | ≈ 10 Go | ≈ 0,24 USD/mois |
| Projection 1 an, multi-sources | ≈ 100 Go | ≈ 2,40 USD/mois |
| **Licences logicielles (toute la stack)** | — | **0 €** |

**Lecture :** à l'échelle du projet, le coût de stockage objet est négligeable ; le poste réellement structurant serait les licences et les services managés dans une approche propriétaire — précisément ce que l'architecture open source supprime. La solution est donc économiquement pérenne, et l'estimation cloud ci-dessus fournit une borne haute crédible en cas de bascule future vers une infrastructure managée.

## 7. Plan d'action et recommandations (pérennité)

| # | Anomalie | Action corrective recommandée | Lien |
|---|---|---|---|
| 1 | Small files (45 k fichiers en CLEAN) | Job de **compaction** périodique (réécriture / `coalesce` / OPTIMIZE) pour regrouper les petits fichiers | Comp. 8 |
| 2 | Checkpoints accumulés (35 k) | Mise en place d'une **purge des checkpoints** obsolètes et/ou rotation | Comp. 8 |
| 3 | Répertoire `_temporary` orphelin | **Nettoyage des répertoires de staging** orphelins | Comp. 8 |
| 4 | PVC sous-dimensionnés (1 Gio) | **Redimensionnement** des volumes et alerte sur seuil de remplissage (déjà supervisé via Prometheus/Grafana) | Comp. 5 / 8 |

Ces actions sont consignées dans le protocole de maintenance (compétence 8), qui définit leur fréquence, leur responsable et leur procédure d'exécution.

## 8. Conclusion

Le stockage de VélibData est **économiquement pérenne** (licences nulles, coût de support négligeable, conformité RGPD par localisation locale) et **techniquement performant** (format Parquet+Snappy mesuré 4,8× plus compact que le JSON brut). L'architecture est **évolutive** par conception (MinIO distribué + autoscaling KEDA).

Le contrôle a néanmoins mis en évidence un enjeu de **maintenance préventive** — le *small files problem* et l'accumulation de fichiers techniques — qui n'altère pas le service mais doit être traité pour garantir la durabilité des performances. Ce constat constitue l'entrée directe du protocole de maintenance (compétence 8).

---

*Annexe — Commandes de mesure utilisées (reproductibilité) :*
`mc du velibdc/raw velibdc/clean velibdc/curated` · `mc ls --recursive --summarize velibdc/clean` · lecture des métadonnées Parquet via PyArrow · relevé du 29/06/2026.
