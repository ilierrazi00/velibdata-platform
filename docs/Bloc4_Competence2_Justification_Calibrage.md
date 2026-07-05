# Compétence 2 — Configuration du cluster et justification du calibrage

## Objectif

Cette compétence vise à configurer un cluster de nœuds avec une solution de stockage
distribuée afin d'assurer une **tolérance aux pannes**. Au-delà de la mise en œuvre, la
grille attend une **justification explicite des choix** : calibrage du nombre de nœuds,
taux de réplication retenu, et choix d'une machine virtuelle ou non. Cette section
détaille ces trois décisions et explique ce qu'elles apportent en matière de
disponibilité, de montée en charge et de répartition équilibrée de la charge.

## Choix de la solution : MinIO en mode distribué

Le stockage distribué de la plateforme repose sur **MinIO**, un stockage objet compatible
S3. Deux instances distinctes coexistent, avec des rôles clairement séparés :

- une instance **MinIO en Docker Compose**, mono-nœud, qui porte les données réelles du
  pipeline (zones RAW / CLEAN / CURATED) ;
- une instance **MinIO déployée en cluster distribué sur Kubernetes** (StatefulSet à
  **4 réplicas**), dédiée à la démonstration du stockage distribué, du *self-healing* et
  de l'autoscaling exigés par le Bloc 4.

Cette séparation est assumée : elle permet de démontrer proprement les mécanismes de
cluster distribué sans perturber le pipeline de production. La justification du calibrage
qui suit porte sur l'**instance distribuée Kubernetes**, celle qui répond directement au
critère de tolérance aux pannes.

## Justification du calibrage

| Paramètre | Valeur retenue | Justification |
|---|---|---|
| Nombre de nœuds | **4 réplicas** (StatefulSet) | Minimum pertinent pour un *erasure set* MinIO offrant à la fois de la parité et une répartition équilibrée ; permet de tolérer la perte de nœuds sans reconfiguration. |
| Schéma de réplication | **Erasure coding EC:2** (2 données + 2 parité) | Tolérance à la panne avec un coût de stockage maîtrisé, sans dupliquer intégralement chaque objet comme le ferait une réplication simple. |
| Machine virtuelle | **Non — conteneurs** (Docker / Kubernetes) | Provisioning rapide et reproductible, faible surcoût par rapport à des VM, isolation suffisante, cohérent avec une approche cloud-native. |

### Nombre de nœuds : pourquoi 4

Le choix de **4 nœuds** n'est pas arbitraire. MinIO organise le stockage distribué en
*erasure sets* : les objets sont découpés en fragments de données et de parité répartis
sur l'ensemble des nœuds. Quatre nœuds constituent le premier palier qui permet à la fois
d'appliquer un schéma de parité significatif (EC:2) et de répartir les fragments de manière
équilibrée sur plusieurs machines. Avec un seul nœud, aucune distribution réelle n'est
démontrable ; avec deux, la tolérance se limite à une copie miroir. Quatre nœuds offrent
une distribution authentique tout en gardant une empreinte matérielle maîtrisée, adaptée à
un environnement pédagogique.

### Taux de réplication : erasure coding plutôt que réplication simple

Plutôt qu'une réplication par copie intégrale (facteur 2 ou 3), la plateforme retient
l'**erasure coding**, mécanisme natif de MinIO. Avec 4 nœuds, le schéma par défaut est
**EC:2** : chaque objet est réparti en 2 fragments de données et 2 fragments de parité.

Concrètement, cela signifie :

- la plateforme tolère la **perte d'un nœud complet** sans interruption ni perte de
  données, en lecture comme en écriture ;
- elle continue de **servir les lectures** même en cas de perte simultanée de deux nœuds,
  grâce aux fragments de parité.

L'erasure coding présente un double avantage face à la réplication par blocs classique :
la tolérance ne dépend pas de la survie d'une **copie précise** (n'importe quels nœuds
peuvent tomber, tant que le quorum est atteint), et l'occupation de stockage reste
maîtrisée puisqu'on stocke de la parité plutôt que des copies entières. C'est un choix
plus souple et plus efficace en volume qu'une simple réplication à facteur fixe.

### Machine virtuelle ou conteneurs : le choix des conteneurs

La plateforme n'utilise **pas de machines virtuelles** : chaque nœud du cluster est un
**conteneur** orchestré par Kubernetes (StatefulSet), chaque pod disposant de son propre
volume persistant (PVC). Ce choix se justifie par :

- un **provisioning rapide et reproductible** : un nœud se recrée en quelques secondes,
  sans installation d'un système d'exploitation complet ;
- un **surcoût minimal** : les conteneurs partagent le noyau de l'hôte, contrairement aux
  VM qui embarquent chacune un OS entier ;
- une **isolation suffisante** pour le besoin, avec une densité bien supérieure sur la même
  machine ;
- une **cohérence cloud-native** : le même mécanisme (StatefulSet, sondes, PVC) serait
  transposable tel quel sur un cluster Kubernetes managé.

## Ce que le calibrage apporte

**Disponibilité des ressources.** L'erasure coding EC:2 combiné au *self-healing* de
Kubernetes assure la continuité de service : lorsqu'un pod MinIO tombe, le StatefulSet le
recrée automatiquement (reconstruction observée en **~3 secondes**), et MinIO reconstruit
les fragments manquants à partir de la parité. Aucune intervention manuelle n'est requise.

**Montée en charge.** Le déploiement en StatefulSet permet d'augmenter la capacité en
ajoutant des nœuds, tandis que la couche de calcul est dimensionnée séparément via
l'autoscaling (KEDA). Le stockage et le calcul évoluent ainsi indépendamment, chacun selon
son besoin.

**Répartition équilibrée de la charge.** MinIO distribue les fragments de données et de
parité de manière déterministe sur l'ensemble des 4 nœuds. Aucun nœud ne concentre la
totalité d'un objet : la charge de lecture / écriture est naturellement répartie, ce qui
évite les points chauds et améliore le débit agrégé.

## Preuve

*Captures à insérer :*

- **`Comp2_minio_4pods.png`** — les 4 pods MinIO actifs dans le namespace `velibdata`
  (`kubectl get pods`), prouvant le cluster à 4 nœuds.
- **`Comp2_self_healing.png`** — suppression d'un pod puis recréation automatique par le
  StatefulSet, illustrant la reprise sans intervention.

## Limites assumées

Le cluster distribué à 4 nœuds démontre pleinement la tolérance aux pannes et la
distribution du stockage. Deux limites sont assumées honnêtement :

- l'instance distribuée sert la **démonstration** du stockage tolérant aux pannes ; les
  données réelles du pipeline transitent par l'instance MinIO Docker Compose, mono-nœud ;
- le dimensionnement (4 nœuds, EC:2) est calibré pour un environnement pédagogique. Une
  bascule vers un hébergement cloud managé permettrait d'augmenter le nombre de nœuds et
  d'ajuster le schéma de parité pour un contexte de production.

Ces limites ne remettent pas en cause la démonstration : les mécanismes de tolérance aux
pannes, de réplication par parité et de répartition de charge sont réellement en place et
observables sur le cluster.
