# Bloc 4 - Competence 6 : autoscaling KEDA sur consumer lag Kafka

## Objectif

Remplacer la preuve initiale fondee sur une charge CPU artificielle par un autoscaling
**evenementiel reel**. KEDA observe le retard du consumer group `velib-keda-workers`
sur le topic `velib.keda.demo` et pilote le nombre de pods du Deployment
`velib-worker`.

## Architecture de la preuve

- Kafka KRaft de demonstration : `docker-compose.keda.yml` ;
- acces depuis Kubernetes : `host.docker.internal:29092` ;
- topic : `velib.keda.demo`, 5 partitions ;
- consumer group : `velib-keda-workers` ;
- workers : consommateurs Python reels avec commit manuel apres traitement ;
- declencheur KEDA : `type: kafka` ;
- seuil : 20 messages de lag par replica ;
- bornes : 1 a 5 replicas ;
- scale-down apres drainage complet et fenetre de stabilisation.

Le maximum de 5 replicas correspond aux 5 partitions du topic. Cette limite evite de
creer des consumers inactifs qui n'auraient aucune partition a traiter.

## Test reproductible

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-keda-kafka-autoscaling.ps1
```

Le script :

1. lance le broker Kafka dedie ;
2. cree le topic a 5 partitions ;
3. deploie le vrai worker consommateur ;
4. initialise le consumer group ;
5. injecte 300 messages ;
6. mesure le lag ;
7. prouve le scale-out vers plusieurs pods ;
8. attend le traitement complet ;
9. prouve le scale-down automatique a un pod ;
10. genere une preuve horodatee dans `evidence/`.

## Resultats attendus

```text
PASS - consumer lag reel detecte
SCALE-OUT PASS
PASS - backlog Kafka entierement traite ; lag=0
SCALE-DOWN PASS
RESULTAT GLOBAL : PASS
```

## Integrite et partage de la donnee

Les workers sont stateless. L'etat de progression est gere par les offsets du consumer
group Kafka. Chaque partition est affectee a un seul consumer actif du groupe a un
instant donne. Le stockage durable reste separe dans MinIO ; l'autoscaling ne replique
pas la couche de stockage.

## Limite annoncee

La demonstration s'execute sur Docker Desktop Kubernetes mono-noeud. Elle prouve le
mecanisme d'autoscaling evenementiel et la consommation parallele, mais pas l'ajout
automatique de machines physiques au cluster.
