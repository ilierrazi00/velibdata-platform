# VélibData — Haute disponibilité Kafka à 3 brokers

## Objectif

Le Kafka principal de développement utilise un seul broker et un facteur de réplication égal à 1. Le démonstrateur HA ajoute un cluster Kafka KRaft isolé composé de trois brokers afin de vérifier la continuité de production et de lecture après la perte volontaire d'un broker.

Le démonstrateur ne remplace pas automatiquement le Kafka principal de `docker-compose.yml`. Il permet de prouver la configuration et le comportement de la tolérance aux pannes sans risquer les flux Vélib existants.

## Configuration testée

- 3 nœuds Kafka 3.9.0 en mode KRaft combiné broker/contrôleur ;
- quorum de contrôleurs à 3 membres ;
- topic `velib.ha.test` à 3 partitions ;
- facteur de réplication : 3 ;
- `min.insync.replicas=2` ;
- producteurs configurés avec `acks=all` ;
- volumes Docker séparés pour chaque broker.

## Scénario automatique

Le script `scripts/test-kafka-ha.ps1` :

1. crée un cluster HA propre et isolé ;
2. attend que les trois brokers soient sains ;
3. crée le topic répliqué et vérifie `ISR=3` ;
4. produit 60 messages avec `acks=all` ;
5. arrête volontairement le broker leader de la partition 0 ;
6. vérifie la réélection des leaders et `ISR=2` ;
7. produit 60 messages supplémentaires pendant la panne ;
8. lit les 120 messages pendant la panne ;
9. compare un SHA-256 après tri pour vérifier l'absence de perte et de doublon ;
10. redémarre le broker, attend la resynchronisation `ISR=3` et mesure le temps de récupération.

## Exécution

```powershell
docker compose --project-name velibdata-kafka-ha --file docker-compose.kafka-ha.yml config --quiet

powershell -ExecutionPolicy Bypass `
  -File .\scripts\test-kafka-ha.ps1
```

Le rapport est créé sous :

```text
evidence/kafka-ha-YYYYMMDD-HHMMSS.txt
```

Après les captures, arrêter le démonstrateur avec :

```powershell
docker compose `
  --project-name velibdata-kafka-ha `
  --file docker-compose.kafka-ha.yml `
  down --volumes --remove-orphans
```

## Critères de réussite

- trois brokers `healthy` ;
- trois copies synchronisées de chaque partition avant l'incident ;
- deux copies synchronisées et leaders disponibles pendant la panne ;
- production avec `acks=all` toujours fonctionnelle avec un broker arrêté ;
- 120 messages lus pendant la panne, sans perte ni doublon ;
- SHA-256 attendu et observé identiques ;
- retour à trois ISR après redémarrage ;
- `RESULTAT GLOBAL : PASS`.

## Portée et limite

Le test démontre la perte d'un broker/conteneur sur une machine locale. Les trois brokers partagent toujours le même ordinateur et le même moteur Docker. Il ne démontre donc pas la résistance à la perte complète de la machine ou du site. En production, les brokers seraient placés sur des machines ou zones de disponibilité distinctes, avec authentification, chiffrement, supervision et sauvegardes adaptés.

## Formulation pour la soutenance

> Nous avons déployé un cluster Kafka KRaft à trois brokers avec un facteur de réplication de 3 et un minimum de 2 réplicas synchronisés. Nous avons arrêté volontairement le broker leader d'une partition. Les leaders ont été réélus, la production et la lecture ont continué avec `acks=all`, puis le broker a rejoint le cluster et les trois copies se sont resynchronisées. L'intégrité des 120 messages a été vérifiée par SHA-256.
