# Bloc 4 — Compétence 3 : CI, build Docker et smoke tests

## Objectif

La chaîne automatisée vérifie chaque modification du dépôt dans cet ordre :

1. validation des fichiers Docker Compose et de la syntaxe Python ;
2. exécution des tests unitaires Spark ;
3. construction des images Docker des producers et de Great Expectations ;
4. déploiement temporaire d'une stack technique isolée ;
5. smoke tests sur Kafka, MinIO, Prometheus, Grafana et kafka-exporter ;
6. publication des rapports comme artefacts GitHub Actions ;
7. suppression automatique de l'environnement temporaire.

Cette chaîne est un **déploiement de validation éphémère**, pas un déploiement de production.

## Fichiers ajoutés

- `.github/workflows/ci.yml` : pipeline GitHub Actions en trois étapes ;
- `docker-compose.smoke.yml` : stack isolée sans appel aux API Vélib et météo ;
- `scripts/ci-smoke-tests.sh` : tests utilisés sur le runner Linux GitHub ;
- `scripts/smoke-tests.ps1` : même démonstration reproductible sous Windows ;
- `evidence/` : rapports horodatés.

## Vérifications réalisées

Le smoke test confirme automatiquement :

- Kafka sain ;
- création d'un topic, production puis consommation d'un message témoin ;
- MinIO sain ;
- présence des buckets RAW, CLEAN et CURATED ;
- écriture et relecture d'un objet dans RAW ;
- endpoint `/-/ready` de Prometheus ;
- endpoint `/api/health` de Grafana ;
- exposition de la métrique `kafka_brokers` par kafka-exporter.


## Exécution de toute la chaîne en local

Pour valider le build Docker puis déployer et tester la stack temporaire en une seule commande :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\cicd-local.ps1
```

Pour inclure également les tests unitaires locaux :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\cicd-local.ps1 -RunUnitTests
```

Le rapport global est créé dans `evidence/cicd-local-YYYYMMDD-HHMMSS.txt`.

## Exécution locale sous Windows

Depuis la racine du projet :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\smoke-tests.ps1
```

Le script utilise des ports dédiés pour éviter de modifier la stack principale :

- MinIO : `19000` ;
- console MinIO : `19001` ;
- Prometheus : `19090` ;
- Grafana : `13000` ;
- kafka-exporter : `19308`.

Il détruit automatiquement sa stack et ses volumes temporaires à la fin. Pour la conserver afin de prendre des captures :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\smoke-tests.ps1 -KeepRunning
```

Puis nettoyage manuel :

```powershell
docker compose --project-name velibdata-smoke --file docker-compose.smoke.yml down --volumes --remove-orphans
```

## Preuves attendues

Le rapport local est créé sous la forme :

```text
evidence/ci-smoke-YYYYMMDD-HHMMSS.txt
```

La dernière ligne doit être :

```text
RESULTAT GLOBAL : PASS
```

Dans GitHub, deux artefacts sont publiés pour chaque exécution :

- résultats `pytest` ;
- rapport du build Docker et des smoke tests.

## Limite déclarée

La chaîne automatise compilation/validation, tests, construction et déploiement temporaire. Elle ne pousse pas encore les images dans un registre et ne déploie pas sur un cluster distant de production.
