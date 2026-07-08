# Contrôle qualité automatisé et quarantaine — VélibData

## Objectif

Le contrôle qualité ne doit pas seulement afficher une erreur. Il doit empêcher les lignes invalides d'atteindre la zone de données acceptées, conserver une preuve et isoler les anomalies pour correction.

## Chaîne mise en œuvre

1. Lecture de fichiers Parquet ou CSV depuis MinIO.
2. Contrôles Great Expectations : présence de `stationcode`, bornes du taux d'occupation, de la capacité, du nombre de vélos et de la température.
3. Contrôles ligne par ligne, notamment `numbikesavailable <= capacity`.
4. Routage des lignes valides vers une zone acceptée.
5. Routage des lignes invalides vers une zone `quarantine` avec la colonne `_quality_errors`.
6. Génération d'un rapport JSON dans MinIO et dans le dossier local `evidence`.

## Test reproductible

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-data-quality-quarantine.ps1
```

Le jeu de démonstration contient huit lignes : cinq sont valides et trois comportent volontairement des anomalies.

Résultat attendu :

```text
INPUT_ROWS=8
ACCEPTED_ROWS=5
QUARANTINED_ROWS=3
RESULTAT GLOBAL : PASS
```

Le statut `PASS_WITH_QUARANTINE` signifie que le contrôle a détecté les anomalies, que les lignes incorrectes n'ont pas rejoint la sortie acceptée et qu'elles ont été conservées pour analyse.

## Preuves générées

- `evidence/data-quality-quarantine-<date>.txt`
- `evidence/data-quality-report-<date>.json`
- objet accepté dans MinIO sous `curated-quality/quality-test/...`
- objet invalide dans MinIO sous `quarantine/quality-test/...`
- rapport dans MinIO sous `quality-reports/quality-test/...`

## Limite

Le test porte sur un jeu contrôlé. En production, les règles doivent évoluer avec le contrat de données, le métier et les changements de schéma.

## Formulation pour la soutenance

> Great Expectations contrôle automatiquement nos règles de qualité. Les lignes conformes sont acceptées, tandis que les lignes incorrectes sont isolées dans une zone de quarantaine avec la cause de l'erreur. Un rapport JSON permet de prouver le nombre de lignes contrôlées, acceptées et rejetées.
