# Bloc 4 - Sauvegarde, restauration, RPO, RTO et intégrité MinIO

## Objectif

Démontrer une procédure de reprise reproductible sur le cluster MinIO Kubernetes :

1. écrire des objets représentatifs des zones RAW, CLEAN et CURATED ;
2. créer une sauvegarde locale transportable ;
3. simuler un incident par suppression contrôlée ;
4. restaurer les objets ;
5. comparer les empreintes SHA-256 ;
6. mesurer le RPO et le RTO observés.

## Périmètre sécurisé

Le test n'efface pas les données applicatives existantes. Il utilise un préfixe dédié :

```text
dr-test/<horodatage>
```

à l'intérieur des buckets `raw`, `clean` et `curated`.

## Prérequis

Le cluster MinIO Kubernetes doit être disponible et un port-forward doit rester ouvert :

```powershell
kubectl port-forward -n velibdata svc/minio 19000:9000
```

Le script lit les identifiants dans le Secret Kubernetes `minio-creds` sans afficher leurs valeurs.

## Test reproductible

Depuis la racine du dépôt :

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test-minio-backup-restore.ps1
```

Le script utilise l'image officielle MinIO Client, produit une archive ZIP locale et un manifeste SHA-256 dans :

```text
%TEMP%\velibdata-minio-dr\<horodatage>\
```

Le rapport horodaté est créé dans :

```text
evidence\minio-backup-restore-<horodatage>.txt
```

## Indicateurs

- **RPO observé** : nombre d'objets et d'octets absents de la sauvegarde par rapport au dernier état écrit. L'objectif du test est `0 objet / 0 octet perdu`.
- **RTO observé** : temps écoulé entre le début de la suppression contrôlée et la fin de la restauration avec vérification SHA-256.
- **Intégrité** : l'empreinte SHA-256 de chaque objet restauré doit être identique à celle de l'original.

## Résultat attendu

```text
PASS - sauvegarde locale créée
PASS - incident simulé
PASS - intégrité SHA-256 RAW
PASS - intégrité SHA-256 CLEAN
PASS - intégrité SHA-256 CURATED
RPO OBSERVE : 0 objet et 0 octet perdus
RTO OBSERVE : ... secondes
RESULTAT GLOBAL : PASS
```

## Limites annoncées

- Le cluster Kubernetes est mono-nœud sous Docker Desktop.
- La sauvegarde est conservée sur le même poste physique : elle prouve la procédure et la mesure, mais pas une sauvegarde hors site.
- Le test porte sur un jeu contrôlé RAW/CLEAN/CURATED afin de ne pas supprimer les données applicatives existantes.
- En production, l'archive devrait être chiffrée, versionnée et répliquée vers une autre zone ou un autre fournisseur.
