# Test de résilience MinIO — compétence 2

## Objectif

Démontrer autre chose que la simple recréation d'un pod : la lecture d'un objet existant, l'écriture d'un nouvel objet et l'intégrité SHA-256 doivent rester valides lorsqu'une instance MinIO est indisponible.

## Prérequis

1. Kubernetes activé dans Docker Desktop.
2. Un fichier `.env` local contenant `MINIO_ROOT_USER` et `MINIO_ROOT_PASSWORD`.
3. `kubectl` accessible depuis PowerShell.

## Déploiement sécurisé

```powershell
powershell -ExecutionPolicy Bypass -File scripts/deploy-minio-k8s.ps1
```

Les identifiants ne sont plus écrits en clair dans le manifeste Git. Le script crée le Secret Kubernetes depuis le fichier `.env` local.

## Exécution du test

```powershell
powershell -ExecutionPolicy Bypass -File scripts/test-minio-resilience.ps1
```

Le test :

1. rétablit quatre pods MinIO ;
2. dépose un objet de 5 Mio et calcule son SHA-256 ;
3. réduit volontairement le StatefulSet de quatre à trois pods ;
4. relit l'objet et compare son SHA-256 ;
5. écrit puis relit un second objet pendant le mode dégradé ;
6. rétablit quatre pods ;
7. vérifie de nouveau les deux hashes ;
8. produit une preuve horodatée dans `evidence/`.

## Résultats à capturer pour le jury

- les lignes `BASELINE PASS`, `DEGRADED READ PASS`, `DEGRADED WRITE PASS` ;
- l'état `mc admin info` pendant la panne ;
- le retour des quatre pods en `Running/Ready` ;
- `RÉSULTAT GLOBAL : PASS` ;
- le temps total du mode dégradé.

## Limite à annoncer honnêtement

Le test démontre la continuité lors de la perte logique d'un pod/volume MinIO. Les quatre pods restent hébergés par le même nœud Docker Desktop : la perte complète de l'ordinateur hôte n'est pas couverte.
