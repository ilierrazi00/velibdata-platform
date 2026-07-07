param(
    [string]$EnvFile = ".env",
    [string]$Namespace = "velibdata"
)

$ErrorActionPreference = "Stop"

& "$PSScriptRoot/create-k8s-secret.ps1" -EnvFile $EnvFile -Namespace $Namespace
if ($LASTEXITCODE -ne 0) { throw "Échec de création du secret." }

kubectl apply -f k8s/01-minio.yaml | Out-Host
kubectl apply -f k8s/04-minio-pdb.yaml | Out-Host
kubectl -n $Namespace rollout status statefulset/minio --timeout=300s | Out-Host

Write-Host "MinIO distribué prêt :" -ForegroundColor Green
kubectl -n $Namespace get pods -l app=minio -o wide
