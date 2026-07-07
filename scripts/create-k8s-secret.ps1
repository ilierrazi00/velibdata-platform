param(
    [string]$EnvFile = ".env",
    [string]$Namespace = "velibdata"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "kubectl est introuvable. Active Kubernetes dans Docker Desktop puis réessaie."
}

if (-not (Test-Path $EnvFile)) {
    throw "Fichier $EnvFile introuvable. Copie .env.example vers .env et renseigne les valeurs."
}

$values = @{}
Get-Content $EnvFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $parts = $line.Split("=", 2)
        $values[$parts[0].Trim()] = $parts[1].Trim()
    }
}

foreach ($key in @("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD")) {
    if (-not $values.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($values[$key])) {
        throw "La variable $key est absente ou vide dans $EnvFile."
    }
}

kubectl apply -f k8s/00-namespace.yaml | Out-Host
if ($LASTEXITCODE -ne 0) { throw "Échec de création du namespace." }

$secretYaml = kubectl -n $Namespace create secret generic minio-creds `
    --from-literal="MINIO_ROOT_USER=$($values['MINIO_ROOT_USER'])" `
    --from-literal="MINIO_ROOT_PASSWORD=$($values['MINIO_ROOT_PASSWORD'])" `
    --dry-run=client -o yaml
if ($LASTEXITCODE -ne 0) { throw "Échec de génération du secret Kubernetes." }

$secretYaml | kubectl apply -f - | Out-Host
if ($LASTEXITCODE -ne 0) { throw "Échec d'application du secret Kubernetes." }

Write-Host "Secret minio-creds créé/mis à jour sans enregistrer les valeurs dans Git." -ForegroundColor Green
