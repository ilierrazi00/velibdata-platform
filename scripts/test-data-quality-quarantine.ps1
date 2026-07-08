[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$RunId = Get-Date -Format "yyyyMMdd-HHmmss"
$EvidenceDir = Join-Path $Root "evidence"
$LogPath = Join-Path $EvidenceDir "data-quality-quarantine-$RunId.txt"
$JsonPath = Join-Path $EvidenceDir "data-quality-report-$RunId.json"
New-Item -ItemType Directory -Force -Path $EvidenceDir | Out-Null

function Write-Evidence {
    param([string]$Message)
    $Line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $Line
    Add-Content -Path $LogPath -Value $Line -Encoding UTF8
}

function Fail-Test {
    param([string]$Message)
    Write-Evidence "FAIL - $Message"
    Write-Evidence "RESULTAT GLOBAL : FAIL"
    exit 1
}

function Invoke-Docker {
    param([string[]]$Arguments)

    # Docker Compose ecrit certains messages normaux (Running, Created, etc.)
    # sur STDERR. Avec $ErrorActionPreference = "Stop", Windows PowerShell 5.1
    # peut les convertir en NativeCommandError alors que le code retour vaut 0.
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $Output = & docker @Arguments 2>&1
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }

    foreach ($Line in $Output) {
        Write-Host ([string]$Line)
        Add-Content -Path $LogPath -Value ([string]$Line) -Encoding UTF8
    }
    if ($ExitCode -ne 0) {
        Fail-Test "commande Docker en echec (code=$ExitCode)."
    }
    return $Output
}

Write-Evidence "TEST QUALITE / QUARANTAINE - debut"
Write-Evidence "Perimetre : detection, isolation et reporting automatiques de lignes invalides."

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail-Test "Docker est introuvable."
}

& docker compose -f docker-compose.yml -f docker-compose.quality.yml config --quiet
if ($LASTEXITCODE -ne 0) {
    Fail-Test "configuration Docker Compose invalide."
}
Write-Evidence "PASS - configuration Docker Compose valide."

Invoke-Docker -Arguments @(
    "compose", "-f", "docker-compose.yml", "-f", "docker-compose.quality.yml",
    "up", "-d", "minio"
) | Out-Null

$Healthy = $false
for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
    $Status = (& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" velib-minio 2>$null)
    if ($Status -eq "healthy") {
        $Healthy = $true
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $Healthy) {
    Fail-Test "MinIO n'est pas devenu healthy."
}
Write-Evidence "PASS - MinIO healthy."

Invoke-Docker -Arguments @(
    "compose", "-f", "docker-compose.yml", "-f", "docker-compose.quality.yml",
    "build", "great-expectations"
) | Out-Null
Write-Evidence "PASS - image Great Expectations construite."

$InputPath = "quality-input/$RunId"
$QuarantinePath = "quarantine/quality-test"
$AcceptedPath = "curated-quality/quality-test"
$ReportPath = "quality-reports/quality-test/$RunId/report.json"
$LocalReportPath = "/evidence/data-quality-report-$RunId.json"

$Output = Invoke-Docker -Arguments @(
    "compose", "-f", "docker-compose.yml", "-f", "docker-compose.quality.yml",
    "run", "--rm", "--no-deps",
    "-e", "QUALITY_DEMO=true",
    "-e", "QUALITY_RUN_ID=$RunId",
    "-e", "QUALITY_INPUT_PATH=$InputPath",
    "-e", "QUALITY_QUARANTINE_PATH=$QuarantinePath",
    "-e", "QUALITY_ACCEPTED_PATH=$AcceptedPath",
    "-e", "QUALITY_REPORT_PATH=$ReportPath",
    "-e", "QUALITY_LOCAL_REPORT_PATH=$LocalReportPath",
    "-e", "QUALITY_STRICT=false",
    "great-expectations"
)

$OutputText = ($Output | ForEach-Object { [string]$_ }) -join "`n"
foreach ($Expected in @(
    "INPUT_ROWS=8",
    "ACCEPTED_ROWS=5",
    "QUARANTINED_ROWS=3",
    "RESULTAT GLOBAL : PASS"
)) {
    if ($OutputText -notmatch [regex]::Escape($Expected)) {
        Fail-Test "sortie attendue absente : $Expected"
    }
}
Write-Evidence "PASS - 8 lignes analysees : 5 acceptees et 3 mises en quarantaine."

if (-not (Test-Path $JsonPath)) {
    Fail-Test "rapport JSON local absent : $JsonPath"
}

try {
    $Report = Get-Content $JsonPath -Raw | ConvertFrom-Json
} catch {
    Fail-Test "rapport JSON illisible : $($_.Exception.Message)"
}

if ([int]$Report.counts.input_rows -ne 8) {
    Fail-Test "nombre de lignes d'entree incorrect dans le rapport."
}
if ([int]$Report.counts.accepted_rows -ne 5) {
    Fail-Test "nombre de lignes acceptees incorrect dans le rapport."
}
if ([int]$Report.counts.quarantined_rows -ne 3) {
    Fail-Test "nombre de lignes en quarantaine incorrect dans le rapport."
}
if ($Report.status -ne "PASS_WITH_QUARANTINE") {
    Fail-Test "statut qualite inattendu : $($Report.status)"
}
if (-not $Report.outputs.quarantine_path) {
    Fail-Test "chemin de quarantaine absent du rapport."
}

Write-Evidence "PASS - rapport JSON verifie : status=PASS_WITH_QUARANTINE."
Write-Evidence "PASS - donnees valides routees vers $($Report.outputs.accepted_path)."
Write-Evidence "PASS - donnees invalides isolees vers $($Report.outputs.quarantine_path)."
Write-Evidence "PASS - rapport MinIO disponible sous $($Report.outputs.report_path)."
Write-Evidence "RESULTAT GLOBAL : PASS"
Write-Evidence "Rapport texte : $LogPath"
Write-Evidence "Rapport JSON : $JsonPath"
Write-Evidence "Etat final : les objets de demonstration restent dans MinIO pour inspection."
