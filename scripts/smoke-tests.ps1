param(
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"
$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir

$ProjectName = if ($env:SMOKE_PROJECT_NAME) { $env:SMOKE_PROJECT_NAME } else { "velibdata-smoke" }
$ComposeFile = if ($env:SMOKE_COMPOSE_FILE) { $env:SMOKE_COMPOSE_FILE } else { "docker-compose.smoke.yml" }
$MinioPort = if ($env:SMOKE_MINIO_PORT) { $env:SMOKE_MINIO_PORT } else { "19000" }
$PrometheusPort = if ($env:SMOKE_PROMETHEUS_PORT) { $env:SMOKE_PROMETHEUS_PORT } else { "19090" }
$GrafanaPort = if ($env:SMOKE_GRAFANA_PORT) { $env:SMOKE_GRAFANA_PORT } else { "13000" }
$KafkaExporterPort = if ($env:SMOKE_KAFKA_EXPORTER_PORT) { $env:SMOKE_KAFKA_EXPORTER_PORT } else { "19308" }
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$EvidenceDir = Join-Path $RootDir "evidence"
New-Item -ItemType Directory -Path $EvidenceDir -Force | Out-Null
$Report = Join-Path $EvidenceDir "ci-smoke-$Timestamp.txt"
$DiagnosticLog = Join-Path $EvidenceDir "ci-smoke-containers-$Timestamp.log"
$ComposeBaseArgs = @("compose", "--project-name", $ProjectName, "--file", $ComposeFile)

function Add-Report {
    param([string]$Message)
    $Line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $Line
    Add-Content -Path $Report -Value $Line -Encoding UTF8
}

function Invoke-External {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$Context = "Commande externe",
        [switch]$Capture
    )

    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = & $FilePath @Arguments 2>&1
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }

    foreach ($Line in $Output) {
        $Text = [string]$Line
        Write-Host $Text
        Add-Content -Path $Report -Value $Text -Encoding UTF8
    }

    if ($ExitCode -ne 0) {
        throw "$Context a echoue (code $ExitCode)."
    }

    if ($Capture) {
        return ($Output | ForEach-Object { [string]$_ }) -join "`n"
    }
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$Context = "Docker Compose",
        [switch]$Capture
    )
    $AllArgs = @($ComposeBaseArgs + $Arguments)
    return Invoke-External -FilePath "docker" -Arguments $AllArgs -Context $Context -Capture:$Capture
}

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$Label,
        [int]$Attempts = 60
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        $PreviousPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "Continue"
            $ContainerId = (& docker @ComposeBaseArgs ps -q $Service 2>$null | Out-String).Trim()
            if ($ContainerId) {
                $Status = (& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $ContainerId 2>$null | Out-String).Trim()
                if ($Status -in @("healthy", "running")) {
                    Add-Report "PASS - $Label est $Status."
                    return
                }
            }
        }
        finally {
            $ErrorActionPreference = $PreviousPreference
        }
        Start-Sleep -Seconds 3
    }
    throw "$Label n'est pas devenu sain dans le delai imparti."
}

function Wait-Http {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][string]$Label,
        [int]$Attempts = 60
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            $Response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($Response.StatusCode -ge 200 -and $Response.StatusCode -lt 400) {
                Add-Report "PASS - $Label repond sur $Url"
                return
            }
        }
        catch {
            Start-Sleep -Seconds 3
        }
    }
    throw "$Label ne repond pas sur $Url"
}

$Succeeded = $false
try {
    Add-Report "SMOKE TEST VELIBDATA - debut"
    Add-Report "Compose : $ComposeFile ; projet isole : $ProjectName"

    if (-not (Test-Path ".env")) {
        throw "Le fichier .env est absent. Creez-le a partir de .env.example."
    }

    Invoke-Compose -Arguments @("config", "--quiet") -Context "Validation Compose"
    Add-Report "PASS - configuration Docker Compose valide."

    Invoke-Compose -Arguments @("up", "-d", "kafka", "minio", "kafka-exporter", "prometheus", "grafana") -Context "Demarrage de la stack smoke"
    Wait-ContainerHealthy -Service "kafka" -Label "Kafka"
    Wait-ContainerHealthy -Service "minio" -Label "MinIO"

    Invoke-Compose -Arguments @("run", "--rm", "minio-setup") -Context "Initialisation MinIO"
    Add-Report "PASS - initialisation des buckets terminee."

    Wait-Http -Url "http://127.0.0.1:$MinioPort/minio/health/ready" -Label "MinIO"
    Wait-Http -Url "http://127.0.0.1:$PrometheusPort/-/ready" -Label "Prometheus"
    Wait-Http -Url "http://127.0.0.1:$GrafanaPort/api/health" -Label "Grafana"
    Wait-Http -Url "http://127.0.0.1:$KafkaExporterPort/metrics" -Label "Kafka Exporter"

    Add-Report "Verification lecture/ecriture des buckets MinIO."
    # Le jeton est genere par PowerShell. La comparaison est effectuee directement
    # dans le shell du conteneur, sans analyser la sortie de Docker Compose. Cela
    # evite les messages "Container ... Creating/Created" et les problemes de
    # transmission du caractere \n sous Windows.
    $Probe = "velibdata-smoke-$Timestamp"
    $MinioCommand = @'
set -eu
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
for bucket in raw clean curated; do
  mc ls "local/$bucket" >/dev/null
done
echo "__PROBE__" >/tmp/smoke-probe.txt
mc cp /tmp/smoke-probe.txt local/raw/ci/smoke-probe.txt >/dev/null
mc stat local/raw/ci/smoke-probe.txt >/dev/null
mc cat local/raw/ci/smoke-probe.txt >/tmp/smoke-readback.txt
IFS= read -r actual </tmp/smoke-readback.txt
[ "$actual" = "__PROBE__" ]
echo "MINIO_CONTENT_OK"
'@
    $MinioCommand = $MinioCommand.Replace("__PROBE__", $Probe)
    Invoke-Compose -Arguments @("run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "minio-setup", "-c", $MinioCommand) -Context "Test MinIO"
    Add-Report "PASS - buckets RAW/CLEAN/CURATED accessibles, objet MinIO ecrit puis relu avec contenu identique : $Probe"

    $Topic = "velib.ci.smoke.$Timestamp"
    $Message = "velibdata-smoke-$Timestamp"
    Add-Report "Verification Kafka sur le topic $Topic."
    Invoke-Compose -Arguments @("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--create", "--if-not-exists", "--topic", $Topic, "--partitions", "1", "--replication-factor", "1") -Context "Creation topic Kafka"

    $ProducerCommand = "echo '$Message' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic '$Topic'"
    Invoke-Compose -Arguments @("exec", "-T", "kafka", "/bin/bash", "-lc", $ProducerCommand) -Context "Production Kafka"

    $Consumed = Invoke-Compose -Arguments @("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "kafka:9092", "--topic", $Topic, "--from-beginning", "--max-messages", "1", "--timeout-ms", "15000") -Context "Consommation Kafka" -Capture
    $ConsumedLines = $Consumed -split "`r?`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }
    if ($ConsumedLines -notcontains $Message) {
        $Observed = ($ConsumedLines -join " | ")
        throw "Message Kafka attendu '$Message', sortie obtenue '$Observed'."
    }
    Add-Report "PASS - message Kafka produit puis consomme : $Message"

    $ExporterMetricFound = $false
    for ($i = 1; $i -le 30; $i++) {
        try {
            $Exporter = Invoke-WebRequest -Uri "http://127.0.0.1:$KafkaExporterPort/metrics" -UseBasicParsing -TimeoutSec 10
            if ($Exporter.Content -match "(?m)^kafka_brokers ") {
                $ExporterMetricFound = $true
                break
            }
        }
        catch {
            # Le service peut etre encore en cours d'initialisation.
        }
        Start-Sleep -Seconds 2
    }
    if (-not $ExporterMetricFound) {
        throw "La metrique kafka_brokers est absente."
    }
    Add-Report "PASS - kafka-exporter expose la metrique kafka_brokers."

    $Prometheus = Invoke-RestMethod -Uri "http://127.0.0.1:$PrometheusPort/api/v1/status/config" -TimeoutSec 10
    if ($Prometheus.status -ne "success") {
        throw "Prometheus ne valide pas sa configuration."
    }
    Add-Report "PASS - configuration Prometheus chargee."

    $Grafana = Invoke-RestMethod -Uri "http://127.0.0.1:$GrafanaPort/api/health" -TimeoutSec 10
    if (-not $Grafana.database) {
        throw "Reponse de sante Grafana inattendue."
    }
    Add-Report "PASS - Grafana et sa base interne sont operationnels."

    Add-Report "Etat final des services :"
    Invoke-Compose -Arguments @("ps") -Context "Etat final Docker Compose"

    $Succeeded = $true
    Add-Report "RESULTAT GLOBAL : PASS"
}
catch {
    Add-Report "RESULTAT GLOBAL : FAIL - $($_.Exception.Message)"
    try {
        $Diagnostics = & docker @ComposeBaseArgs logs --no-color 2>&1
        $Diagnostics | Set-Content -Path $DiagnosticLog -Encoding UTF8
        Add-Report "Diagnostics : $DiagnosticLog"
    }
    catch {
        Add-Report "Impossible de collecter tous les diagnostics."
    }
    throw
}
finally {
    if (-not $KeepRunning) {
        Add-Report "Nettoyage de la stack de smoke test."
        $PreviousPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "Continue"
            $CleanupOutput = & docker @ComposeBaseArgs down --volumes --remove-orphans 2>&1
            $CleanupExitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $PreviousPreference
        }
        if ($CleanupExitCode -ne 0) {
            Add-Report "Avertissement : nettoyage incomplet (code $CleanupExitCode)."
            foreach ($Line in $CleanupOutput) {
                Add-Content -Path $Report -Value ([string]$Line) -Encoding UTF8
            }
        }
        else {
            Add-Report "PASS - stack temporaire et volumes supprimes."
        }
    }
    else {
        Add-Report "Stack conservee avec -KeepRunning."
    }
    Add-Report "Rapport : $Report"
}
