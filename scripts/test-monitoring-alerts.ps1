[CmdletBinding()]
param(
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$EvidenceDir = Join-Path $Root "evidence"
New-Item -ItemType Directory -Path $EvidenceDir -Force | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Report = Join-Path $EvidenceDir "monitoring-$Timestamp.txt"
$WebhookLog = Join-Path $EvidenceDir "monitoring-alerts.jsonl"

function Add-Report([string]$Message) {
    $Line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $Line
    Add-Content -Path $Report -Value $Line -Encoding utf8
}

function Wait-Http([string]$Url, [string]$Name, [int]$Timeout = 120) {
    $deadline = (Get-Date).AddSeconds($Timeout)
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
                Add-Report "PASS - $Name repond sur $Url"
                return
            }
        } catch {
            Start-Sleep -Seconds 3
        }
    } while ((Get-Date) -lt $deadline)
    throw "$Name ne repond pas sur $Url apres $Timeout secondes."
}

function Get-PrometheusValue([string]$Query) {
    $encoded = [uri]::EscapeDataString($Query)
    $result = Invoke-RestMethod -Uri "http://127.0.0.1:9090/api/v1/query?query=$encoded" -TimeoutSec 10
    if ($result.status -ne "success" -or $result.data.result.Count -eq 0) {
        return $null
    }
    return [double]$result.data.result[0].value[1]
}

function Wait-PrometheusValue([string]$Query, [scriptblock]$Predicate, [string]$Description, [int]$Timeout = 120) {
    $deadline = (Get-Date).AddSeconds($Timeout)
    do {
        try {
            $value = Get-PrometheusValue $Query
            if ($null -ne $value -and (& $Predicate $value)) {
                Add-Report "PASS - $Description (valeur=$value)"
                return $value
            }
        } catch {
            # Prometheus peut ne pas encore avoir scrape la cible.
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "Condition Prometheus non satisfaite : $Description ; requete=$Query"
}

function Wait-WebhookPattern([string]$Pattern, [string]$Description, [int]$Timeout = 180) {
    $deadline = (Get-Date).AddSeconds($Timeout)
    do {
        if (Test-Path $WebhookLog) {
            $content = Get-Content $WebhookLog -Raw -ErrorAction SilentlyContinue
            if ($content -match $Pattern) {
                Add-Report "PASS - $Description"
                return
            }
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "Notification webhook non recue : $Description"
}

try {
    Add-Report "TEST MONITORING COMPLET - debut"

    docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Configuration Docker Compose invalide." }
    Add-Report "PASS - configuration Docker Compose valide."

    if (Test-Path $WebhookLog) { Remove-Item $WebhookLog -Force }

    docker compose up -d minio kafka kafka-exporter cadvisor alert-webhook alertmanager prometheus grafana
    if ($LASTEXITCODE -ne 0) { throw "Echec du demarrage de la stack monitoring." }

    Wait-Http "http://127.0.0.1:9000/minio/health/ready" "MinIO"
    Wait-Http "http://127.0.0.1:8081/metrics" "cAdvisor"
    Wait-Http "http://127.0.0.1:5001/health" "Webhook d'alertes"
    Wait-Http "http://127.0.0.1:9093/-/ready" "Alertmanager"
    Wait-Http "http://127.0.0.1:9090/-/ready" "Prometheus"
    Wait-Http "http://127.0.0.1:3000/api/health" "Grafana"

    Wait-PrometheusValue 'up{job="minio-cluster"}' { param($v) $v -eq 1 } "cible MinIO collectee"
    Wait-PrometheusValue 'up{job="cadvisor"}' { param($v) $v -eq 1 } "cible cAdvisor collectee"
    Wait-PrometheusValue 'up{job="alertmanager"}' { param($v) $v -eq 1 } "cible Alertmanager collectee"

    Wait-PrometheusValue 'count({job="minio-cluster",__name__=~"minio_.+"})' { param($v) $v -gt 0 } "metriques MinIO disponibles"
    Wait-PrometheusValue 'count(container_cpu_usage_seconds_total{job="cadvisor"})' { param($v) $v -gt 0 } "metriques CPU des conteneurs disponibles"
    Wait-PrometheusValue 'count(container_memory_working_set_bytes{job="cadvisor"})' { param($v) $v -gt 0 } "metriques memoire des conteneurs disponibles"

    Add-Report "Declenchement controle : arret temporaire de cAdvisor."
    docker compose stop cadvisor
    if ($LASTEXITCODE -ne 0) { throw "Impossible d'arreter cAdvisor pour le test." }

    Wait-WebhookPattern '"status":"firing".*"alertname":"VelibCadvisorDown"|"alertname":"VelibCadvisorDown".*"status":"firing"' "notification FIRING VelibCadvisorDown recue" $TimeoutSeconds

    Add-Report "Redemarrage de cAdvisor."
    docker compose start cadvisor
    if ($LASTEXITCODE -ne 0) { throw "Impossible de redemarrer cAdvisor." }

    Wait-Http "http://127.0.0.1:8081/metrics" "cAdvisor apres recuperation"
    Wait-PrometheusValue 'up{job="cadvisor"}' { param($v) $v -eq 1 } "cible cAdvisor retablie"
    Wait-WebhookPattern '"status":"resolved".*"alertname":"VelibCadvisorDown"|"alertname":"VelibCadvisorDown".*"status":"resolved"' "notification RESOLVED VelibCadvisorDown recue" $TimeoutSeconds

    Add-Report "RESULTAT GLOBAL : PASS"
    Add-Report "Preuve webhook : $WebhookLog"
    Add-Report "Dashboard Grafana : http://localhost:3000/d/velib-main"
}
catch {
    Add-Report "RESULTAT GLOBAL : FAIL - $($_.Exception.Message)"
    throw
}
finally {
    docker compose start cadvisor 2>$null | Out-Null
    Add-Report "Etat de securite : consigne de redemarrage cAdvisor envoyee."
    Add-Report "Rapport : $Report"
}
