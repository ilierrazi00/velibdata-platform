param(
    [int]$MessageCount = 300,
    [int]$MinimumScaleOut = 3,
    [int]$ScaleOutTimeoutSeconds = 240,
    [int]$DrainTimeoutSeconds = 420,
    [int]$ScaleDownTimeoutSeconds = 240
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ComposeFile = Join-Path $ProjectRoot "docker-compose.keda.yml"
$ManifestFile = Join-Path $ProjectRoot "k8s\02-autoscaling-demo.yaml"
$EvidenceDir = Join-Path $ProjectRoot "evidence"
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$Report = Join-Path $EvidenceDir "keda-kafka-autoscaling-$Timestamp.txt"
$ComposeArgs = @("compose", "--project-name", "velibdata-keda", "--file", $ComposeFile)
$Namespace = "velibdata"
$Deployment = "velib-worker"
$ScaledObject = "velib-worker-scaler"
$Topic = "velib.keda.demo"
$ConsumerGroup = "velib-keda-workers"

New-Item -ItemType Directory -Force -Path $EvidenceDir | Out-Null

function Add-Report {
    param([string]$Message)
    $Line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $Line
    Add-Content -Path $Report -Value $Line -Encoding UTF8
}

function Invoke-Checked {
    param(
        [string]$Command,
        [string[]]$Arguments,
        [string]$Context
    )
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Context a echoue (code $LASTEXITCODE)."
    }
}

function Invoke-Captured {
    param(
        [string]$Command,
        [string[]]$Arguments,
        [string]$Context
    )
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = (& $Command @Arguments 2>&1 | Out-String).Trim()
        $Code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($Code -ne 0) {
        throw "$Context a echoue (code $Code). Sortie : $Output"
    }
    return $Output
}

function Invoke-Compose {
    param([string[]]$Arguments, [string]$Context)
    Invoke-Checked -Command "docker" -Arguments ($ComposeArgs + $Arguments) -Context $Context
}

function Invoke-ComposeCaptured {
    param([string[]]$Arguments, [string]$Context)
    return Invoke-Captured -Command "docker" -Arguments ($ComposeArgs + $Arguments) -Context $Context
}

function Get-ReplicaCount {
    $Value = Invoke-Captured -Command "kubectl" -Arguments @(
        "get", "deployment", $Deployment, "-n", $Namespace,
        "-o", "jsonpath={.spec.replicas}"
    ) -Context "Lecture du nombre de replicas"
    if ([string]::IsNullOrWhiteSpace($Value)) { return 0 }
    return [int]$Value.Trim()
}

function Get-ReadyReplicaCount {
    $Value = Invoke-Captured -Command "kubectl" -Arguments @(
        "get", "deployment", $Deployment, "-n", $Namespace,
        "-o", "jsonpath={.status.readyReplicas}"
    ) -Context "Lecture du nombre de replicas prets"
    if ([string]::IsNullOrWhiteSpace($Value)) { return 0 }
    return [int]$Value.Trim()
}

function Get-KafkaLag {
    $Output = Invoke-ComposeCaptured -Arguments @(
        "exec", "-T", "kafka-keda",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka-keda:9092",
        "--describe", "--group", $ConsumerGroup
    ) -Context "Lecture du consumer lag Kafka"

    $Total = 0
    $Found = $false
    foreach ($Line in ($Output -split "`r?`n")) {
        $Clean = $Line.Trim()
        if ($Clean -match ("^" + [regex]::Escape($ConsumerGroup) + "\s+" + [regex]::Escape($Topic) + "\s+\d+\s+\S+\s+\S+\s+(\d+|-)") ) {
            $Found = $true
            if ($Matches[1] -ne "-") {
                $Total += [int]$Matches[1]
            }
        }
    }
    if (-not $Found) {
        return $null
    }
    return $Total
}

function Wait-Until {
    param(
        [scriptblock]$Condition,
        [int]$TimeoutSeconds,
        [int]$IntervalSeconds,
        [string]$Description
    )
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        if (& $Condition) { return $true }
        Start-Sleep -Seconds $IntervalSeconds
    }
    throw "Timeout : $Description apres $TimeoutSeconds secondes."
}

try {
    Add-Report "TEST KEDA KAFKA LAG - debut"
    Add-Report "Perimetre : autoscaling reel du Deployment velib-worker selon le consumer lag Kafka."

    Invoke-Checked -Command "docker" -Arguments @("version", "--format", "{{.Server.Version}}") -Context "Docker indisponible"
    Invoke-Checked -Command "kubectl" -Arguments @("get", "crd", "scaledobjects.keda.sh") -Context "KEDA non installe"
    Invoke-Checked -Command "docker" -Arguments ($ComposeArgs + @("config", "--quiet")) -Context "Configuration docker-compose.keda.yml invalide"
    Add-Report "PASS - prerequis Docker, Kubernetes et KEDA valides."

    Invoke-Compose -Arguments @("up", "-d", "--wait", "kafka-keda") -Context "Demarrage du broker Kafka KEDA"
    Add-Report "PASS - broker Kafka de demonstration healthy sur host.docker.internal:29092."

    Invoke-Compose -Arguments @(
        "exec", "-T", "kafka-keda",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "kafka-keda:9092",
        "--create", "--if-not-exists",
        "--topic", $Topic,
        "--partitions", "5",
        "--replication-factor", "1"
    ) -Context "Creation du topic Kafka"
    Add-Report "PASS - topic $Topic cree avec 5 partitions."

    $SeedCommand = 'echo seed-message | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-keda:9092 --topic velib.keda.demo'
    Invoke-Compose -Arguments @("exec", "-T", "kafka-keda", "/bin/bash", "-lc", $SeedCommand) -Context "Production du message initial"

    Invoke-Checked -Command "kubectl" -Arguments @("apply", "-f", $ManifestFile) -Context "Application du manifeste KEDA Kafka"
    Invoke-Checked -Command "kubectl" -Arguments @("rollout", "status", "deployment/$Deployment", "-n", $Namespace, "--timeout=240s") -Context "Demarrage du worker Kafka"

    Wait-Until -TimeoutSeconds 180 -IntervalSeconds 5 -Description "creation du consumer group" -Condition {
        try {
            $Lag = Get-KafkaLag
            return $null -ne $Lag
        }
        catch { return $false }
    } | Out-Null

    Wait-Until -TimeoutSeconds 120 -IntervalSeconds 5 -Description "consommation du message initial" -Condition {
        try {
            $Lag = Get-KafkaLag
            return ($null -ne $Lag -and $Lag -eq 0)
        }
        catch { return $false }
    } | Out-Null

    $ReadyCondition = Invoke-Captured -Command "kubectl" -Arguments @(
        "get", "scaledobject", $ScaledObject, "-n", $Namespace,
        "-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}"
    ) -Context "Lecture de l'etat KEDA"
    if ($ReadyCondition.Trim() -ne "True") {
        throw "Le ScaledObject KEDA n'est pas Ready."
    }
    Add-Report "PASS - worker Kafka reel et ScaledObject KEDA prets ; consumer group initialise."

    $ProduceCommand = 'for i in $(seq 1 {0}); do echo "velib-keda-message-$i"; done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-keda:9092 --topic velib.keda.demo' -f $MessageCount
    Invoke-Compose -Arguments @("exec", "-T", "kafka-keda", "/bin/bash", "-lc", $ProduceCommand) -Context "Injection du backlog Kafka"
    Add-Report "Backlog injecte : $MessageCount messages sur $Topic."

    $InitialLag = $null
    Wait-Until -TimeoutSeconds 60 -IntervalSeconds 3 -Description "apparition du consumer lag" -Condition {
        try {
            $script:InitialLag = Get-KafkaLag
            return ($null -ne $script:InitialLag -and $script:InitialLag -gt 0)
        }
        catch { return $false }
    } | Out-Null
    Add-Report "PASS - consumer lag reel detecte : $InitialLag messages."

    $ScaleOutStart = Get-Date
    Wait-Until -TimeoutSeconds $ScaleOutTimeoutSeconds -IntervalSeconds 5 -Description "scale-out vers au moins $MinimumScaleOut replicas" -Condition {
        $Replicas = Get-ReplicaCount
        $Ready = Get-ReadyReplicaCount
        $Lag = Get-KafkaLag
        Add-Report "Observation scale-out : replicas=$Replicas, ready=$Ready, lag=$Lag"
        return ($Replicas -ge $MinimumScaleOut -and $Ready -ge $MinimumScaleOut)
    } | Out-Null
    $ScaleOutSeconds = [math]::Round(((Get-Date) - $ScaleOutStart).TotalSeconds, 1)
    $PeakReplicas = Get-ReplicaCount
    Add-Report "SCALE-OUT PASS - $PeakReplicas replicas obtenus en $ScaleOutSeconds secondes grace au lag Kafka."

    $HpaState = Invoke-Captured -Command "kubectl" -Arguments @("get", "hpa", "-n", $Namespace) -Context "Lecture du HPA KEDA"
    Add-Report "Etat HPA apres scale-out :`n$HpaState"

    Wait-Until -TimeoutSeconds $DrainTimeoutSeconds -IntervalSeconds 10 -Description "drainage du consumer lag" -Condition {
        $Lag = Get-KafkaLag
        $Replicas = Get-ReplicaCount
        Add-Report "Observation drainage : replicas=$Replicas, lag=$Lag"
        return ($null -ne $Lag -and $Lag -eq 0)
    } | Out-Null
    Add-Report "PASS - backlog Kafka entierement traite ; lag=0."

    $ScaleDownStart = Get-Date
    Wait-Until -TimeoutSeconds $ScaleDownTimeoutSeconds -IntervalSeconds 5 -Description "retour a 1 replica" -Condition {
        $Replicas = Get-ReplicaCount
        $Ready = Get-ReadyReplicaCount
        Add-Report "Observation scale-down : replicas=$Replicas, ready=$Ready"
        return ($Replicas -eq 1 -and $Ready -eq 1)
    } | Out-Null
    $ScaleDownSeconds = [math]::Round(((Get-Date) - $ScaleDownStart).TotalSeconds, 1)
    Add-Report "SCALE-DOWN PASS - retour automatique a 1 replica en $ScaleDownSeconds secondes."

    $Pods = Invoke-Captured -Command "kubectl" -Arguments @("get", "pods", "-n", $Namespace, "-l", "app=velib-worker", "-o", "wide") -Context "Lecture des pods finaux"
    Add-Report "Etat final des workers :`n$Pods"
    Add-Report "RESULTAT GLOBAL : PASS"
    Add-Report "Rapport : $Report"
}
catch {
    Add-Report "RESULTAT GLOBAL : FAIL - $($_.Exception.Message)"
    try {
        $Diagnostics = Invoke-Captured -Command "kubectl" -Arguments @("describe", "scaledobject", $ScaledObject, "-n", $Namespace) -Context "Diagnostics KEDA"
        Add-Content -Path $Report -Value "`n--- DIAGNOSTICS KEDA ---`n$Diagnostics" -Encoding UTF8
    }
    catch {}
    try {
        $Logs = Invoke-Captured -Command "kubectl" -Arguments @("logs", "-n", $Namespace, "deployment/$Deployment", "--tail=100") -Context "Logs worker"
        Add-Content -Path $Report -Value "`n--- LOGS WORKER ---`n$Logs" -Encoding UTF8
    }
    catch {}
    throw
}
finally {
    Add-Report "Etat de securite : le broker de demonstration et le ScaledObject restent actifs pour inspection."
}
