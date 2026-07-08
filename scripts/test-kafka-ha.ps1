[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$RunId = Get-Date -Format "yyyyMMdd-HHmmss"
$Project = "velibdata-kafka-ha"
$ComposeFile = "docker-compose.kafka-ha.yml"
$Topic = "velib.ha.test"
$EvidenceDir = Join-Path $Root "evidence"
$LogPath = Join-Path $EvidenceDir "kafka-ha-$RunId.txt"
$WorkDir = Join-Path $env:TEMP "velibdata-kafka-ha\$RunId"
New-Item -ItemType Directory -Force -Path $EvidenceDir, $WorkDir | Out-Null

$ComposeArgs = @("compose", "--project-name", $Project, "--file", $ComposeFile)
$AllBootstrap = "kafka-1:9092,kafka-2:9092,kafka-3:9092"

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
    param(
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

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

    if (($ExitCode -ne 0) -and (-not $AllowFailure)) {
        Fail-Test "commande Docker en echec (code=$ExitCode) : docker $($Arguments -join ' ')"
    }

    return [pscustomobject]@{
        Output = @($Output | ForEach-Object { [string]$_ })
        ExitCode = $ExitCode
    }
}

function Invoke-Compose {
    param(
        [string[]]$Arguments,
        [switch]$AllowFailure
    )
    return Invoke-Docker -Arguments ($ComposeArgs + $Arguments) -AllowFailure:$AllowFailure
}

function Invoke-Kafka {
    param(
        [string]$Container,
        [string]$Command,
        [switch]$AllowFailure
    )
    return Invoke-Docker -Arguments @("exec", $Container, "bash", "-lc", $Command) -AllowFailure:$AllowFailure
}

function Wait-Healthy {
    param(
        [string]$Container,
        [int]$TimeoutSeconds = 240
    )

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $Status = (& docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $Container 2>$null)
        if ($Status -eq "healthy") {
            return $true
        }
        Start-Sleep -Seconds 5
    }
    return $false
}

function Get-TopicDescription {
    param(
        [string]$Container,
        [string]$Bootstrap
    )
    $Result = Invoke-Kafka -Container $Container -Command "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $Bootstrap --topic $Topic --describe" -AllowFailure
    if ($Result.ExitCode -ne 0) {
        return $null
    }
    return ($Result.Output -join "`n")
}

function Get-PartitionStates {
    param([string]$Description)
    $States = @()
    if (-not $Description) {
        return $States
    }

    foreach ($Line in ($Description -split "`r?`n")) {
        if ($Line -match "Partition:\s*(\d+).*Leader:\s*(-?\d+).*Replicas:\s*([0-9,]+).*Isr:\s*([0-9,]+)") {
            $Replicas = @($Matches[3].Split(',') | Where-Object { $_ -ne "" })
            $Isr = @($Matches[4].Split(',') | Where-Object { $_ -ne "" })
            $States += [pscustomobject]@{
                Partition = [int]$Matches[1]
                Leader = [int]$Matches[2]
                Replicas = $Replicas
                Isr = $Isr
            }
        }
    }
    return $States
}

function Wait-PartitionState {
    param(
        [string]$Container,
        [string]$Bootstrap,
        [int]$ExpectedIsrCount,
        [int]$TimeoutSeconds = 180
    )

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $Description = Get-TopicDescription -Container $Container -Bootstrap $Bootstrap
        $States = Get-PartitionStates -Description $Description
        if (($States.Count -eq 3) -and
            (($States | Where-Object { $_.Leader -lt 0 }).Count -eq 0) -and
            (($States | Where-Object { $_.Replicas.Count -ne 3 }).Count -eq 0) -and
            (($States | Where-Object { $_.Isr.Count -ne $ExpectedIsrCount }).Count -eq 0)) {
            return [pscustomobject]@{
                Description = $Description
                States = $States
            }
        }
        Start-Sleep -Seconds 5
    }
    return $null
}

function Get-TopicMessageCount {
    param(
        [string]$Container,
        [string]$Bootstrap
    )

    $Result = Invoke-Kafka -Container $Container -Command "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server $Bootstrap --topic $Topic --time -1" -AllowFailure
    if ($Result.ExitCode -ne 0) {
        return -1
    }

    $Total = 0L
    $Found = 0
    foreach ($Line in $Result.Output) {
        if ($Line -match ":(-?\d+)\s*$") {
            $Total += [int64]$Matches[1]
            $Found++
        }
    }
    if ($Found -ne 3) {
        return -1
    }
    return $Total
}

function Produce-File {
    param(
        [string]$Container,
        [string]$Bootstrap,
        [string]$LocalPath,
        [string]$RemotePath
    )

    Invoke-Docker -Arguments @("cp", $LocalPath, "${Container}:$RemotePath") | Out-Null
    $Command = "cat $RemotePath | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $Bootstrap --topic $Topic --producer-property acks=all --producer-property enable.idempotence=true --producer-property retries=20 --producer-property delivery.timeout.ms=120000"
    $Result = Invoke-Kafka -Container $Container -Command $Command -AllowFailure
    return ($Result.ExitCode -eq 0)
}

function Verify-ConsumedMessages {
    param(
        [string]$Container,
        [string]$Bootstrap,
        [string]$ExpectedPath,
        [int]$ExpectedCount
    )

    $RemotePath = "/tmp/velib-ha-consumed-$RunId.txt"
    $LocalPath = Join-Path $WorkDir "consumed.txt"
    $Command = "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $Bootstrap --topic $Topic --from-beginning --max-messages $ExpectedCount --timeout-ms 90000 > $RemotePath 2>/tmp/velib-ha-consumer-$RunId.err"
    $Result = Invoke-Kafka -Container $Container -Command $Command -AllowFailure
    if ($Result.ExitCode -ne 0) {
        return $null
    }

    Invoke-Docker -Arguments @("cp", "${Container}:$RemotePath", $LocalPath) | Out-Null
    $Expected = @(Get-Content -Path $ExpectedPath | Where-Object { $_ -ne "" })
    $Consumed = @(Get-Content -Path $LocalPath | Where-Object { $_ -ne "" })

    $ExpectedSorted = Join-Path $WorkDir "expected-sorted.txt"
    $ConsumedSorted = Join-Path $WorkDir "consumed-sorted.txt"
    $Expected | Sort-Object | Set-Content -Path $ExpectedSorted -Encoding ASCII
    $Consumed | Sort-Object | Set-Content -Path $ConsumedSorted -Encoding ASCII

    return [pscustomobject]@{
        ExpectedCount = $Expected.Count
        ConsumedCount = $Consumed.Count
        UniqueConsumedCount = @($Consumed | Sort-Object -Unique).Count
        ExpectedHash = (Get-FileHash -Algorithm SHA256 -Path $ExpectedSorted).Hash.ToLowerInvariant()
        ConsumedHash = (Get-FileHash -Algorithm SHA256 -Path $ConsumedSorted).Hash.ToLowerInvariant()
        LocalPath = $LocalPath
    }
}

Write-Evidence "TEST HA KAFKA 3 BROKERS - debut"
Write-Evidence "Perimetre : cluster KRaft local, replication 3, min.insync.replicas=2, perte volontaire d'un broker."

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Fail-Test "Docker est introuvable."
}
if (-not (Test-Path $ComposeFile)) {
    Fail-Test "fichier absent : $ComposeFile"
}

$Config = Invoke-Compose -Arguments @("config", "--quiet") -AllowFailure
if ($Config.ExitCode -ne 0) {
    Fail-Test "configuration Docker Compose invalide."
}
Write-Evidence "PASS - configuration Docker Compose valide."

Invoke-Compose -Arguments @("down", "--volumes", "--remove-orphans") -AllowFailure | Out-Null
Invoke-Compose -Arguments @("up", "-d") | Out-Null

foreach ($Container in @("velib-kafka-ha-1", "velib-kafka-ha-2", "velib-kafka-ha-3")) {
    if (-not (Wait-Healthy -Container $Container -TimeoutSeconds 300)) {
        Fail-Test "$Container n'est pas devenu healthy."
    }
}
Write-Evidence "PASS - 3 brokers Kafka KRaft healthy."

$CreateTopic = "/opt/kafka/bin/kafka-topics.sh --bootstrap-server $AllBootstrap --create --if-not-exists --topic $Topic --partitions 3 --replication-factor 3 --config min.insync.replicas=2"
$TopicResult = Invoke-Kafka -Container "velib-kafka-ha-1" -Command $CreateTopic -AllowFailure
if ($TopicResult.ExitCode -ne 0) {
    Fail-Test "creation du topic repliquee impossible."
}

$InitialState = Wait-PartitionState -Container "velib-kafka-ha-1" -Bootstrap $AllBootstrap -ExpectedIsrCount 3 -TimeoutSeconds 180
if (-not $InitialState) {
    Fail-Test "le topic n'a pas atteint 3 replicas synchronisees par partition."
}
Write-Evidence "PASS - topic $Topic : 3 partitions, replication factor 3, ISR=3."
Write-Evidence "Etat initial du topic :"
foreach ($Line in ($InitialState.Description -split "`r?`n")) {
    if ($Line.Trim()) { Write-Evidence $Line.Trim() }
}

$BaselinePath = Join-Path $WorkDir "baseline.txt"
$FailurePath = Join-Path $WorkDir "during-failure.txt"
$ExpectedPath = Join-Path $WorkDir "expected-all.txt"
$BaselineMessages = @(1..60 | ForEach-Object { "baseline-$RunId-{0:D3}" -f $_ })
$FailureMessages = @(1..60 | ForEach-Object { "during-failure-$RunId-{0:D3}" -f $_ })
$BaselineMessages | Set-Content -Path $BaselinePath -Encoding ASCII
$FailureMessages | Set-Content -Path $FailurePath -Encoding ASCII
@($BaselineMessages + $FailureMessages) | Set-Content -Path $ExpectedPath -Encoding ASCII

if (-not (Produce-File -Container "velib-kafka-ha-1" -Bootstrap $AllBootstrap -LocalPath $BaselinePath -RemotePath "/tmp/baseline.txt")) {
    Fail-Test "production initiale avec acks=all impossible."
}
$BaselineCount = Get-TopicMessageCount -Container "velib-kafka-ha-1" -Bootstrap $AllBootstrap
if ($BaselineCount -ne 60) {
    Fail-Test "compteur apres production initiale inattendu : $BaselineCount au lieu de 60."
}
Write-Evidence "PASS - 60 messages produits avec acks=all avant incident."

$LeaderId = [int]$InitialState.States[0].Leader
if ($LeaderId -notin @(1, 2, 3)) {
    Fail-Test "leader initial inattendu : $LeaderId"
}
$FailedService = "kafka-$LeaderId"
$FailedContainer = "velib-kafka-ha-$LeaderId"
$RemainingIds = @(@(1, 2, 3) | Where-Object { $_ -ne $LeaderId })
$HelperId = $RemainingIds[0]
$HelperContainer = "velib-kafka-ha-$HelperId"
$RemainingBootstrap = (($RemainingIds | ForEach-Object { "kafka-$($_):9092" }) -join ",")

$IncidentStart = Get-Date
Write-Evidence "Incident controle : arret du broker $LeaderId, leader initial de la partition 0."
Invoke-Compose -Arguments @("stop", $FailedService) | Out-Null

$DegradedState = Wait-PartitionState -Container $HelperContainer -Bootstrap $RemainingBootstrap -ExpectedIsrCount 2 -TimeoutSeconds 180
if (-not $DegradedState) {
    Fail-Test "le cluster n'est pas reste disponible avec ISR=2 apres la perte du broker $LeaderId."
}
$FailoverSeconds = [math]::Round(((Get-Date) - $IncidentStart).TotalSeconds, 2)
Write-Evidence "PASS - reelection des leaders et service disponible avec 2 brokers ; failover observe=$FailoverSeconds s."
Write-Evidence "Etat degrade du topic :"
foreach ($Line in ($DegradedState.Description -split "`r?`n")) {
    if ($Line.Trim()) { Write-Evidence $Line.Trim() }
}

if (-not (Produce-File -Container $HelperContainer -Bootstrap $RemainingBootstrap -LocalPath $FailurePath -RemotePath "/tmp/during-failure.txt")) {
    Fail-Test "production avec acks=all impossible pendant la panne d'un broker."
}
$DegradedCount = Get-TopicMessageCount -Container $HelperContainer -Bootstrap $RemainingBootstrap
if ($DegradedCount -ne 120) {
    Fail-Test "compteur pendant la panne inattendu : $DegradedCount au lieu de 120."
}
Write-Evidence "PASS - 60 messages supplementaires produits pendant la panne ; total=120."

$Integrity = Verify-ConsumedMessages -Container $HelperContainer -Bootstrap $RemainingBootstrap -ExpectedPath $ExpectedPath -ExpectedCount 120
if (-not $Integrity) {
    Fail-Test "lecture complete impossible pendant la panne."
}
if (($Integrity.ConsumedCount -ne 120) -or ($Integrity.UniqueConsumedCount -ne 120)) {
    Fail-Test "lecture pendant la panne incomplete ou dupliquee : total=$($Integrity.ConsumedCount), uniques=$($Integrity.UniqueConsumedCount)."
}
if ($Integrity.ExpectedHash -ne $Integrity.ConsumedHash) {
    Fail-Test "integrite SHA-256 invalide apres lecture pendant la panne."
}
Write-Evidence "PASS - lecture des 120 messages pendant la panne, sans perte ni doublon."
Write-Evidence "SHA-256 attendu/lu : $($Integrity.ExpectedHash)"

$RecoveryStart = Get-Date
Write-Evidence "Redemarrage du broker $LeaderId."
Invoke-Compose -Arguments @("start", $FailedService) | Out-Null
if (-not (Wait-Healthy -Container $FailedContainer -TimeoutSeconds 300)) {
    Fail-Test "le broker $LeaderId n'est pas redevenu healthy."
}

$RecoveredState = Wait-PartitionState -Container $HelperContainer -Bootstrap $AllBootstrap -ExpectedIsrCount 3 -TimeoutSeconds 240
if (-not $RecoveredState) {
    Fail-Test "les 3 replicas ne se sont pas resynchronisees apres le redemarrage."
}
$RecoverySeconds = [math]::Round(((Get-Date) - $RecoveryStart).TotalSeconds, 2)
$FinalCount = Get-TopicMessageCount -Container $HelperContainer -Bootstrap $AllBootstrap
if ($FinalCount -ne 120) {
    Fail-Test "compteur final inattendu : $FinalCount au lieu de 120."
}
Write-Evidence "PASS - broker $LeaderId retabli et ISR revenu a 3 sur les 3 partitions."
Write-Evidence "RTO technique observe pour la resynchronisation complete : $RecoverySeconds s."
Write-Evidence "PASS - compteur final=120 messages."
Write-Evidence "RESULTAT GLOBAL : PASS"
Write-Evidence "Rapport : $LogPath"
Write-Evidence "Etat final : le cluster Kafka HA reste actif pour inspection."
Write-Evidence "Arret ulterieur : docker compose --project-name $Project --file $ComposeFile down --volumes --remove-orphans"
