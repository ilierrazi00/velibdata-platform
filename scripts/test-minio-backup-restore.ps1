[CmdletBinding()]
param(
    [string]$Namespace = "velibdata",
    [string]$SecretName = "minio-creds",
    [int]$Port = 19000,
    [string]$McImage = "minio/mc:latest"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$EvidenceDir = Join-Path $ProjectRoot "evidence"
$RunId = Get-Date -Format "yyyyMMdd-HHmmss"
$Report = Join-Path $EvidenceDir "minio-backup-restore-$RunId.txt"
$TempBase = Join-Path $env:TEMP "velibdata-minio-dr"
$RunRoot = Join-Path $TempBase $RunId
$SourceRoot = Join-Path $RunRoot "source"
$BackupRoot = Join-Path $RunRoot "backup"
$RestoreStagingRoot = Join-Path $RunRoot "restore-staging"
$RestoredRoot = Join-Path $RunRoot "restored"
$Archive = Join-Path $RunRoot "velibdata-minio-backup-$RunId.zip"
$Manifest = Join-Path $RunRoot "manifest.sha256"
$Endpoint = "http://host.docker.internal:$Port"
$HealthEndpoint = "http://127.0.0.1:$Port/minio/health/ready"
$RemotePrefix = "dr-test/$RunId"

New-Item -ItemType Directory -Force -Path $EvidenceDir, $SourceRoot, $BackupRoot, $RestoreStagingRoot, $RestoredRoot | Out-Null

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

function Get-SecretValue {
    param([string]$Key)
    $Encoded = (& kubectl get secret $SecretName -n $Namespace -o "jsonpath={.data.$Key}" 2>$null | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($Encoded)) {
        throw "Impossible de lire la cle $Key dans le Secret $SecretName/$Namespace."
    }
    return [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($Encoded))
}

function Invoke-McResult {
    param([string]$Command)

    $Mount = "${RunRoot}:/dr"
    $ShellCommand = 'mc alias set k8s "$MINIO_ENDPOINT" "$MINIO_USER" "$MINIO_PASSWORD" >/dev/null && ' + $Command
    $DockerArgs = @(
        "run", "--rm",
        "-e", "MINIO_ENDPOINT=$Endpoint",
        "-e", "MINIO_USER=$script:MinioUser",
        "-e", "MINIO_PASSWORD=$script:MinioPassword",
        "-v", $Mount,
        "--entrypoint", "/bin/sh",
        $McImage,
        "-c", $ShellCommand
    )

    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = (& docker @DockerArgs 2>&1 | Out-String).Trim()
        $Code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }

    return [pscustomobject]@{
        ExitCode = $Code
        Output = $Output
    }
}

function Invoke-Mc {
    param(
        [string]$Command,
        [string]$Context
    )
    $Result = Invoke-McResult -Command $Command
    if ($Result.ExitCode -ne 0) {
        throw "$Context a echoue (code $($Result.ExitCode)). Sortie : $($Result.Output)"
    }
    return $Result.Output
}

function New-TestData {
    $Data = @(
        [pscustomobject]@{
            Bucket = "raw"
            File = "raw-event.json"
            Content = '{"station_id":16107,"num_bikes_available":12,"source":"velib","zone":"RAW"}'
        },
        [pscustomobject]@{
            Bucket = "clean"
            File = "clean-event.json"
            Content = '{"station_id":16107,"num_bikes_available":12,"quality_valid":true,"zone":"CLEAN"}'
        },
        [pscustomobject]@{
            Bucket = "curated"
            File = "curated-event.csv"
            Content = "station_id,num_bikes_available,temperature_c,zone`r`n16107,12,21.4,CURATED"
        }
    )

    foreach ($Item in $Data) {
        $Directory = Join-Path $SourceRoot $Item.Bucket
        New-Item -ItemType Directory -Force -Path $Directory | Out-Null
        Set-Content -Path (Join-Path $Directory $Item.File) -Value $Item.Content -Encoding UTF8
    }
    return $Data
}

try {
    Add-Report "TEST SAUVEGARDE / RESTAURATION MINIO - debut"
    Add-Report "Perimetre : objets dedies sous raw, clean et curated, prefixe $RemotePrefix."

    Invoke-Checked -Command "docker" -Arguments @("version", "--format", "{{.Server.Version}}") -Context "Docker indisponible"
    Invoke-Checked -Command "kubectl" -Arguments @("get", "secret", $SecretName, "-n", $Namespace) -Context "Secret MinIO introuvable"

    $Health = Invoke-WebRequest -Uri $HealthEndpoint -UseBasicParsing -TimeoutSec 15
    if ($Health.StatusCode -ne 200) {
        throw "MinIO ne repond pas sur $HealthEndpoint."
    }
    Add-Report "PASS - MinIO Kubernetes accessible via le port-forward local $Port."

    $script:MinioUser = Get-SecretValue -Key "MINIO_ROOT_USER"
    $script:MinioPassword = Get-SecretValue -Key "MINIO_ROOT_PASSWORD"
    Add-Report "PASS - identifiants lus depuis le Secret Kubernetes sans affichage des valeurs."

    Invoke-Checked -Command "docker" -Arguments @("pull", $McImage) -Context "Telechargement de l'image MinIO Client"
    Invoke-Mc -Command "mc admin info k8s >/dev/null" -Context "Connexion MinIO avec mc" | Out-Null
    Add-Report "PASS - connexion authentifiee au cluster MinIO."

    $Data = New-TestData
    Add-Report "PASS - jeu de test RAW/CLEAN/CURATED cree localement."

    foreach ($Item in $Data) {
        Invoke-Mc -Command "mc mb --ignore-existing k8s/$($Item.Bucket) >/dev/null" -Context "Creation du bucket $($Item.Bucket)" | Out-Null
        Invoke-Mc -Command "mc cp /dr/source/$($Item.Bucket)/$($Item.File) k8s/$($Item.Bucket)/$RemotePrefix/$($Item.File) >/dev/null" -Context "Envoi de $($Item.Bucket)/$($Item.File)" | Out-Null
    }
    $LastWriteTime = Get-Date
    Add-Report "PASS - 3 objets ecrits dans MinIO (RAW, CLEAN et CURATED)."

    $BackupStart = Get-Date
    foreach ($Item in $Data) {
        Invoke-Mc -Command "mc mirror k8s/$($Item.Bucket)/$RemotePrefix /dr/backup/$($Item.Bucket) >/dev/null" -Context "Sauvegarde du prefixe $($Item.Bucket)/$RemotePrefix" | Out-Null
    }
    $BackupCompleted = Get-Date
    $BackupWindowSeconds = [math]::Round(($BackupCompleted - $BackupStart).TotalSeconds, 2)
    $WriteToBackupSeconds = [math]::Round(($BackupCompleted - $LastWriteTime).TotalSeconds, 2)

    $BackupFiles = @(Get-ChildItem -Path $BackupRoot -Recurse -File)
    if ($BackupFiles.Count -ne $Data.Count) {
        throw "La sauvegarde contient $($BackupFiles.Count) fichier(s), $($Data.Count) attendu(s)."
    }

    $ManifestLines = @()
    foreach ($File in $BackupFiles) {
        $Relative = $File.FullName.Substring($BackupRoot.Length).TrimStart([char[]]"\/")
        $Hash = (Get-FileHash -Path $File.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        $ManifestLines += "$Hash  $Relative"
    }
    Set-Content -Path $Manifest -Value $ManifestLines -Encoding UTF8

    Compress-Archive -Path (Join-Path $BackupRoot "*") -DestinationPath $Archive -Force
    $ArchiveHash = (Get-FileHash -Path $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
    $ArchiveSize = (Get-Item $Archive).Length
    Add-Report "PASS - sauvegarde locale creee : $($BackupFiles.Count) objets, archive=$ArchiveSize octets."
    Add-Report "SHA-256 archive : $ArchiveHash"
    Add-Report "Fenetre de sauvegarde : $BackupWindowSeconds s ; derniere ecriture vers sauvegarde terminee : $WriteToBackupSeconds s."
    Add-Report "RPO OBSERVE : 0 objet et 0 octet perdus dans le jeu teste."

    $RtoStart = Get-Date
    foreach ($Item in $Data) {
        Invoke-Mc -Command "mc rm --recursive --force k8s/$($Item.Bucket)/$RemotePrefix >/dev/null" -Context "Suppression controlee de $($Item.Bucket)/$RemotePrefix" | Out-Null
    }

    foreach ($Item in $Data) {
        $Stat = Invoke-McResult -Command "mc stat k8s/$($Item.Bucket)/$RemotePrefix/$($Item.File) >/dev/null"
        if ($Stat.ExitCode -eq 0) {
            throw "L'objet $($Item.Bucket)/$RemotePrefix/$($Item.File) existe encore apres la suppression controlee."
        }
    }
    Add-Report "PASS - incident simule : les 3 objets ont ete supprimes et sont indisponibles."

    Expand-Archive -Path $Archive -DestinationPath $RestoreStagingRoot -Force
    foreach ($Item in $Data) {
        Invoke-Mc -Command "mc mirror /dr/restore-staging/$($Item.Bucket) k8s/$($Item.Bucket)/$RemotePrefix >/dev/null" -Context "Restauration de $($Item.Bucket)/$RemotePrefix" | Out-Null
    }

    foreach ($Item in $Data) {
        Invoke-Mc -Command "mc cp k8s/$($Item.Bucket)/$RemotePrefix/$($Item.File) /dr/restored/$($Item.Bucket)/$($Item.File) >/dev/null" -Context "Telechargement de verification $($Item.Bucket)/$($Item.File)" | Out-Null
    }

    $RestoredCount = 0
    foreach ($Item in $Data) {
        $OriginalPath = Join-Path (Join-Path $SourceRoot $Item.Bucket) $Item.File
        $RestoredPath = Join-Path (Join-Path $RestoredRoot $Item.Bucket) $Item.File
        if (-not (Test-Path $RestoredPath)) {
            throw "Fichier restaure absent : $RestoredPath"
        }
        $OriginalHash = (Get-FileHash -Path $OriginalPath -Algorithm SHA256).Hash.ToLowerInvariant()
        $RestoredHash = (Get-FileHash -Path $RestoredPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($OriginalHash -ne $RestoredHash) {
            throw "Echec d'integrite pour $($Item.Bucket)/$($Item.File) : SHA-256 different."
        }
        $RestoredCount++
        Add-Report "PASS - integrite SHA-256 $($Item.Bucket)/$($Item.File) : $RestoredHash"
    }

    $RtoSeconds = [math]::Round(((Get-Date) - $RtoStart).TotalSeconds, 2)
    Add-Report "PASS - $RestoredCount/$($Data.Count) objets restaures et verifies."
    Add-Report "RTO OBSERVE : $RtoSeconds secondes entre l'incident simule et la verification complete."
    Add-Report "RESULTAT GLOBAL : PASS"
    Add-Report "Rapport : $Report"
    Add-Report "Archive de sauvegarde : $Archive"
    Add-Report "Manifest SHA-256 : $Manifest"
    Add-Report "Etat final : les objets restaures restent disponibles sous le prefixe $RemotePrefix pour inspection."
}
catch {
    Add-Report "RESULTAT GLOBAL : FAIL - $($_.Exception.Message)"
    Add-Report "Rapport : $Report"
    exit 1
}
finally {
    $script:MinioUser = $null
    $script:MinioPassword = $null
}
