param(
    [string]$Namespace = "velibdata",
    [string]$StatefulSet = "minio",
    [string]$ClientPod = "minio-test-client",
    [string]$Bucket = "resilience-test"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$evidenceDir = Join-Path $root "evidence"
New-Item -ItemType Directory -Force -Path $evidenceDir | Out-Null
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$report = Join-Path $evidenceDir "minio-resilience-$timestamp.txt"
$originalReplicas = 4

function Add-Report {
    param([string]$Text)
    $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Text"
    $line | Tee-Object -FilePath $report -Append | Write-Host
}

function Assert-LastExit {
    param([string]$Context)
    if ($LASTEXITCODE -ne 0) {
        throw "$Context (code $LASTEXITCODE)"
    }
}

function Invoke-Client {
    param([string]$Command)

    # Certaines commandes Linux (par exemple dd) ecrivent des informations
    # normales sur stderr meme lorsque leur code de sortie vaut 0. Avec
    # ErrorActionPreference=Stop, Windows PowerShell les traite sinon comme
    # des erreurs. On se fie donc au code de sortie reel de kubectl.
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = & kubectl -n $Namespace exec $ClientPod -- /bin/sh -c $Command 2>&1
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    $text = ($output | ForEach-Object { $_.ToString() } | Out-String).Trim()

    if ($exitCode -ne 0) {
        throw "Commande MinIO en echec : $Command`n$text"
    }

    return $text
}

function Get-RemoteSha256 {
    param([string]$Path)

    $raw = Invoke-Client "sha256sum $Path"
    return (($raw -split '\s+')[0]).Trim()
}

try {
    Add-Report "TEST DE RESILIENCE MINIO - debut"
    Add-Report "Perimetre : perte volontaire d'une instance MinIO sur un cluster Kubernetes local mono-noeud."

    & kubectl -n $Namespace get secret minio-creds *> $null
    Assert-LastExit "Secret minio-creds absent. Lance scripts/create-k8s-secret.ps1"

    & kubectl -n $Namespace scale "statefulset/$StatefulSet" "--replicas=$originalReplicas" | Out-Host
    Assert-LastExit "Impossible de remettre le StatefulSet a 4 replicas"

    & kubectl -n $Namespace rollout status "statefulset/$StatefulSet" --timeout=300s | Out-Host
    Assert-LastExit "Les 4 pods MinIO ne sont pas prets"

    & kubectl -n $Namespace delete pod $ClientPod --ignore-not-found=true --wait=true | Out-Null
    & kubectl apply -f (Join-Path $root "k8s/05-minio-test-client.yaml") | Out-Host
    Assert-LastExit "Impossible de creer le client de test"

    & kubectl -n $Namespace wait --for=condition=Ready "pod/$ClientPod" --timeout=120s | Out-Host
    Assert-LastExit "Le client de test n'est pas pret"

    Invoke-Client 'mc alias set lake http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"' | Out-Null
    Invoke-Client "mc mb --ignore-existing lake/$Bucket" | Out-Null

    Invoke-Client 'dd if=/dev/urandom of=/tmp/probe-before.bin bs=1048576 count=5 2>/dev/null' | Out-Null
    $hashBefore = Get-RemoteSha256 "/tmp/probe-before.bin"

    Invoke-Client "mc cp /tmp/probe-before.bin lake/$Bucket/probe-before.bin" | Out-Null
    Invoke-Client "rm -f /tmp/probe-before-read.bin" | Out-Null
    Invoke-Client "mc cp lake/$Bucket/probe-before.bin /tmp/probe-before-read.bin" | Out-Null
    $hashBaseline = Get-RemoteSha256 "/tmp/probe-before-read.bin"

    if ($hashBefore -ne $hashBaseline) {
        throw "Hash baseline different : source=$hashBefore, objet=$hashBaseline"
    }

    Add-Report "BASELINE PASS - ecriture, lecture et SHA-256 identiques : $hashBefore"

    Add-Report "Passage volontaire de 4 a 3 pods : minio-3 devient indisponible."
    $degradedStart = Get-Date

    & kubectl -n $Namespace scale "statefulset/$StatefulSet" --replicas=3 | Out-Host
    Assert-LastExit "Echec du passage a 3 replicas"

    & kubectl -n $Namespace wait --for=delete "pod/$StatefulSet-3" --timeout=120s | Out-Host
    Assert-LastExit "minio-3 n'a pas ete supprime"

    Invoke-Client "rm -f /tmp/probe-degraded-read.bin" | Out-Null
    Invoke-Client "mc cp lake/$Bucket/probe-before.bin /tmp/probe-degraded-read.bin" | Out-Null
    $hashDuringRead = Get-RemoteSha256 "/tmp/probe-degraded-read.bin"

    if ($hashDuringRead -ne $hashBefore) {
        throw "Lecture degradee corrompue : attendu=$hashBefore, obtenu=$hashDuringRead"
    }

    Add-Report "DEGRADED READ PASS - objet lisible avec une instance indisponible."

    Invoke-Client 'dd if=/dev/urandom of=/tmp/probe-during.bin bs=1048576 count=1 2>/dev/null' | Out-Null
    $hashDuringSource = Get-RemoteSha256 "/tmp/probe-during.bin"

    Invoke-Client "mc cp /tmp/probe-during.bin lake/$Bucket/probe-during.bin" | Out-Null
    Invoke-Client "rm -f /tmp/probe-during-read.bin" | Out-Null
    Invoke-Client "mc cp lake/$Bucket/probe-during.bin /tmp/probe-during-read.bin" | Out-Null
    $hashDuringObject = Get-RemoteSha256 "/tmp/probe-during-read.bin"

    if ($hashDuringSource -ne $hashDuringObject) {
        throw "Ecriture degradee corrompue : attendu=$hashDuringSource, obtenu=$hashDuringObject"
    }

    Add-Report "DEGRADED WRITE PASS - nouvel objet ecrit et relu pendant la panne."

    try {
        $adminInfo = Invoke-Client "mc admin info lake"
        Add-Report "Etat MinIO en mode degrade :`n$adminInfo"
    }
    catch {
        Add-Report "INFO - mc admin info indisponible en mode degrade, sans impact sur les tests lecture/ecriture."
    }

    Add-Report "Retour a 4 pods et attente du retablissement."

    & kubectl -n $Namespace scale "statefulset/$StatefulSet" "--replicas=$originalReplicas" | Out-Host
    Assert-LastExit "Echec du retour a 4 replicas"

    & kubectl -n $Namespace rollout status "statefulset/$StatefulSet" --timeout=300s | Out-Host
    Assert-LastExit "Le cluster n'est pas revenu a 4 pods prets"

    $recoverySeconds = [math]::Round(((Get-Date) - $degradedStart).TotalSeconds, 1)

    Invoke-Client "rm -f /tmp/probe-final-before.bin /tmp/probe-final-during.bin" | Out-Null
    Invoke-Client "mc cp lake/$Bucket/probe-before.bin /tmp/probe-final-before.bin" | Out-Null
    Invoke-Client "mc cp lake/$Bucket/probe-during.bin /tmp/probe-final-during.bin" | Out-Null

    $hashAfter1 = Get-RemoteSha256 "/tmp/probe-final-before.bin"
    $hashAfter2 = Get-RemoteSha256 "/tmp/probe-final-during.bin"

    if (($hashAfter1 -ne $hashBefore) -or ($hashAfter2 -ne $hashDuringSource)) {
        throw "Controle final SHA-256 en echec apres recuperation."
    }

    Add-Report "RECOVERY PASS - 4 pods prets apres $recoverySeconds secondes."
    Add-Report "INTEGRITY PASS - les deux objets conservent leur SHA-256 apres recuperation."
    Add-Report "RESULTAT GLOBAL : PASS"
}
catch {
    Add-Report "RESULTAT GLOBAL : FAIL - $($_.Exception.Message)"
    throw
}
finally {
    & kubectl -n $Namespace scale "statefulset/$StatefulSet" "--replicas=$originalReplicas" *> $null
    Add-Report "Nettoyage de securite : consigne de retour a 4 replicas envoyee."
    Add-Report "Rapport : $report"
}
