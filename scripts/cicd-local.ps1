param(
    [switch]$RunUnitTests,
    [switch]$KeepSmokeRunning
)

$ErrorActionPreference = "Stop"
$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir
$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$EvidenceDir = Join-Path $RootDir "evidence"
New-Item -ItemType Directory -Path $EvidenceDir -Force | Out-Null
$Report = Join-Path $EvidenceDir "cicd-local-$Timestamp.txt"

function Add-Report {
    param([string]$Message)
    $Line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Write-Host $Line
    Add-Content -Path $Report -Value $Line -Encoding UTF8
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Context
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
}

try {
    Add-Report "PIPELINE LOCAL VELIBDATA - debut"

    if (-not (Test-Path ".env")) {
        throw "Le fichier .env est absent. Creez-le a partir de .env.example."
    }

    Invoke-Native -FilePath "docker" -Arguments @("compose", "config", "--quiet") -Context "Validation docker-compose.yml"
    Invoke-Native -FilePath "docker" -Arguments @("compose", "--file", "docker-compose.smoke.yml", "config", "--quiet") -Context "Validation docker-compose.smoke.yml"
    Add-Report "PASS - configurations Docker Compose valides."

    if ($RunUnitTests) {
        Invoke-Native -FilePath "python" -Arguments @("-m", "pytest", "tests", "-v") -Context "Tests unitaires"
        Add-Report "PASS - tests unitaires."
    }
    else {
        Add-Report "INFO - tests unitaires locaux ignores. GitHub Actions les executera automatiquement."
    }

    Add-Report "Construction des images applicatives."
    Invoke-Native -FilePath "docker" -Arguments @(
        "compose", "build", "--pull",
        "producer-availability", "producer-stations", "producer-weather", "great-expectations"
    ) -Context "Build Docker"
    Add-Report "PASS - images Docker construites."

    Invoke-Native -FilePath "docker" -Arguments @(
        "compose", "run", "--rm", "--no-deps", "producer-availability",
        "python", "-m", "py_compile", "velib_availability.py", "velib_stations.py", "weather.py"
    ) -Context "Smoke test image producers"

    Invoke-Native -FilePath "docker" -Arguments @(
        "compose", "run", "--rm", "--no-deps", "great-expectations",
        "python", "-m", "py_compile", "validate_quality.py"
    ) -Context "Smoke test image Great Expectations"
    Add-Report "PASS - images applicatives executables."

    $SmokeArgs = @("-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "smoke-tests.ps1"))
    if ($KeepSmokeRunning) {
        $SmokeArgs += "-KeepRunning"
    }
    Invoke-Native -FilePath "powershell" -Arguments $SmokeArgs -Context "Smoke tests infrastructure"
    Add-Report "PASS - deploiement temporaire et smoke tests."
    Add-Report "RESULTAT GLOBAL : PASS"
}
catch {
    Add-Report "RESULTAT GLOBAL : FAIL - $($_.Exception.Message)"
    throw
}
finally {
    Add-Report "Rapport : $Report"
}
