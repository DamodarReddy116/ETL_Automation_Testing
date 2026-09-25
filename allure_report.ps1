$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$resultsDir = Join-Path $root "allure-results"
$reportDir = Join-Path $root "allure-report"
$allureBin = Join-Path $root "tools\allure\allure-2.28.0\bin\allure.bat"

if (-not (Test-Path $resultsDir)) {
    Write-Error "Allure results directory not found at $resultsDir. Run pytest --alluredir=allure-results first."
    exit 1
}

if (-not (Test-Path $allureBin)) {
    Write-Error "Allure CLI not found at $allureBin. Download it or re-run the setup step."
    exit 1
}

& $allureBin generate $resultsDir --clean -o $reportDir
Write-Host "Allure report generated at: $reportDir"
