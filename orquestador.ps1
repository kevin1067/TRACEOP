# Lanza la interfaz del orquestador TraceOP en http://localhost:3000
Set-Location $PSScriptRoot
$env:DAGSTER_HOME = Join-Path $PSScriptRoot ".dagster"
$env:PYTHONUTF8 = "1"
if (Test-Path "venv\Scripts\Activate.ps1") { . .\venv\Scripts\Activate.ps1 }
Write-Host "Iniciando orquestador TraceOP en http://localhost:3000 ..."
dagster dev -m orquestador.definitions -p 3000
