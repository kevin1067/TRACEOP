# Orquestador TraceOP (Apache Airflow en Docker)
#   .\orquestador.ps1          -> levanta Airflow y abre http://localhost:8080
#   .\orquestador.ps1 detener  -> detiene los contenedores
#   .\orquestador.ps1 logs     -> muestra los logs del scheduler
param([string]$accion = "iniciar")
Set-Location (Join-Path $PSScriptRoot "airflow")

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker no esta instalado. Instale Docker Desktop: https://www.docker.com/products/docker-desktop/" -ForegroundColor Red
    exit 1
}

switch ($accion) {
    "detener" { docker compose down }
    "logs"    { docker compose logs -f airflow-scheduler }
    default {
        Write-Host "Construyendo e iniciando Airflow (la primera vez tarda unos minutos)..." -ForegroundColor Cyan
        docker compose up -d --build
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Write-Host "Esperando a que la interfaz este lista..." -ForegroundColor Cyan
        $listo = $false
        for ($i = 0; $i -lt 60; $i++) {
            try {
                Invoke-WebRequest "http://localhost:8080/api/v2/monitor/health" -UseBasicParsing -TimeoutSec 3 | Out-Null
                $listo = $true
                break
            } catch { Start-Sleep 5 }
        }
        if (-not $listo) {
            Write-Host "La interfaz no respondio en 5 minutos. Revise: .\orquestador.ps1 logs" -ForegroundColor Yellow
            exit 1
        }
        Write-Host "Airflow listo en http://localhost:8080  (usuario: airflow / clave: airflow)" -ForegroundColor Green
        Start-Process "http://localhost:8080"
    }
}
