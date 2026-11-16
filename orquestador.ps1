# Orquestador TraceOP (Apache Airflow en Docker)
#   .\orquestador.ps1          -> levanta Airflow y abre http://localhost:8080
#   .\orquestador.ps1 detener  -> detiene los contenedores
#   .\orquestador.ps1 logs     -> muestra los logs del scheduler
param([string]$accion = "iniciar")
Set-Location (Join-Path $PSScriptRoot "airflow")

# Docker Desktop puede instalarse por usuario o para todo el equipo. Si la terminal
# se abrio antes de instalarlo, su PATH no lo incluye: se busca en las rutas conocidas.
$instalaciones = @(
    "$env:LOCALAPPDATA\Programs\DockerDesktop",
    "$env:ProgramFiles\Docker\Docker"
)
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    foreach ($dir in $instalaciones) {
        if (Test-Path "$dir\resources\bin\docker.exe") {
            $env:Path = "$dir\resources\bin;$env:Path"
            break
        }
    }
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "No se encontro Docker. Instale Docker Desktop: https://www.docker.com/products/docker-desktop/" -ForegroundColor Red
    exit 1
}

# El motor de Docker debe estar corriendo (Docker Desktop abierto)
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    $exe = $instalaciones | ForEach-Object { "$_\Docker Desktop.exe" } | Where-Object { Test-Path $_ } | Select-Object -First 1
    if (-not $exe) {
        Write-Host "Docker no esta corriendo. Abra Docker Desktop y vuelva a ejecutar el script." -ForegroundColor Red
        exit 1
    }
    Write-Host "Iniciando Docker Desktop..." -ForegroundColor Cyan
    Start-Process $exe
    $listo = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep 5
        docker info *> $null
        if ($LASTEXITCODE -eq 0) { $listo = $true; break }
    }
    if (-not $listo) {
        Write-Host "Docker Desktop no respondio en 5 minutos. Abralo manualmente y reintente." -ForegroundColor Red
        exit 1
    }
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
