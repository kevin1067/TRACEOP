@echo off
REM Orquestador TraceOP (Apache Airflow en Docker). Uso: orquestador.bat [iniciar^|detener^|logs]
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0orquestador.ps1" %*
