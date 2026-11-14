@echo off
REM Lanza la interfaz del orquestador TraceOP en http://localhost:3000
cd /d "%~dp0"
set "DAGSTER_HOME=%~dp0.dagster"
set PYTHONUTF8=1
if exist venv\Scripts\activate.bat call venv\Scripts\activate.bat
echo Iniciando orquestador TraceOP en http://localhost:3000 ...
dagster dev -m orquestador.definitions -p 3000
