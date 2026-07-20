@echo off
REM ============================================================
REM  start.bat  –  One-click launcher for the DE PoC project
REM  Double-click this file to start everything!
REM ============================================================

echo.
echo ============================================================
echo  DATA ENGINEERING POC  –  Starting Full Stack
echo ============================================================
echo.

REM Check Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Docker Desktop is not running!
    echo  Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo  [1/4] Building custom Airflow image...
docker-compose build --no-cache

echo.
echo  [2/4] Starting PostgreSQL...
docker-compose up -d postgres
timeout /t 15 /nobreak >nul

echo.
echo  [3/4] Running pipeline bootstrap (data + schemas)...
docker-compose up pipeline-init
echo.

echo  [4/4] Starting Airflow (init + scheduler + webserver)...
docker-compose up -d airflow-init
timeout /t 20 /nobreak >nul
docker-compose up -d airflow-scheduler airflow-webserver

echo.
echo ============================================================
echo  All services started!
echo.
echo  Airflow UI  : http://localhost:8080
echo  Username    : admin
echo  Password    : admin123
echo.
echo  PostgreSQL  : localhost:5432
echo  Database    : de_poc
echo  User        : de_user  /  Password: de_password123
echo.
echo  Next steps:
echo   1. Open http://localhost:8080
echo   2. Trigger DAG: 01_bronze_ingestion
echo   3. Trigger DAG: 02_silver_transform
echo   4. Trigger DAG: 03_gold_aggregation
echo ============================================================
echo.
pause
