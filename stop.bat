@echo off
REM ============================================================
REM  stop.bat  –  Stop all DE PoC containers
REM ============================================================
echo.
echo  Stopping all containers...
docker-compose down
echo.
echo  Done! Data is preserved in Docker volumes.
echo  To also delete all data: docker-compose down -v
echo.
pause
