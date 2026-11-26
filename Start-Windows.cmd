@echo off
cd /d "%~dp0"
if not exist ".env" (
  echo Copy .env.example to .env and add your own Kalshi DEMO credentials first.
  pause
  exit /b 1
)
if not exist "bitcoin-prediction-bot.exe" (
  echo Build with Build-Windows.ps1 or download the Windows release ZIP.
  pause
  exit /b 1
)
set "KALSHI_MODE=demo"
set "KALSHI_DATA_MODE=demo"
set "KALSHI_PAPER=0"
set "KALSHI_ENV_PATH=%~dp0.env"
set "DASHBOARD_PORT=5150"
echo Starting DEMO exchange trading. Stop with Ctrl+C.
echo Dashboard: http://127.0.0.1:5150
"%~dp0bitcoin-prediction-bot.exe" --config "%~dp0config.yaml"
pause
