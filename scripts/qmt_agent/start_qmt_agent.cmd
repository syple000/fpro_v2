@echo off
setlocal
chcp 65001 >nul
set "QMT_BIN=C:\Program Files\东北证券NET专业版\bin.x64"
set "QMT_SHORTCUT=%USERPROFILE%\Desktop\东北证券NET专业版.lnk"

where uv >nul 2>nul
if errorlevel 1 (
    echo uv not found. Install it first: https://docs.astral.sh/uv/
    pause
    exit /b 1
)

pushd "%~dp0\..\.."
uv run --group qmt-agent --no-dev qmt-agent-start ^
  --qmt-bin "%QMT_BIN%" ^
  --qmt-shortcut "%QMT_SHORTCUT%" ^
  %*
set "QMT_AGENT_EXIT_CODE=%errorlevel%"
popd

if not "%QMT_AGENT_EXIT_CODE%"=="0" pause
exit /b %QMT_AGENT_EXIT_CODE%
