@echo off
setlocal
where uv >nul 2>nul
if errorlevel 1 (
    echo uv not found. Install it first: https://docs.astral.sh/uv/
    pause
    exit /b 1
)

pushd "%~dp0\..\.."
uv run --extra qmt-agent --no-dev python "%~dp0start_qmt_agent.py" %*
set "QMT_AGENT_EXIT_CODE=%errorlevel%"
popd

if not "%QMT_AGENT_EXIT_CODE%"=="0" pause
exit /b %QMT_AGENT_EXIT_CODE%
