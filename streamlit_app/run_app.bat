@echo off
REM NFL Fantasy Dashboard — Launch Script

set STREAMLIT=C:\Users\cincy\AppData\Local\r-reticulate\r-reticulate\pyenv\pyenv-win\versions\3.9.13\Scripts\streamlit.exe
set APP=%~dp0app.py

REM Change to repo root (one level up from streamlit_app/)
cd /d "%~dp0.."

echo Starting NFL Player Dashboard...
echo Open http://localhost:8501 in your browser.
echo.

"%STREAMLIT%" run "%APP%" --server.port 8501 --server.headless false --browser.gatherUsageStats false
pause
