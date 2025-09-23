#!/usr/bin/env bash
set -euo pipefail

# Cross-platform setup for Django + Scrapers
# Works on: Linux, macOS, Windows (Git Bash / WSL)
# Usage: bash setup.sh

echo "🔍 Checking Python installation..."

if command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
elif command -v python >/dev/null 2>&1; then
    PYTHON=python
else
    echo "❌ Python not found. Please install Python (3.9+ recommended)."
    exit 1
fi

echo "✅ Using Python: $($PYTHON --version 2>&1)"

# -------------------------------
# Create virtual environment if missing
# -------------------------------
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    # try to ensure pip is available (best-effort)
    $PYTHON -m ensurepip --upgrade >/dev/null 2>&1 || true
    $PYTHON -m venv venv
else
    echo "✔️ Virtual environment already exists."
fi

# -------------------------------
# Activate venv (cross-platform for bash)
# -------------------------------
# Prefer Windows Git Bash path first, then POSIX one.
if [ -f "venv/Scripts/activate" ]; then
    # Git Bash on Windows
    # shellcheck disable=SC1091
    source venv/Scripts/activate
elif [ -f "venv/bin/activate" ]; then
    # Linux / macOS
    # shellcheck disable=SC1091
    source venv/bin/activate
else
    echo "❌ Could not find venv activate script. If you're on PowerShell run the PowerShell version or create venv manually."
    exit 1
fi

echo "✅ Virtual environment activated."

# -------------------------------
# Install dependencies
# -------------------------------
if [ -f "requirements.txt" ]; then
    echo "📦 Installing Python dependencies from requirements.txt..."
    python -m pip install --upgrade pip setuptools wheel
    python -m pip install -r requirements.txt
else
    echo "⚠️ requirements.txt not found — skipping pip install."
fi

# -------------------------------
# Change directory to mydashboard
# -------------------------------
echo "➡️ Changing directory to mydashboard..."
if [ -d "mydashboard" ]; then
    cd mydashboard
    echo "✅ Directory changed to mydashboard."
else
    echo "❌ mydashboard directory not found. Exiting."
    exit 1
fi

# -------------------------------
# Run Django migrations
# -------------------------------
if [ -f "manage.py" ]; then
    echo "⚙️ Running Django migrations..."
    python manage.py migrate
else
    echo "⚠️ manage.py not found — skipping migrations."
fi


# -------------------------------
# Start Django development server
# -------------------------------
if [ -f "manage.py" ]; then
    echo "🌐 Starting Django development server at http://127.0.0.1:8000 ..."

    # Server ko background mein run karein
    nohup python manage.py runserver >/dev/null 2>&1 &

    # Thoda wait karein taaki server start ho jaaye
    echo "⏳ Waiting for server to start..."
    sleep 3  # Aap isko 5 ya 10 seconds tak bhi kar sakte hain agar zaroorat pade

    # Browser mein URL open karein (cross-platform)
    URL="http://127.0.0.1:8000/"
    echo "➡️ Opening $URL in your default web browser..."

    if command -v xdg-open >/dev/null 2>&1; then
        xdg-open "$URL"
    elif command -v open >/dev/null 2>&1; then
        open "$URL"
    elif command -v start >/dev/null 2>&1; then
        start "$URL"
    else
        echo "❌ Could not find a command to open the browser automatically. Please open the URL manually."
    fi

    echo "✅ Server is running. Quit with CTRL-C in this terminal to stop."
    # Wait karein taaki user CTRL-C se server stop kar sakein
    wait
else
    echo "❌ manage.py not found — cannot start server."
    exit 1
fi