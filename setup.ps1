# Cross-platform setup for Django + Scrapers
# Works on: Windows (PowerShell)
# Usage: .\setup.ps1

# Strict mode jise errors par script stop ho jaaye
$ErrorActionPreference = "Stop"

Write-Host "Checking Python installation..."

# Python command ko dhoondna
if (Get-Command python -ErrorAction SilentlyContinue) {
    $PYTHON = "python"
}
elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
    $PYTHON = "python3"
}
else {
    Write-Host "Python not found. Please install Python (3.9+ recommended)."
    exit 1
}

Write-Host "Using Python: $(& $PYTHON --version)"

# Create virtual environment if missing
if (-not (Test-Path -Path "venv" -PathType Container)) {
    Write-Host "Creating virtual environment..."
    & $PYTHON -m ensurepip --upgrade | Out-Null
    & $PYTHON -m venv venv
}
else {
    Write-Host "Virtual environment already exists."
}

# Activate venv
if (Test-Path -Path "venv/Scripts/Activate.ps1") {
    Write-Host "Activating virtual environment."
    . ".\venv\Scripts\Activate.ps1"
}
else {
    Write-Host "Could not find venv activate script. Exiting."
    exit 1
}

# Install dependencies
if (Test-Path -Path "requirements.txt") {
    Write-Host "Installing Python dependencies from requirements.txt..."
    & $PYTHON -m pip install --upgrade pip setuptools wheel
    & $PYTHON -m pip install -r requirements.txt
}
else {
    Write-Host "requirements.txt not found - skipping pip install."
}

# Change directory to core
Write-Host "Changing directory to core..."
if (Test-Path -Path "core" -PathType Container) {
    Set-Location -Path "core"
    Write-Host "Directory changed to core."
}
else {
    Write-Host "core directory not found. Exiting."
    exit 1
}

# Run Django migrations
if (Test-Path -Path "manage.py") {
    Write-Host "Running Django migrations..."
    & $PYTHON manage.py migrate
}
else {
    Write-Host "manage.py not found - skipping migrations."
}

# Start Django development server
if (Test-Path -Path "manage.py") {
    Write-Host "Starting Django development server at http://127.0.0.1:8000 ..."

    Start-Process -FilePath $PYTHON -ArgumentList "manage.py", "runserver" -NoNewWindow
    
    Write-Host "Waiting for server to start..."
    Start-Sleep -Seconds 3

    $URL = "http://127.0.0.1:8000/"
    Write-Host "Opening $URL in your default web browser..."
    Start-Process -FilePath $URL

    Write-Host "Server is running. Quit with CTRL-C in this terminal to stop."
    Read-Host -Prompt "Press Enter to stop the server..."
}
else {
    Write-Host "manage.py not found - cannot start server."
    exit 1
}