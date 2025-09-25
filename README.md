# GSCCCA-Tax-Records-Scraper
A Django-controlled, Playwright-based scraper designed to extract real estate and personal property tax records from the GSCCCA (Georgia Superior Court Clerks' Cooperative Authority) website.

---

### Features

-   **Automated Scraping**: Automatically fetches real estate and tax record data.
-   **Dashboard Control**: Manages the scraping process through a user-friendly web interface.
-   **Comprehensive Data**: Extracts critical details like Address, Zipcode, and Total Due.
-   **PDF Generation**: Creates single-page PDFs for the scraped documents.
-   **Excel Export**: Saves all extracted data into a well-organized `.xlsx` file.

---

### Installation

Follow these steps to set up the project on your machine.

#### **Step 1: Download the Project**

First, download the project's zip file from GitHub and extract it to your desired location.

#### **Step 2: Added .env File**

Create a new file named `.env` in the root directory of the project. This file will store your environment variables.

#### **Step 3: Open Terminal**

Navigate to the extracted folder, right-click, and select the "Open in Terminal" option.

#### **Step 4: Run the Setup Script**

Execute the appropriate commands below, based on your operating system:

**For Windows Users (using PowerShell)**

If you are using PowerShell on Windows, run these two commands one by one:

Bypass the execution policy to allow the script to run:
```bash
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Run the setup script:
```bash

.\setup.ps1
```

**For Linux, macOS, or Windows (using Git Bash/WSL)**

If you are on Linux, macOS, or using Git Bash / WSL on Windows, use these commands instead:

Make the setup script executable:
```bash

chmod +x setup.sh
```

Run the setup script:
```bash
./setup.sh
```

**Finalizing Setup**

Once the setup process is successfully completed, your browser will automatically redirect you to the GSCCCA Data Scraper Dashboard. You can now control the entire scraping process from there.