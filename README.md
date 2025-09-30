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

#### **Step 3: Install Tesseract OCR**

To enable image-based text extraction (OCR), you must install Tesseract OCR. Follow the instructions for your specific operating system:

#### Windows:

Download and run the [Tesseract-OCR installer from this link](https://github.com/UB-Mannheim/tesseract/wiki).

During installation, be sure to select the "Add to PATH" option. This is crucial for the scraper to find the Tesseract executable.

If you choose not to add it to your PATH, you will need to manually configure the path to tesseract.exe in the script.

#### macOS:

Open your Terminal and install Tesseract using Homebrew with the following command:
```bash

    brew install tesseract

    The Tesseract executable is typically installed at /usr/local/bin/tesseract.
```
#### Linux:
Open your Terminal and install Tesseract using your package manager. For Debian/Ubuntu-based systems, use:
```bash

        sudo apt-get install tesseract-ocr

        The Tesseract executable is typically installed at /usr/bin/tesseract.
```

#### **Step 4: Configure Tesseract Path in Script**

Update the lien_index_scraper.py file to automatically set the Tesseract path based on the operating system.
Python

#### load Tesseract path for different OS if needed
```bash
    import os
    import sys

    try:
        if os.name == "nt": # Windows
            pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        elif sys.platform == "darwin": # macOS
            pytesseract.pytesseract.tesseract_cmd = r"/usr/local/bin/tesseract"
        elif sys.platform.startswith("linux"): # Linux
            pytesseract.pytesseract.tesseract_cmd = r"/usr/bin/tesseract"
    except Exception as e:
        console.print(f"[red]Error setting up Tesseract: {e}[/red]")
```
#### **Step 5: Open Terminal**

Navigate to the extracted folder, right-click, and select the "Open in Terminal" option.

#### **Step 6: Run the Setup Script**

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