from __future__ import annotations

import os
import json
import random
import asyncio
import traceback
import ssl
import certifi
import pandas as pd
import re
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw
from datetime import datetime
from PIL import Image
import img2pdf

load_dotenv()
console = Console()

# ---------- Config -------------------------------------------------------------
HEADLESS = False
STATE_FILE = Path("cookies.json")
TAX_EMAIL = os.getenv("GSCCCA_USERNAME")
TAX_PASSWORD = os.getenv("GSCCCA_PASSWORD")
LOCALE = "en-GB"
TIMEZONE = "UTC"
VIEWPORT = {"width": 1366, "height": 900}
UA_DICT = {
    "mac": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "linux": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "win": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome"
}
UA_TYPE = "win"
UA = UA_DICT.get(UA_TYPE, UA_DICT["win"])
EXTRA_HEADERS = {"Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}

EXCEL_FILE = "LienResults.xlsx"
FIRSTNAME_COL = "Direct Party (Debtor)"

## ---------- Output Directories -------------------------------------------------
BASE_DIR = Path(__file__).parent.absolute()
OUTPUT_DIR = BASE_DIR / "Output"
REAL_ESTATE_EXCEL_DIR = BASE_DIR / "Real estate excel"
PDF_DIR = BASE_DIR / "realestate_documents"

OUTPUT_DIR.mkdir(exist_ok=True)
REAL_ESTATE_EXCEL_DIR.mkdir(exist_ok=True)
PDF_DIR.mkdir(exist_ok=True)

console.print(f"[green]Real Estate Excel folder: {REAL_ESTATE_EXCEL_DIR}[/green]")
console.print(f"[green]PDF Documents folder: {PDF_DIR}[/green]")
console.print(f"[green]Output folder: {OUTPUT_DIR}[/green]")

# ---------- Utility Function to find latest Excel file -------------------------
def check_and_wait_for_excel_file(folder_path: Path, timeout=60) -> Path | None:
    """Wait for Excel file to appear in folder"""
    import time
    
    console.print(f"[yellow]Waiting for Excel file in {folder_path}...[/yellow]")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        latest_file = find_latest_excel_file(folder_path)
        if latest_file:
            return latest_file
        
        console.print("[yellow]Excel file not found yet, waiting 5 seconds...[/yellow]")
        time.sleep(5)
    
    return None

def find_latest_excel_file(folder_path: Path) -> Path | None:
    """
    Finds the latest modified Excel file (.xlsx or .xls) in a given folder.
    """
    try:
        if not folder_path.exists():
            console.print(f"[yellow]Folder does not exist: {folder_path}[/yellow]")
            return None
            
        files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix in ('.xlsx', '.xls', '.csv')]
        if not files:
            console.print(f"[yellow]No Excel/CSV files found in: {folder_path}[/yellow]")
            return None
        
        latest_file = max(files, key=os.path.getmtime)
        console.print(f"[green]Latest Excel/CSV file found: {latest_file.name}[/green]")
        return latest_file
    except Exception as e:
        console.print(f"[red]Error finding latest file: {e}[/red]")
        return None

def image_to_pdf(img_path, pdf_path):
    try:
        with Image.open(img_path) as image:
            pdf_bytes = img2pdf.convert(image.filename)
            with open(pdf_path, "wb") as f:
                f.write(pdf_bytes)
        return True
    except Exception as e:
        console.print(f"[red]Failed to convert image {img_path} to PDF: {e}[/red]")
        return False

# ---------- Scraper Class -----------------------------------------------------
class RealestateIndexScraper:
    def __init__(self) -> None:
        try:
            self.page = None
            self.email = TAX_EMAIL
            self.password = TAX_PASSWORD
            self.realestate_url = "https://search.gsccca.org/RealEstate/namesearch.asp"
            self.results = []
        except Exception as e:
            console.print(f"[red]Error initializing RealestateIndexScraper: {e}[/red]")
            raise

    def time_sleep(self, a=1.5, b=3.0) -> float:
        try:
            return random.uniform(a, b)
        except Exception as e:
            console.print(f"[red]Error in time_sleep: {e}[/red]")
            return random.uniform(1.0, 2.0)

    async def login(self) -> bool:
        try:
            console.print("[yellow]Starting login process...[/yellow]")
            await self.page.goto("https://apps.gsccca.org/login.asp", wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            
            await self.check_and_handle_announcement()
            
            await self.page.fill("input[name='txtUserID']", self.email)
            await self.page.fill("input[name='txtPassword']", self.password)
            await self.page.wait_for_timeout(2000) 
            
            checkbox = await self.page.query_selector("input[type='checkbox'][name='permanent']")
            if checkbox:
                is_checked = await checkbox.is_checked()
                if not is_checked:
                    await checkbox.click()
            
            try:
                await self.page.click("img[name='logon']")
            except Exception:
                await self.page.evaluate("document.forms['frmLogin'].submit()")
            
            await self.page.wait_for_load_state("networkidle", timeout=15000)
            
            if await self.page.query_selector("a:has-text('Logout')"):
                console.print("[green]Login successful[/green]")
                state = await self.page.context.storage_state()
                Path(STATE_FILE).write_text(json.dumps(state, indent=2))
                return True
            
            console.print("[red]Login failed.[/red]")
            return False
        except Exception as e:
            console.print(f"[red]Error during login: {e}[/red]")
            traceback.print_exc()
            return False

    async def check_and_handle_announcement(self):
        try:
            if "CustomerCommunicationApiAnnouncement1.asp" in self.page.url:
                console.print("[yellow]Dismissing announcement[/yellow]")
                await self.page.select_option("#Options", "dismiss")
                await self.page.wait_for_timeout(1500)
                await self.page.click("input[name='Continue']")
                await self.page.wait_for_timeout(2000)
        except Exception as e:
            console.print(f"[red]Error handling announcement: {e}[/red]")

    async def step2_open_realestate_search(self):
        try:
            console.print("[cyan]Opening Real Estate Name Search page[/cyan]")
            await self.page.goto(self.realestate_url, wait_until="domcontentloaded", timeout=60000)
            await self.check_and_handle_announcement()
            await self.page.wait_for_timeout(self.time_sleep())
        except Exception as e:
            console.print(f"[red]Error opening real estate search: {e}[/red]")
            raise

    async def step3_fill_form_from_excel(self):
        try:
            console.print("[cyan]Reading Excel and performing searches[/cyan]")
            
            latest_excel_path = check_and_wait_for_excel_file(OUTPUT_DIR)
            
            if not latest_excel_path:
                raise FileNotFoundError(f"No Excel/CSV file found in '{OUTPUT_DIR}'. Please place a file there.")
            
            console.print(f"[yellow]Using latest file: {latest_excel_path.name}[/yellow]")
            
            if latest_excel_path.suffix == '.csv':
                df = pd.read_csv(latest_excel_path)
            else:
                df = pd.read_excel(latest_excel_path)
            
            if FIRSTNAME_COL not in df.columns:
                raise ValueError(f"File must have a column named: '{FIRSTNAME_COL}'")

            console.print(f"[yellow]Total rows found: {len(df)}[/yellow]")
            
            for idx, row in df.iterrows():
                try:
                    console.print(f"[yellow]Processing row {idx+1}/{len(df)}...[/yellow]")
                    raw_name = str(row[FIRSTNAME_COL]).strip()
                    if not raw_name or raw_name.lower() in ["nan", "not found"]:
                        console.print(f"[red]Skipping row {idx+1} due to empty/invalid name.[/red]")
                        continue

                    # Handle multiple names in one cell
                    search_names = [name.strip() for name in raw_name.split(';')]
                    
                    for search_name in search_names:
                        if not search_name:
                            continue
                        
                        console.print(f"[blue]Searching -> {search_name}[/blue]")
                        
                        # Navigate to search page before each new search
                        await self.page.goto(self.realestate_url, wait_until="domcontentloaded", timeout=30000)
                        await self.check_and_handle_announcement()
                        await self.page.wait_for_selector("input[name='txtSearchName']")
                        
                        await self.page.fill("input[name='txtSearchName']", search_name)
                        await self.page.click("#btnSubmit")
                        
                        # Wait for results or timeout
                        try:
                            await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                            await asyncio.sleep(self.time_sleep())
                            await self.step4_select_names_and_display(search_name)
                        except pw.TimeoutError:
                            console.print(f"[yellow]Search results for '{search_name}' timed out or not found.[/yellow]")
                            continue
                            
                except Exception as e:
                    console.print(f"[red]Error processing row {idx+1}: {e}[/red]")
                    continue

        except Exception as e:
            console.print(f"[red]Error in step3_fill_form_from_excel: {e}[/red]")
            raise
    
    async def step4_select_names_and_display(self, search_name: str):
        try:
            radios = await self.page.query_selector_all("input[name='rdoEntityName']")
            console.print(f"[cyan]Found {len(radios)} potential entity names for '{search_name}'[/cyan]")

            for i in range(len(radios)):
                try:
                    # Refreshing radio buttons list to avoid staleness
                    current_radios = await self.page.query_selector_all("input[name='rdoEntityName']")
                    radio = current_radios[i]

                    if not radio:
                        console.print(f"[yellow]Radio button {i+1} not found[/yellow]")
                        continue

                    await radio.scroll_into_view_if_needed()
                    await radio.wait_for_element_state("visible", timeout=10000)
                    await radio.click()

                    await self.page.click("#btnDisplayDetails")
                    await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    await asyncio.sleep(self.time_sleep())

                    console.print(f"[blue]Opened Display Details for entity {i+1}[/blue]")

                    await self.step5_parse_documents(search_name, i+1)

                    # Go back to entity selection
                    await self.page.go_back()
                    await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    await asyncio.sleep(self.time_sleep())
                
                except Exception as e:
                    console.print(f"[red]Error processing entity {i+1}: {e}[/red]")
                    continue

        except Exception as e:
            console.print(f"[red]Step 4 error: {e}[/red]")
            traceback.print_exc()

    async def step5_parse_documents(self, search_name: str, entity_idx: int):
        try:
            links = await self.page.query_selector_all("a[href*='final.asp']")
            console.print(f"[cyan]Found {len(links)} GE/GR document links for entity {entity_idx}[/cyan]")

            if not links:
                console.print(f"[yellow]No document links found for entity {entity_idx}[/yellow]")
                return

            hrefs = []
            for link in links:
                href = await link.get_attribute("href")
                if href:
                    if "fnSubmitThisForm" in href:
                        inner = href.split("fnSubmitThisForm('")[1].split("')")[0]
                        pdf_url = f"https://search.gsccca.org/RealEstate/{inner}"
                    else:
                        pdf_url = href
                    hrefs.append(pdf_url)

            for i, pdf_url in enumerate(hrefs):
                try:
                    console.print(f"[green]Opening doc {i + 1} for entity {entity_idx}[/green]")
                    await self.page.goto(pdf_url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(self.time_sleep())

                    # --- Open popup or use same page ---
                    popup = None
                    try:
                        async with self.page.context.expect_page(timeout=5000) as popup_info:
                            view_button = await self.page.query_selector("input[value='View Image']")
                            if view_button:
                                await view_button.click()
                        popup = await popup_info.value
                        await popup.wait_for_load_state("domcontentloaded")
                        console.print(f"[green]Popup opened for doc {i+1}[/green]")
                    except Exception:
                        console.print(f"[yellow]No popup opened, using main page for doc {i+1}[/yellow]")
                        popup = self.page

                    # --- Collect thumbnails ---
                    thumb_links = await popup.query_selector_all("a[id*='lvThumbnails_lnkThumbnail']")
                    console.print(f"[cyan]Found {len(thumb_links)} thumbnails in viewer[/cyan]")

                    for j, thumb_link in enumerate(thumb_links):
                        try:
                            await thumb_link.click()
                            await popup.wait_for_timeout(2000)

                            # --- Extract Book & Page Number from header ---
                            try:
                                header_text = await popup.inner_text("#lblHeader")
                                match = re.search(r"Book\s+(\d+)\s+Page\s+(\d+)", header_text)
                                if match:
                                    book_no = match.group(1)
                                    page_no = match.group(2)
                                    safe_title = f"RE_Book_{book_no}_Page_{page_no}"
                                else:
                                    safe_title = f"Entity_{entity_idx}_Doc_{i+1}_Page_{j+1}"
                            except Exception:
                                safe_title = f"Entity_{entity_idx}_Doc_{i+1}_Page_{j+1}"

                            # --- Screenshot canvas and save to PDF ---
                            try:
                                canvas = await popup.query_selector("canvas")
                                if canvas:
                                    screenshot_path = PDF_DIR / f"{safe_title}.png"
                                    await canvas.screenshot(path=str(screenshot_path))
                                    pdf_path = PDF_DIR / f"{safe_title}.pdf"
                                    if image_to_pdf(screenshot_path, pdf_path):
                                        console.print(f"[blue]Saved PDF: {pdf_path}[/blue]")
                                        
                                        # IMPORTANT: Save result to self.results list
                                        relative_pdf_path = pdf_path.relative_to(BASE_DIR)
                                        result_data = {
                                            "Search Name": search_name,
                                            "Entity Index": entity_idx,
                                            "Doc Index": i + 1,
                                            "Page Index": j + 1,
                                            "PDF Viewer URL": popup.url,
                                            "Real Estate PDF": str(relative_pdf_path)
                                        }
                                        self.results.append(result_data)
                                        console.print(f"[green]Added to results: {result_data['Real Estate PDF']}[/green]")
                                        
                                    else:
                                        console.print(f"[red]PDF conversion failed for {screenshot_path.name}[/red]")

                                    # Delete PNG after conversion
                                    if screenshot_path.exists():
                                        screenshot_path.unlink()

                                else:
                                    console.print("[yellow]Canvas not found for screenshot[/yellow]")
                            
                            except Exception as e:
                                console.print(f"[red]Error saving PDF for thumbnail {j+1}: {e}[/red]")
                                traceback.print_exc()

                        except Exception as e:
                            console.print(f"[red]Error processing thumbnail {j+1}: {e}[/red]")
                            continue

                    # --- Close popup automatically ---
                    if popup and popup != self.page:
                        await popup.close()
                        await asyncio.sleep(1)
                        console.print(f"[green]Popup closed for doc {i+1}[/green]")
                
                except Exception as e:
                    console.print(f"[red]Error processing document {i+1}: {e}[/red]")
                    traceback.print_exc()
                    continue

        except Exception as e:
            console.print(f"[bold red]Fatal error in step5_parse_documents: {e}[/bold red]")
            traceback.print_exc()

    def save_results_to_excel(self, filename_prefix="realestate_index"):
        """Save results to Excel file in 'Real estate excel' folder"""
        if not self.results:
            console.print("[red]No results to save[/red]")
            return None

        try:
            df = pd.DataFrame(self.results)
            # Drop duplicates based on a combination of columns
            df.drop_duplicates(subset=["Search Name", "Real Estate PDF"], inplace=True)
            df.reset_index(drop=True, inplace=True)

            REAL_ESTATE_EXCEL_DIR.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_filename = f"{filename_prefix}_{ts}.xlsx"
            final_path = REAL_ESTATE_EXCEL_DIR / final_filename

            with pd.ExcelWriter(final_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Real Estate Data', index=False)
                workbook = writer.book
                worksheet = writer.sheets['Real Estate Data']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            console.print(f"[green]Real Estate Excel saved -> {final_path}[/green]")
            file_size = final_path.stat().st_size / 1024
            console.print(f"[blue]File size: {file_size:.2f} KB[/blue]")
            console.print(f"[blue]Total records saved: {len(df)}[/blue]")
            
            return final_path
            
        except Exception as e:
            console.print(f"[red]Failed to save Excel: {e}[/red]")
            traceback.print_exc()
            return None

    async def run(self):
        playwright = None
        browser = None
        try:
            playwright = await pw.async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=HEADLESS,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled", "--start-maximized"]
            )

            if STATE_FILE.exists():
                context = await browser.new_context(
                    storage_state=STATE_FILE,
                    user_agent=UA,
                    locale=LOCALE,
                    timezone_id=TIMEZONE,
                    viewport=VIEWPORT,
                    device_scale_factor=1,
                    extra_http_headers=EXTRA_HEADERS,
                )
            else:
                context = await browser.new_context(
                    user_agent=UA,
                    locale=LOCALE,
                    timezone_id=TIMEZONE,
                    viewport=VIEWPORT,
                    device_scale_factor=1,
                    extra_http_headers=EXTRA_HEADERS,
                    ignore_https_errors=True,
                )

            self.page = await context.new_page()
            
            # Check if login is needed
            await self.page.goto("https://apps.gsccca.org/", wait_until="domcontentloaded", timeout=30000)
            if not await self.page.query_selector("a:has-text('Logout')"):
                await self.login()
                if not await self.page.query_selector("a:has-text('Logout')"):
                    console.print("[red]Login failed. Exiting...[/red]")
                    return

            await self.step2_open_realestate_search()
            await self.step3_fill_form_from_excel()
            
        except Exception as e:
            console.print(f"[red]Error in run method: {e}[/red]")
            traceback.print_exc()
        finally:
            if browser:
                await browser.close()
            if playwright:
                await playwright.stop()

async def main():
    """Main function to run the scraper."""
    scraper = RealestateIndexScraper()
    try:
        await scraper.run()
        
        # Ab, sirf ek baar check karein aur save karein
        if scraper.results:
            console.print(f"[bold green]Total results collected: {len(scraper.results)}[/bold green]")
            excel_path = scraper.save_results_to_excel()
            if excel_path:
                console.print(f"[bold green]✓ Real Estate data successfully saved to: {excel_path}[/bold green]")
            else:
                console.print("[bold red]✗ Failed to save Excel file[/bold red]")
        else:
            console.print("[yellow]No results to save[/yellow]")
            print("⚠️ No results found, nothing to save.")
            
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Interrupted by user![/bold yellow]\n")
    except Exception as e:
        console.print(f"\n[bold red]Unexpected error: {e}[/bold red]\n")
        traceback.print_exc()
    finally:
        console.print("[bold green]Exiting...[/bold green]")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Interrupted by user![/bold yellow]\n")
    except Exception as e:
        console.print(f"\n[bold red]Unexpected error: {e}[/bold red]\n")
        traceback.print_exc()
    finally:
        console.print("[bold green]Exiting...[/bold green]")
        