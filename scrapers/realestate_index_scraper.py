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
from dashboard.utils.state import stop_scraper_flag 

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


BASE_DIR = Path(__file__).parent.absolute() 
BASE_OUTPUT_DIR = os.path.join(BASE_DIR.parent, "output")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

REAL_ESTATE_DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "real_estate")
REAL_ESTATE_EXCEL_DIR = REAL_ESTATE_DATA_DIR 

PDF_DIR = os.path.join(REAL_ESTATE_DATA_DIR, "documents")
LIEN_INPUT_DATA_DIR = os.path.join(BASE_OUTPUT_DIR, "lien")

INPUT_EXCEL_SEARCH_DIR = LIEN_INPUT_DATA_DIR
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(REAL_ESTATE_EXCEL_DIR, exist_ok=True)

# console.print(f"[green]Real Estate Excel folder: {REAL_ESTATE_EXCEL_DIR}[/green]")
# console.print(f"[green]PDF Documents folder: {PDF_DIR}[/green]")
# console.print(f"[green]Base Output folder: {BASE_OUTPUT_DIR}[/green]")
# console.print(f"[yellow]Input Excel Search folder (Lien data): {INPUT_EXCEL_SEARCH_DIR}[/yellow]") # Added log for verification

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
    def __init__(self, params: dict) -> None:
        try:
            self.page = None
            self.email = TAX_EMAIL
            self.password = TAX_PASSWORD
            self.realestate_url = "https://search.gsccca.org/RealEstate/namesearch.asp"
            self.results = []
            self.params = params
            
            # Use global constants defined above
            self.input_excel_search_dir = INPUT_EXCEL_SEARCH_DIR 
            self.pdf_dir = PDF_DIR
            self.excel_output_dir = REAL_ESTATE_EXCEL_DIR
            
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
    
    async def step3_fill_form(self):
        """Fill the real estate form using the provided parameters."""
        console.print("[cyan]Filling form with provided parameters[/cyan]")
        try:
            await self.page.wait_for_selector("input[name='txtSearchName']")
            
            await self.page.select_option("select[name='txtPartyType']", self.params.get("txtPartyType", "2"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='txtInstrCode']", self.params.get("txtInstrCode", "ALL"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='intCountyID']", self.params.get("intCountyID", "-1"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            
            include_val = self.params.get("bolInclude", "0")
            checkbox_selector = f"input[name='bolInclude'][value='{include_val}']"
            if await self.page.query_selector(checkbox_selector):
                await self.page.check(checkbox_selector)
            
            await self.page.fill("input[name='txtSearchName']", self.params.get("txtSearchName", ""))
            await self.page.fill("input[name='txtFromDate']", self.params.get("txtFromDate", ""))
            await self.page.fill("input[name='txtToDate']", self.params.get("txtToDate", ""))
            
            await self.page.select_option("select[name='MaxRows']", self.params.get("MaxRows", "100"))
            await self.page.select_option("select[name='TableType']", self.params.get("TableType", "1"))
            
            await self.page.wait_for_timeout(self.time_sleep())
            await self.page.click("#btnSubmit")
            
        except Exception as e:
            console.print(f"[red]Error filling real estate form: {e}[/red]")
            raise

    async def step4_select_names_and_display(self, search_name: str):
        try:
            radios = await self.page.query_selector_all("input[name='rdoEntityName']")
            console.print(f"[cyan]Found {len(radios)} potential entity names for '{search_name}'[/cyan]")

            for i in range(len(radios)):
                # Add stop check
                if stop_scraper_flag['realestate']:
                    console.print("[yellow]Stop signal received. Stopping real estate scraper.[/yellow]")
                    break
                    
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
                # Add stop check
                if stop_scraper_flag['realestate']:
                    console.print("[yellow]Stop signal received. Stopping real estate scraper.[/yellow]")
                    break
                    
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
                        # Add stop check
                        if stop_scraper_flag['realestate']:
                            console.print("[yellow]Stop signal received. Stopping real estate scraper.[/yellow]")
                            if popup and popup != self.page: await popup.close()
                            return
                            
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
                                    # Use the new self.pdf_dir
                                    screenshot_path = self.pdf_dir / f"{safe_title}.png"
                                    await canvas.screenshot(path=str(screenshot_path))
                                    pdf_path = self.pdf_dir / f"{safe_title}.pdf"
                                    if image_to_pdf(screenshot_path, pdf_path):
                                        console.print(f"[blue]Saved PDF: {pdf_path}[/blue]")
                                        
                                        # Save result to self.results list
                                        relative_pdf_path = pdf_path.relative_to(BASE_DIR.parent) 
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
        """Save results to Excel file in 'Real estate data' folder"""
        if not self.results:
            console.print("[red]No results to save[/red]")
            return None

        try:
            df = pd.DataFrame(self.results)
            # Drop duplicates based on a combination of columns
            df.drop_duplicates(subset=["Search Name", "Real Estate PDF"], inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Use the new self.excel_output_dir
            self.excel_output_dir.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_filename = f"{filename_prefix}_{ts}.xlsx"
            final_path = self.excel_output_dir / final_filename

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

    async def run_dynamic(self):
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
            
            # Global stop check before starting core work
            if stop_scraper_flag['realestate']:
                console.print("[yellow]Scraper started but received immediate stop signal. Exiting.[/yellow]")
                await browser.close()
                await playwright.stop()
                return

            await self.step2_open_realestate_search()
            
            if stop_scraper_flag['realestate']:
                console.print("[yellow]Stop signal received after search start. Exiting.[/yellow]")
                await browser.close()
                await playwright.stop()
                return
            
            # Use form parameters to fill the form
            await self.step3_fill_form()
            
            # Agar search name hai to results ko process karein, varna skip karein
            if self.params.get("txtSearchName"):
                try:
                    await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    await asyncio.sleep(self.time_sleep())
                    await self.step4_select_names_and_display(self.params.get("txtSearchName"))
                except pw.TimeoutError:
                    console.print(f"[yellow]Search results for '{self.params.get('txtSearchName')}' timed out or not found.[/yellow]")
            else:
                console.print("[yellow]No search name provided, skipping search result processing.[/yellow]")
            
        except Exception as e:
            console.print(f"[red]Error in run_dynamic method: {e}[/red]")
            traceback.print_exc()
        finally:
            if browser:
                await browser.close()
            if playwright:
                await playwright.stop()

async def main():
    """Main function to run the scraper."""
    # Dummy parameters for local testing
    params = {
        'txtPartyType': '2',
        'txtInstrCode': 'ALL',
        'intCountyID': '64',
        'bolInclude': '0',
        'txtSearchName': '1290 VETERANS MEMORIAL LLC',
        'txtFromDate': '01/01/1990',
        'txtToDate': '08/17/2025',
        'MaxRows': '100',
        'TableType': '1'
    }
    scraper = RealestateIndexScraper(params)
    try:
        await scraper.run_dynamic()
        
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
        console.print(f"\n[bold red]Unexpected error: {e}[/bold red]\\n")
        traceback.print_exc()
    finally:
        console.print("[bold green]Exiting...[/bold green]")