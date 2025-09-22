from __future__ import annotations

import os
import json
import random
import asyncio
import traceback
import ssl
import certifi
import pandas as pd
import base64
import re
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw
from datetime import datetime
from PIL import Image  # Add this import for image to PDF conversion
import img2pdf  # Import img2pdf for image to PDF conversion

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

EXCEL_FILE = "LienResults.xlsx"  # This is no longer used, but kept for context.
FIRSTNAME_COL = "Direct Party (Debtor)"

# ---------- Utility Function to find latest Excel file -------------------------
def find_latest_excel_file(folder_path: Path) -> Path | None:
    """
    Finds the latest modified Excel file (.xlsx or .xls) in a given folder.
    """
    try:
        files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix in ('.xlsx', '.xls')]
        if not files:
            return None
        
        latest_file = max(files, key=os.path.getmtime)
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
            self.ssl_context = ssl.create_default_context(cafile=certifi.where())
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
            await self.page.goto("https://apps.gsccca.org/login.asp", wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()  
            await self.page.fill("input[name='txtUserID']", self.email)
            await self.page.fill("input[name='txtPassword']", self.password)
            await self.page.wait_for_timeout(2000) 

            console.print("[LOGIN] Checking 'Remember login details' checkbox if not already checked...")
            checkbox = await self.page.query_selector("input[type='checkbox'][name='permanent']")
            if checkbox:
                is_checked = await checkbox.is_checked()
                if not is_checked:
                    await checkbox.click()
                    console.print("[LOGIN] Checkbox clicked.")
                else:
                    console.print("[LOGIN] Checkbox already checked.")
            else:
                console.print("[LOGIN] Checkbox not found on the page.")
            
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
            return False
        except Exception as e:
            console.print(f"[red]Error during login: {e}[/red]")
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
            
            output_folder = Path("Output") # Specify the folder to look for files
            latest_excel_path = find_latest_excel_file(output_folder)
            
            if not latest_excel_path:
                raise FileNotFoundError(f"No Excel file found in '{output_folder}'. Please place a file there.")
            
            console.print(f"[yellow]Using latest Excel file: {latest_excel_path.name}[/yellow]")
            
            df = pd.read_excel(latest_excel_path)
            
            if FIRSTNAME_COL not in df.columns:
                raise ValueError(f"Excel must have a column named: '{FIRSTNAME_COL}'")

            console.print(f"[yellow]Total rows found: {len(df)}[/yellow]")
            
            for idx, row in df.iterrows():
                try:
                    console.print(f"[yellow]Processing row {idx+1}...[/yellow]")
                    raw_name = str(row[FIRSTNAME_COL]).strip()
                    if not raw_name or raw_name.lower() == "nan":
                        console.print(f"[red]Skipping row {idx+1} due to empty name.[/red]")
                        continue

                    search_name = raw_name.split(";")[0].strip()
                    console.print(f"[blue]Searching -> {search_name}[/blue]")

                    await self.page.fill("input[name='txtSearchName']", search_name)
                    await self.page.click("#btnSubmit")
                    await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    await asyncio.sleep(self.time_sleep())

                    await self.step4_select_names_and_display(search_name)

                    await self.page.goto(self.realestate_url, wait_until="domcontentloaded")
                    await self.check_and_handle_announcement()
                    await self.page.wait_for_timeout(self.time_sleep())
                
                except Exception as e:
                    console.print(f"[red]Error processing row {idx+1}: {e}[/red]")
                    continue

        except Exception as e:
            console.print(f"[red]Error in step3_fill_form_from_excel: {e}[/red]")
            raise

    async def step4_select_names_and_display(self, search_name: str):
        try:
            radios = await self.page.query_selector_all("input[name='rdoEntityName']")
            console.print(f"[cyan]Found {len(radios)} entity names for {search_name}[/cyan]")

            for i in range(len(radios)):
                try:
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

                    # Step 5: Parse documents for this entity
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
            console.print(f"[cyan]Found {len(links)} GE/GR document links[/cyan]")

            if not links:
                console.print(f"[yellow]No document links found for entity {entity_idx}[/yellow]")
                return

            hrefs = []
            for link in links:
                try:
                    href = await link.get_attribute("href")
                    if href:
                        if "fnSubmitThisForm" in href:
                            inner = href.split("fnSubmitThisForm('")[1].split("')")[0]
                            pdf_url = f"https://search.gsccca.org/RealEstate/{inner}"
                        else:
                            pdf_url = href
                        hrefs.append(pdf_url)
                except Exception as e:
                    console.print(f"[red]Error extracting href from link: {e}[/red]")
                    continue

            for i, pdf_url in enumerate(hrefs):
                try:
                    console.print(f"[green]Opening doc {i + 1}: {pdf_url}[/green]")

                    await self.page.goto(pdf_url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(self.time_sleep())

                    # --- Try opening popup ---
                    try:
                        async with self.page.context.expect_page(timeout=5000) as popup_info:
                            view_button = await self.page.query_selector("input[value='View Image']")
                            if view_button:
                                await view_button.click()
                            else:
                                console.print("[yellow]'View Image' button not found[/yellow]")
                        popup = await popup_info.value
                        await popup.wait_for_load_state("domcontentloaded")
                        console.print(f"[green]Popup opened for entity {entity_idx}[/green]")
                    except Exception:
                        console.print(f"[yellow]No popup opened, using same page for entity {entity_idx}[/yellow]")
                        popup = self.page

                    # --- Directory for saving PDFs ---
                    pdf_dir = Path("realestate_documents")
                    pdf_dir.mkdir(exist_ok=True)

                    # --- Collect thumbnails ---
                    thumb_links = await popup.query_selector_all("a[id*='lvThumbnails_lnkThumbnail']")
                    console.print(f"[cyan]Found {len(thumb_links)} thumbnails in viewer[/cyan]")

                    for j, thumb_link in enumerate(thumb_links):
                        try:
                            await thumb_link.click()
                            await popup.wait_for_timeout(2000)  # allow render

                            # --- Extract Book & Page Number ---
                            try:
                                header_text = await popup.inner_text("#lblHeader")
                                match = re.search(r"Book\s+(\d+)\s+Page\s+(\d+)", header_text)
                                if match:
                                    book_no = match.group(1)
                                    page_no = match.group(2)
                                    safe_title = f"RE_Book_{book_no}_Page_{page_no}"
                                else:
                                    safe_title = f"Entity_{entity_idx}_Doc_{j+1}"
                            except Exception:
                                safe_title = f"Entity_{entity_idx}_Doc_{j+1}"

                            # --- Screenshot canvas only ---
                            try:
                                canvas = await popup.query_selector("canvas")
                                if canvas:
                                    screenshot_path = pdf_dir / f"{safe_title}.png"
                                    await canvas.screenshot(path=str(screenshot_path))

                                    # --- Convert PNG → PDF ---
                                    pdf_path = pdf_dir / f"{safe_title}.pdf"
                                    with open(pdf_path, "wb") as f:
                                        f.write(img2pdf.convert(str(screenshot_path)))
                                    # ✅ Delete PNG after converting
                                    if screenshot_path.exists():
                                        screenshot_path.unlink()

                                    console.print(f"[blue]Saved PDF: {pdf_path}[/blue]")
                                    
                                    # Save result row
                                    self.results.append({
                                        "Search Name": search_name,
                                        "Entity_index": entity_idx,
                                        "DOC_index": j + 1,
                                        "PDF_viewer": popup.url,
                                        "RealEstate_PDF": str(pdf_path)
                                    })
                                else:
                                    console.print("[yellow]Canvas not found for screenshot[/yellow]")

                            except Exception as e:
                                console.print(f"[red]Error saving PDF: {e}[/red]")

                        except Exception as e:
                            console.print(f"[red]Error processing thumbnail {j+1}: {e}[/red]")
                            continue

                    # --- Close popup automatically ---
                    if popup != self.page:
                        try:
                            await popup.close()
                            await asyncio.sleep(1)  # wait before next
                            console.print(f"[red]Popup closed for entity {entity_idx}[/red]")
                        except Exception as e:
                            console.print(f"[red]Error closing popup: {e}[/red]")
                
                except Exception as e:
                    console.print(f"[red]Error processing document {i+1}: {e}[/red]")
                    continue

        except Exception as e:
            console.print(f"[bold red]Fatal error in step5_parse_documents: {e}[/bold red]")
            traceback.print_exc()


    def save_results_to_excel(self, filename="realestate_index.xlsx"):
        if not self.results:
            console.print("[red]No results to save[/red]")
            return None

        try:
            df = pd.DataFrame(self.results)
            df.drop_duplicates(subset=["PDF_viewer"], inplace=True)
            df.reset_index(drop=True, inplace=True)

            output_dir = Path("RealEstate_Excel")
            output_dir.mkdir(exist_ok=True)

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_filename = f"realestate_index_{ts}.xlsx"
            final_path = output_dir / final_filename

            df.to_excel(final_path, index=False)
            console.print(f"[green]Results saved -> {final_path}[/green]")
            return final_path
        except Exception as e:
            console.print(f"[red]Failed to save Excel: {e}[/red]")
            traceback.print_exc()
            return None

    async def run(self):
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
                )

            self.page = await context.new_page()
            if not STATE_FILE.exists() or not await self.page.query_selector("a:has-text('Logout')"):
                await self.login()

            await self.step2_open_realestate_search()
            await self.step3_fill_form_from_excel()

            await browser.close()
            await playwright.stop()
            
        except Exception as e:
            console.print(f"[red]Error in run method: {e}[/red]")
            traceback.print_exc()

async def main():
    scraper = RealestateIndexScraper()
    try:
        await scraper.run()
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Interrupted by user![/bold yellow]\n")
    except Exception as e:
        console.print(f"\n[bold red]Unexpected error: {e}[/bold red]\n")
        traceback.print_exc()
    finally:
        try:
            scraper.save_results_to_excel()
        except Exception as e:
            console.print(f"[red]Error saving final results: {e}[/red]")
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