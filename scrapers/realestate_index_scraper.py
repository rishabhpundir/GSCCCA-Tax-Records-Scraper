from __future__ import annotations

import re
import os
import json
import random
import img2pdf
import traceback
from pathlib import Path
from datetime import datetime

import pandas as pd
from PIL import Image
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw

from dashboard.utils.state import stop_scraper_flag 

load_dotenv()
console = Console()

# ---------- Config -------------------------------------------------------------
HEADLESS = True if os.getenv("HEADLESS", "False").lower() in ("true", "yes") else False
STATE_FILE = Path("cookies.json")
TAX_EMAIL = os.getenv("GSCCCA_USERNAME")
TAX_PASSWORD = os.getenv("GSCCCA_PASSWORD")
LOCALE = "en-GB"
TIMEZONE = "UTC"
VIEWPORT = {"width": 1366, "height": 900}
UA_DICT = {
    "macos": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "linux": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "windows": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome"
}

UA = UA_DICT.get(os.getenv("OS_NAME"), "windows")

EXTRA_HEADERS = {"Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"}

# ----------- Directories -----------
BASE_DIR = Path(__file__).parent.absolute() 
BASE_OUTPUT_DIR = os.path.join(BASE_DIR.parent, "output")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

REAL_ESTATE_EXCEL_DIR = os.path.join(BASE_OUTPUT_DIR, "real_estate")
PDF_DIR = os.path.join(REAL_ESTATE_EXCEL_DIR, "documents")

os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(REAL_ESTATE_EXCEL_DIR, exist_ok=True)


# ---------- Utility Function to find latest Excel file -------------------------
def images_to_pdf(img_paths, pdf_path):
    try:
        valid_images = []
        for path in img_paths:
            try:
                with Image.open(path) as im:
                    im.verify()
                valid_images.append(path)
            except Exception as e:
                console.print(f"[yellow]Skipping invalid image {path}: {e}[/yellow]")

        if not valid_images:
            console.print("[red]No valid images to convert.[/red]")
            return False

        # Convert the list of valid image paths to a single PDF
        pdf_bytes = img2pdf.convert(valid_images)
        with open(pdf_path, "wb") as f:
            f.write(pdf_bytes)

        console.print(f"[green]Successfully created PDF: {pdf_path}[/green]")
        return True

    except Exception as e:
        console.print(f"[red]Failed to create PDF: {e}[/red]")
        return False

# ---------- Scraper Class -----------------------------------------------------
class RealEstateIndexScraper:
    def __init__(self, form_data: dict) -> None:
        try:
            self.playwright = None
            self.browser = None
            self.page = None
            self.email = TAX_EMAIL
            self.password = TAX_PASSWORD
            self.homepage_url = "https://www.gsccca.org/"
            self.login_url = "https://apps.gsccca.org/login.asp"
            self.realestate_search_url = "https://search.gsccca.org/RealEstate/namesearch.asp"
            self.results = []
            self.form_data = form_data
            
            # Use global constants defined above
            self.pdf_dir = PDF_DIR
            self.excel_output_dir = REAL_ESTATE_EXCEL_DIR
            console.print(f"[green]Real Estate Data Output directory --> {self.excel_output_dir}[/green]")
        except Exception as e:
            console.print(f"[red]Error initializing RealEstateIndexScraper: {e}[/red]")
            raise


    def time_sleep(self, a: int = 2500, b: int = 5000) -> int:
        return random.uniform(a, b)
    

    async def stop_check(self):
        """ Global stop flag to immediately exit scraping if invoked by user. """
        if stop_scraper_flag['lien']:
            console.print("[yellow]Lien Index Scraper received immediate stop signal. Exiting...[/yellow]")
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            return


    async def dump_cookies(self, out_file="cookies.json"):
        """Save cookies + storage ONLY for login check."""
        try:
            state = await self.page.context.storage_state()
            Path(out_file).write_text(json.dumps(state, indent=2))
            print(f"Saved login state to --> {out_file}")
        except Exception as e:
            console.print(f"[red]Failed to dump cookies: {e}[/red]")


    async def already_logged_in(self) -> bool:
        """Check if user is already logged in."""
        try:
            await self.page.wait_for_timeout(self.time_sleep())
            all_text = await self.page.evaluate("document.body.innerText")
            if "logout" in all_text.lower():
                return True
            return False

        except Exception as e:
            print(f"[already_logged_in ERROR] {e}")
            return False

    
    async def check_session(self):
        """Check on homepage if user is logged in."""
        try:
            if await self.already_logged_in():
                console.print("[green]Logged in session detected!![/green]")
                return True
            else:
                console.print("[red]Session not found![/red]")
                return False
        except Exception as e:
            console.print(f"[red]Error in check_session: {e}[/red]")
            return False


    async def check_and_handle_announcement(self):
        """Check if announcement page loaded; if yes, redirect to realestate_search_url."""
        try:
            current_url = self.page.url
            if "Announcement" in current_url:
                await self.page.select_option("#Options", "dismiss")
                await self.page.wait_for_timeout(1000)
                await self.page.click("input[name='Continue']")
                print("Announcement page detected. Turning off...")
        except Exception as e:
            console.print(f"[red]Error handling announcement: {e}[/red]")
            

    async def login(self):
        """Perform login and save cookies."""
        try:
            await self.page.goto(self.login_url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()

            await self.page.fill("input[name='txtUserID']", self.email)
            await self.page.wait_for_timeout(self.time_sleep()) 
            await self.page.fill("input[name='txtPassword']", self.password)
            await self.page.wait_for_timeout(self.time_sleep())
            checkbox = await self.page.query_selector("input[type='checkbox'][name='permanent']")
            await self.page.wait_for_timeout(self.time_sleep(a=1000, b=1200))
            
            if checkbox:
                is_checked = await checkbox.is_checked()
                if not is_checked:
                    await checkbox.click()
            else:
                print("[LOGIN] Checkbox not found on the page.")

            try:
                await self.page.click("img[name='logon']")
            except Exception as e:
                print(f"[LOGIN] Login button Click failed: {e}, using JS submit...")
                await self.page.evaluate("document.forms['frmLogin'].submit()")

            await self.page.wait_for_load_state("networkidle", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()
            
            await self.page.goto(self.realestate_search_url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()
            
            if await self.already_logged_in():
                console.print("[bold green]Login successful![/bold green]")
                await self.dump_cookies()
                return True
            else:
                print("[LOGIN] Login failed!")
                return False
        except Exception as e:
            console.print(f"[red]Error during login: {e}[/red]")
            return False
    
    
    async def start_realestate_search(self):
        """Fill the real estate form using the provided parameters."""
        try:
            print("Conducting search...")
            await self.page.goto(self.realestate_search_url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()
            
            await self.page.wait_for_selector("input[name='txtSearchName']")
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='txtPartyType']", self.form_data.get("txtPartyType", "2"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='txtInstrCode']", self.form_data.get("txtInstrCode", "ALL"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='intCountyID']", self.form_data.get("intCountyID", "-1"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            
            include_val = self.form_data.get("bolInclude", "0")
            checkbox_selector = f"input[name='bolInclude'][value='{include_val}']"
            if await self.page.query_selector(checkbox_selector):
                await self.page.check(checkbox_selector)
            
            await self.page.fill("input[name='txtSearchName']", self.form_data.get("txtSearchName", ""))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.fill("input[name='txtFromDate']", self.form_data.get("txtFromDate", ""))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.fill("input[name='txtToDate']", self.form_data.get("txtToDate", ""))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            
            await self.page.select_option("select[name='MaxRows']", self.form_data.get("MaxRows", "100"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='TableType']", self.form_data.get("TableType", "1"))
            
            await self.page.wait_for_timeout(self.time_sleep())
            await self.page.click("#btnSubmit")
        except Exception as e:
            console.print(f"[red]Error filling real estate form: {e}[/red]\n{traceback.format_exc()}")
            raise


    async def get_search_results(self):
        try:
            await self.page.wait_for_timeout(self.time_sleep(a=4000, b=5000))
            search_name = self.form_data.get("txtSearchName")
            radios = await self.page.query_selector_all("input[name='rdoEntityName']")
            print("*" * 50)
            console.print(f"[cyan]Found {len(radios)} result(s) for '{search_name}'[/cyan]")

            for i in range(len(radios)):
                await self.stop_check()
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
                    await self.page.wait_for_timeout(self.time_sleep())

                    # Parse documents
                    await self.parse_documents(search_name, i+1)

                    # Go back to entity selection
                    await self.page.go_back()
                    await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    await self.page.wait_for_timeout(self.time_sleep())
                
                except Exception as e:
                    console.print(f"[red]Error processing entity {i+1}: {e}[/red]")
                    continue

        except Exception as e:
            console.print(f"[red]Step 4 error: {e}[/red]")


    async def parse_documents(self, search_name: str, entity_idx: int):
        try:
            links = await self.page.query_selector_all("a[href*='final.asp']")
            console.print(f"Found {len(links)} GE/GR document links for entity {entity_idx}")

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
                    await self.stop_check()
                    print("-" * 30)
                    console.print(f"[green]{i + 1}. Opening doc for entity #{entity_idx}[/green]")
                    await self.page.goto(pdf_url, wait_until="domcontentloaded", timeout=30000)
                    await self.page.wait_for_timeout(self.time_sleep())
                    await self.check_and_handle_announcement()

                    # ---------- PDF Extraction ----------
                    html = await self.page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    viewer_script = soup.find("script", string=lambda t: t and "ViewImage" in t)
                    if viewer_script:
                        script_text = viewer_script.string
                        match = re.search(r'var iREID\s*=\s*(\d+);', script_text)
                        if match:
                            reid = match.group(1)
                            county = re.search(r'var county\s*=\s*"(\d+)"', script_text).group(1)
                            book = re.search(r'var book\s*=\s*"(\d+)"', script_text).group(1)
                            page_num = re.search(r'var page\s*=\s*"(\d+)"', script_text).group(1)
                            userid = re.search(r'var user\s*=\s*(\d+)', script_text).group(1)
                            appid = re.search(r'var appid\s*=\s*(\d+)', script_text).group(1)

                            viewer_url = (
                                f"https://search.gsccca.org/Imaging/HTML5Viewer.aspx?" \
                                f"id={reid}&key1={book}&key2={page_num}&county={county}&userid={userid}&appid={appid}"
                            )

                            popup = await self.page.context.new_page()
                            await popup.goto(viewer_url, wait_until="domcontentloaded", timeout=50000)
                            await popup.wait_for_timeout(6000)

                    # --- Collect thumbnails ---
                    thumb_links = await popup.query_selector_all("a[id*='lvThumbnails_lnkThumbnail']")
                    console.print(f"Found {len(thumb_links)} page(s) in viewer...")

                    screenshot_paths = []
                    for j, thumb_link in enumerate(thumb_links):
                        await self.stop_check()
                        
                        # Select "Fit Window" option
                        await popup.wait_for_selector("td.vtm_zoomSelectCell select", timeout=10000)
                        await popup.select_option("td.vtm_zoomSelectCell select", "fitwindow")
                        await popup.wait_for_timeout(2000)

                        await popup.wait_for_selector("div.vtm_imageClipper canvas", timeout=10000, state="attached")
                        await popup.wait_for_timeout(2000)
                        canvas = await popup.query_selector("canvas")
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
                                if canvas:
                                    screenshot_path = Path(os.path.join(self.pdf_dir, f"{safe_title}.png"))
                                    await canvas.screenshot(path=str(screenshot_path), timeout=30000)
                                    screenshot_paths.append(screenshot_path)
                                    await popup.wait_for_timeout(2000)
                                else:
                                    console.print("[yellow]Canvas not found for screenshot[/yellow]")
                            except Exception as e:
                                console.print(f"[red]Error saving PDF for thumbnail {j+1}: {e}[/red]{traceback.format_exc()}")

                        except Exception as e:
                            console.print(f"[red]Error processing thumbnail {j+1}: {e}[/red]")
                            continue

                    # Save screenshots to PDF
                    pdf_path = Path(os.path.join(self.pdf_dir, f"{safe_title}.pdf"))
                    images_to_pdf(screenshot_paths, pdf_path)
                    for path in screenshot_paths:
                        try:
                            os.remove(path)
                        except FileNotFoundError:
                            console.print(f"[yellow]Already missing:[/yellow] {path}")
                            
                    # Save result to self.results list
                    result_data = {
                        "Search Name": search_name,
                        "Entity Index": entity_idx,
                        "Doc Index": i + 1,
                        "Page Index": j + 1,
                        "PDF Viewer URL": popup.url,
                        "Real Estate PDF": str(pdf_path)
                    }
                    
                    self.results.append(result_data)

                    # --- Close popup automatically ---
                    if popup and popup != self.page:
                        await popup.close()
                        await self.page.wait_for_timeout(self.time_sleep())
                        console.print("-" * 30)
                
                except Exception as e:
                    console.print(f"[red]Error processing document {i+1}: {e}[/red]")
                    traceback.print_exc()
                    continue

        except Exception as e:
            console.print(f"[bold red]Fatal error in parse_documents: {e}[/bold red]")
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

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_filename = f"{filename_prefix}_{ts}.xlsx"
            final_path = os.path.join(self.excel_output_dir, final_filename)

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

            print("-" * 50)
            console.print(f"[green]Real Estate Excel saved -> {final_path}[/green]")
            file_size = Path(final_path).stat().st_size / 1024
            console.print(f"[blue]File size: {file_size:.2f} KB[/blue]")
            console.print(f"[blue]Total records saved: {len(df)}[/blue]")
            print("-" * 50)
            
            return final_path
            
        except Exception as e:
            console.print(f"[red]Failed to save Excel: {e}[/red]")
            traceback.print_exc()
            return None


    async def scrape(self):
        try:
            self.playwright = await pw.async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=HEADLESS,
                channel="chrome",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--start-maximized",
                    "--no-proxy-server",
                ]
            )

            print("Starting Real Estate Index Scraper...")
            context = await self.browser.new_context(
                storage_state=Path(STATE_FILE) if Path(STATE_FILE).exists() else None,
                user_agent=UA,
                locale=LOCALE,
                timezone_id=TIMEZONE,
                viewport=VIEWPORT,
                device_scale_factor=1,
                extra_http_headers=EXTRA_HEADERS,
                bypass_csp=True,
                ignore_https_errors=False, 
            )
            self.page = await context.new_page()
            self.context = context

            # login if needed
            await self.page.goto("https://google.com", wait_until="domcontentloaded")
            await self.page.wait_for_timeout(self.time_sleep())
            if STATE_FILE.exists():
                # await context.add_cookies(json.loads(Path(STATE_FILE).read_text())["cookies"])
                await self.page.goto(self.homepage_url, wait_until="domcontentloaded", timeout=60000)
                await self.check_and_handle_announcement()
            else:
                await self.page.goto(self.login_url, wait_until="domcontentloaded")
                await self.check_and_handle_announcement()
                if not await self.check_session():
                    print("Attempting fresh login...")
                    await self.login()
                    await self.page.wait_for_timeout(self.time_sleep())
                        
            await self.stop_check()
            if not await self.check_session():
                console.print("[yellow]Session invalid... logging in again...[/yellow]")
                await self.login()
                await self.page.wait_for_timeout(self.time_sleep())

            # Start real estate index search
            await self.start_realestate_search()

            await self.get_search_results()

        except Exception as e:
            console.print(f"[red]Error in scrape method: {e}[/red]")
            traceback.print_exc()
        finally:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()

