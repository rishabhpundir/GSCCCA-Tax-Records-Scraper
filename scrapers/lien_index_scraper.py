from __future__ import annotations

import os
import re
import html
import json
import random
import traceback
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin

import cv2
import img2pdf
import numpy as np
import pytesseract
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PIL import Image, ImageOps
from rich.console import Console
import playwright.async_api as pw

from dashboard.utils.state import stop_scraper_flag 

# Load environment variables
load_dotenv()
console = Console()

# ---------- config -------------------------------------------------------------
HEADLESS = True if os.getenv("HEADLESS", "False").lower() in ("true", "yes") else False
WIDTH, HEIGHT = os.getenv("RES", "1366x900").split("x")
STATE_FILE = Path("cookies.json")
TAX_EMAIL = os.getenv("GSCCCA_USERNAME")
TAX_PASSWORD = os.getenv("GSCCCA_PASSWORD")
LOCALE = "en-GB"
TIMEZONE = "UTC"
VIEWPORT = {"width": int(WIDTH), "height": int(HEIGHT)}
UA_DICT = {
    "macos": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "linux": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "windows": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome"
}

UA = UA_DICT.get(os.getenv("OS_NAME"), "windows")

EXTRA_HEADERS = {
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
}
TOTAL_LINE_REGEX = re.compile(r'(TOTAL\s*DUE|TOTALDUE)', re.I)
AMOUNT_PATTERN = re.compile(
    r'\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)'
)


# load Tesseract path for Windows if needed
try:
    if os.name == "nt":  # Windows
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
except Exception as e:
    console.print(f"[red]Error setting up Tesseract: {e}[/red]")


# ---------- core scraping ----------------------------------------------------
class LienIndexScraper:
    """Scrape the latest tax records from GSCCCA pages."""

    def __init__(self) -> None:
        try:
            self.playwright = None
            self.browser = None
            self.page = None
            self.email = TAX_EMAIL
            self.password = TAX_PASSWORD
            self.form_data = {}
            self.csv_path = ""
            self.county_folder = ""
            self.homepage = "https://www.gsccca.org/"
            self.login_url = "https://apps.gsccca.org/login.asp"
            self.name_search_url = "https://search.gsccca.org/Lien/namesearch.asp"
            self.results = []
            
            script_dir = Path(__file__).parent.absolute()
            self.county_folder_path = ""
            self.base_output_dir = os.path.join(script_dir.parent, "output") 
            self.lien_output_dir = os.path.join(self.base_output_dir, "lien")
            os.makedirs(self.lien_output_dir, exist_ok=True)
            
            console.print(f"[green]Lien Output directory --> {self.lien_output_dir}[/green]")
        except Exception as e:
            console.print(f"[red]Error initializing LienIndexScraper: {e}[/red]")
            raise
    

    def time_sleep(self, a: int = 3000, b: int = 5000) -> int:
        return random.uniform(a, b)
    
    
    def extract_amount(self, desc: str):
        """Return amount as float, or None if not found."""
        if not isinstance(desc, str):
            return None

        m = AMOUNT_PATTERN.search(desc)
        if not m:
            return ""

        raw = m.group(1)
        raw = raw.replace(',', '')
        return str(raw)
        
        
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
        """Check if announcement page loaded; if yes, redirect to name_search_url."""
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
            
            await self.page.goto(self.name_search_url, wait_until="domcontentloaded", timeout=60000)
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


    async def start_search(self):
        """Name Search page."""
        try:
            print("Executing Name Search...")
            await self.page.goto(self.name_search_url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()
        except Exception as e:
            console.print(f"[red]Error in start_search: {e}[/red]")

        try:
            await self.page.select_option("#txtPartyType", self.form_data.get("party_type"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='txtInstrCode']", self.form_data.get("instrument_type"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='intCountyID']", self.form_data.get("county"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))

            include_val = self.form_data.get("include_counties")
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            checkbox_selector = f"input[name='bolInclude'][value='{include_val}']"
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            if await self.page.query_selector(checkbox_selector):
                await self.page.check(checkbox_selector)
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))

            await self.page.fill("input[name='txtSearchName']", self.form_data.get("search_name"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.fill("input[name='txtFromDate']", self.form_data.get("from_date"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.fill("input[name='txtToDate']", self.form_data.get("to_date"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='MaxRows']", self.form_data.get("max_rows", "100"))
            await self.page.wait_for_timeout(self.time_sleep(a=250, b=500))
            await self.page.select_option("select[name='TableType']", self.form_data.get("table_type", "1"))

            await self.page.wait_for_timeout(self.time_sleep())
            await self.page.locator('input[type="button"][value="Search"]').click()
        except Exception as e:
            print(f"[ERROR] step3_fill_form_dynamic: {e}")


    async def get_search_results(self, email: str = None, password: str = None):
        """Process ALL rows with Occurs values."""
        print(f"Conducting Lien Search...")
        try:
            await self.page.wait_for_selector("table.name_results", state="visible", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            
            name_strongs = await self.page.locator(
                "//td[normalize-space()='Name Searched:']/following-sibling::td//strong"
            ).all_inner_texts()

            searched_strongs = await self.page.locator(
                "//td[normalize-space()='Searched:']/following-sibling::td//strong"
            ).all_inner_texts()

            all_values = searched_strongs + name_strongs
            self.county_folder = "_".join([t.strip().lower().replace(" ", "_") for t in all_values])
            
            self.county_folder_path = os.path.join(self.lien_output_dir, self.county_folder)
            self.documents_dir = os.path.join(self.county_folder_path, "documents") 
            os.makedirs(self.county_folder_path, exist_ok=True)
            os.makedirs(self.documents_dir, exist_ok=True)
            
            # Get total number of rows initially
            rows = await self.page.query_selector_all("table.name_results tr")
            total_rows = len(rows) - 1  # Exclude header
            
            print(f"Found {total_rows} rows to process...")

            # to collect all results URLs
            results_df = pd.DataFrame(columns=['urls'])
            
            for row_index in range(total_rows):
                await self.stop_check()
                print("*" * 50)
                print(f"Exracting URLs from Row {row_index + 1} of {total_rows}")
                    
                try:
                    await self.page.wait_for_selector("table.name_results", timeout=15000)
                    rows = await self.page.query_selector_all("table.name_results tr")
                    
                    if row_index + 1 >= len(rows):
                        print(f"[WARNING] Row index {row_index + 1} not found, skipping")
                        continue
                        
                    current_row = rows[row_index + 1]  # Skip header
                    cols = await current_row.query_selector_all("td")

                    # Get Occurs value and radio button
                    occurs_text = await cols[1].inner_text()
                    radio = await cols[0].query_selector("input[type='radio']")
                    
                    try:
                        if not radio:
                            print(f"[WARNING] No radio button found for row {row_index + 1}, skipping")
                            continue
                            
                        # Click the radio button with retry
                        retries = 3
                        for attempt in range(retries):
                            try:
                                await radio.click()
                                await self.page.wait_for_timeout(500)
                                break
                            except Exception as click_error:
                                if attempt == retries - 1:
                                    raise click_error
                                print(f"[RETRY] Radio click failed, attempt {attempt + 1}/{retries}")
                                await self.page.wait_for_timeout(1000)
                        
                        # Click "Display Details"
                        display_btn = await self.page.query_selector("input[value='Display Details']")
                        if not display_btn:
                            print(f"[ERROR] 'Display Details' button not found for row {row_index + 1}")
                            continue
                            
                        await display_btn.click()
                        next_page_found = True
                        next_page = 1
                        while next_page_found:
                            print(f"Extracting Page {next_page} results...")
                            await self.page.wait_for_selector('a[href^="javascript:fnSubmitThisForm("]', timeout=15000)
                            await self.page.wait_for_timeout(self.time_sleep())
                            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await self.page.wait_for_timeout(self.time_sleep())

                            # grab all matching hrefs in one shot (fast)
                            hrefs = await self.page.eval_on_selector_all(
                                'a[href^="javascript:fnSubmitThisForm("]',
                                'els => els.map(e => e.getAttribute("href"))'
                            )

                            # extract the relative path inside the JS call and build full URLs
                            pattern = re.compile(r"fnSubmitThisForm\('([^']+)'\)")
                            base = "https://search.gsccca.org/Lien/"
                            urls = []

                            for h in hrefs:
                                if not h:
                                    continue
                                m = pattern.search(h)
                                if not m:
                                    continue
                                rel = html.unescape(m.group(1))
                                full = urljoin(base, rel)
                                urls.append(full)

                            # append to your DataFrame
                            if urls:
                                results_df = pd.concat([results_df, pd.DataFrame({'urls': urls})], ignore_index=True)
                            results_url = results_df
                            # if len(results_url) >= 20:
                            #     break
                            
                            next_selectors = [
                                "a[href*='liennamesselected.asp?page=']:has-text('Next Page')",
                                "font a[href*='liennamesselected.asp?page=']",
                                "a:has-text('Next Page')",
                                "font:has-text('Next Page') a",
                                "a:has-text('Next')"
                            ]

                            for selector in next_selectors:
                                next_page_link = await self.page.query_selector(selector)

                            if next_page_link:
                                next_page += 1
                                
                                # Get the href for recovery
                                next_href = await next_page_link.get_attribute("href")
                                if next_href:
                                    await next_page_link.click()     
                                    next_page_found = True      
                            else:
                                next_page_found = False             
                        
                        # if len(results_url) >= 20:
                        #     break
                        
                        back_success = False
                        for i in range(next_page):
                            await self.page.wait_for_timeout(self.time_sleep())
                            back_button = await self.page.query_selector("input[name='bBack']")
                            if back_button:
                                try:
                                    await back_button.click()
                                    await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
                                    back_success = True
                                except Exception as e:
                                    print(f"[WARNING] bBack button failed: {e}")
                        
                        # Fallback: Go to name search page
                        if not back_success:
                            try:
                                await self.page.goto(self.name_search_url, wait_until="domcontentloaded", timeout=30000)
                                await self.check_and_handle_announcement()
                                # Refill the form and search again
                                await self.start_search()
                                await self.page.wait_for_selector("table.name_results", timeout=15000)
                                # Update the search results URL
                                search_results_url = self.page.url
                                print(f"[SUCCESS] Recovered by going to name search page and re-searching")
                            except Exception as e:
                                print(f"[ERROR] Final recovery failed: {e}")
                                break
                        
                        # Verify we're back on search results page
                        try:
                            await self.page.wait_for_selector("table.name_results", timeout=10000)
                        except Exception as timeout_error:
                            print(f"[WARNING] Table reload timeout, but continuing...")
                            
                    except ValueError:
                        print(f"[WARNING] Invalid Occurs value: {occurs_text}, skipping\n", traceback.format_exc())
                        continue
                        
                except Exception as e:
                    print(f"[ERROR] Failed to process row {row_index + 1}: {e}\n{traceback.format_exc()}")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_url = results_df[~results_df['urls'].str.contains('maxrows', case=False, na=False)]
            search_name = re.sub(r'[^a-zA-Z0-9]', '', self.form_data.get("search_name", "")).replace(" ", "_")
            self.csv_path = os.path.join(self.county_folder_path, f"{search_name}_urls_list_{timestamp}.csv")
            results_url.to_csv(self.csv_path, index=False)
            print(f"Success -> Search results' URLs saved to CSV at {self.csv_path}")

        except Exception as e:
            console.print(f"[red]Error in get_search_results: {e}[/red]")
            traceback.format_exc()
            

    async def process_result_urls(self):
        """Step 5: Process all RP buttons, extract data and save with improved reliability"""
        result_urls = pd.read_csv(self.csv_path)
        if result_urls.empty:
            console.print(f"[red][ERROR] No URLs found at: {self.csv_path}[/red]")
            return

        urls = result_urls['urls'].tolist()
        print(f"Initiating lien data extraction...\nTotal URLs count: {len(urls)}")

        try:
            for index, url in enumerate(urls, 1):
                # if index == 20:
                #     break
                await self.stop_check()
                print("-" * 50)
                print(f"{index}. URL: ", url)
                
                await self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
                if await self.page.locator("body:has-text('CANCELLATION')").count() > 0:
                    print(f"⚠️ 'CANCELLATION' found on page. Skipping: {url}")
                    continue
                await self.check_and_handle_announcement()
                await self.page.wait_for_timeout(self.time_sleep())

                # Parse data
                data = await self.parse_lien_data()
                if data:
                    self.results.append(data)
                    console.print(f"[cyan]Saved data for --> {data.get('direct_party_debtor', 'Unknown')}[/cyan]")
                else:
                    print(f"No data found")

        except Exception as e:
            console.print(f"[red]Error in process_result_urls: {e}[/red]")
            traceback.format_exc()


    async def parse_lien_data(self):
        """ Helper: Parse lien detail page """
        await self.stop_check()
        try:
            await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
            await self.page.wait_for_timeout(self.time_sleep())
            html = await self.page.content()
            soup = BeautifulSoup(html, "html.parser")
            data = {}

            def safe_text(el):
                return el.get_text(" ", strip=True) if el else ""

            # ---------- Data Extraction ----------
            doc_table = soup.find("table", width="800", cellpadding="0", cellspacing="0")
            if doc_table:
                rows = doc_table.find_all("tr")[1:]
                if rows:
                    cols = [safe_text(td) for td in rows[0].find_all("td")]
                    if len(cols) >= 6:
                        data.update({
                            "county": cols[0],
                            "instrument": cols[1],
                            "date_filed": cols[2],
                            "time": cols[3],
                            "book": cols[4],
                            "page": cols[5],
                        })

            desc_table = soup.find("td", string=lambda t: t and "Description" in t)
            if desc_table:
                tbody = desc_table.find_parent("table")
                desc_val = safe_text(tbody.find_all("tr")[1].find("td"))
                data["description"] = desc_val
                data["amount"] = self.extract_amount(desc_val)

            debtor_table = soup.find("td", string=lambda t: t and "Direct Party (Debtor)" in t)
            if debtor_table:
                tbody = debtor_table.find_parent("table")
                debtors = [safe_text(td) for td in tbody.find_all("td")[1:]]
                data["direct_party_debtor"] = "; ".join(debtors)

            claimant_table = soup.find("td", string=lambda t: t and "Reverse Party (Claimant)" in t)
            if claimant_table:
                tbody = claimant_table.find_parent("table")
                claimants = [safe_text(td) for td in tbody.find_all("td")[1:]]
                data["reverse_party_claimant"] = "; ".join(claimants)

            # ---------- PDF Extraction ----------
            viewer_script = soup.find("script", string=lambda t: t and "ViewImage" in t)
            if viewer_script:
                script_text = viewer_script.string
                match = re.search(r'var iLienID\s*=\s*(\d+);', script_text)
                if match:
                    lien_id = match.group(1)
                    county = re.search(r'var county\s*=\s*"(\d+)"', script_text).group(1)
                    book = re.search(r'var book\s*=\s*"(\d+)"', script_text).group(1)
                    page_num = re.search(r'var page\s*=\s*"(\d+)"', script_text).group(1)
                    userid = re.search(r'var user\s*=\s*(\d+)', script_text).group(1)
                    appid = re.search(r'var appid\s*=\s*(\d+)', script_text).group(1)

                    viewer_url = (
                        f"https://search.gsccca.org/Imaging/HTML5Viewer.aspx?" \
                        f"id={lien_id}&key1={book}&key2={page_num}&county={county}&userid={userid}&appid={appid}"
                    )
                    data["pdf_document_url"] = viewer_url
                    debtor_name = data.get("direct_party_debtor", "unkown_debtor").split(";")[0][:40]
                    debtor_name = debtor_name.replace(" ", "_").replace(",", "").strip()
                    pdf_name = f"{debtor_name}_page_{page_num}_{county}.pdf"
                    pdf_path = os.path.join(self.documents_dir, pdf_name)

                    try:
                        popup = await self.page.context.new_page()
                        await popup.goto(viewer_url, wait_until="domcontentloaded", timeout=50000)
                        await self.page.wait_for_timeout(5000)

                        # Select "Fit Window" option
                        await popup.wait_for_selector("td.vtm_zoomSelectCell select", timeout=10000)
                        await popup.select_option("td.vtm_zoomSelectCell select", "fitwindow")
                        await self.page.wait_for_timeout(2000)
                        await popup.locator('img[title="Rotate Right"]').click()

                        await popup.wait_for_selector("div.vtm_imageClipper canvas", timeout=10000, state="attached")
                        await self.page.wait_for_timeout(2000)
                        canvas = await popup.query_selector("div.vtm_imageClipper canvas")

                        if canvas:
                            tmp_img = os.path.join(self.documents_dir, f"tmp_{page_num}.png")
                            try:
                                await canvas.screenshot(path=tmp_img, timeout=30000)
                                if not (os.path.exists(tmp_img) and os.path.getsize(tmp_img) > 0):
                                    print("[WARNING] screenshot missing, trying full page screenshot...")
                                    await popup.screenshot(path=tmp_img, full_page=True, timeout=30000)
                                    print(f"Full page screenshot saved!")
                            except Exception as screenshot_error:
                                console.print(f"[red][ERROR] Canvas screenshot failed: {screenshot_error}" \
                                    "\nTrying full page screenshot...[/red]")
                                await popup.screenshot(path=tmp_img, full_page=True, timeout=30000)
                                print(f"Full page screenshot saved!")

                            # Rotate CCW once and overwrite
                            with Image.open(tmp_img) as im:
                                im = ImageOps.exif_transpose(im)
                                im = im.rotate(90, expand=True)
                                im.save(tmp_img)

                            # now convert the rotated image to PDF
                            with open(pdf_path, "wb") as f:
                                f.write(img2pdf.convert([tmp_img]))

                            data["pdf_filename"] = pdf_name
                            print(f"PDF document saved to --> {pdf_path}")

                            # ----------- OCR Extraction + Address1/2 -----------
                            try:
                                img = Image.open(tmp_img).convert("L")
                                text = pytesseract.image_to_string(img, lang="eng")
                                data["ocr_raw_text"] = text.strip()

                                # extract addresses and total due
                                from ocr.ocr_tax_extractor import process_cv2_image
                                addr_list = self.extract_addresses_from_ocr(data["ocr_raw_text"], max_addresses=2)
                                ocr_img = cv2.imread(str(tmp_img))
                                ocr_json = process_cv2_image(ocr_img)
                                print(f"OCR JSON Data: {ocr_json}")
                                
                                data["address"] = addr_list[1]["address"] or ""
                                data["total_due"] = self.extract_total_due(img=img) or ""
                                first_amount = (
                                ocr_json.get("amounts", {})
                                    .get("top_by_score", [{}])[0]
                                    .get("numeric")
                                )

                                addresses = ocr_json.get("addresses", [])
                                data["zipcode"] = addr_list[1]["zipcode"] or ""
                                data["ocr_address"] = addresses or []
                                data["ocr_total_due"] = str(first_amount)

                            except Exception as e:
                                print(f"[ERROR] OCR extraction failed: {e}")

                            os.remove(tmp_img)
                    except Exception as e:
                        print(f"[ERROR] PDF generation failed: {e}")
                    await popup.close()
            return data
        except Exception as e:
            console.print(f"[red]Error in parse_lien_data: {e}[/red]\n{traceback.format_exc()}")
            return {}


    def extract_addresses_from_ocr(self, text, max_addresses=2):
        """
        Return list of addressses in dicts: [{'address': ..., 'zipcode': ...}, ...]
        """
        try:
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            addresses = []

            for idx, ln in enumerate(lines):
                m = re.search(r'([A-Za-z][A-Za-z0-9\.\'&\-\s]+,\s*[A-Za-z]{2}\s*\d{5})', ln)
                if m:
                    city_state_zip = m.group(1).strip()
                    street = ""
                    for j in range(1, 4):
                        if idx - j < 0:
                            break
                        prev = lines[idx - j]
                        if re.search(r'\b(County|Tax|Commissioner|Recorded|Doc:|Rept#|VS\b|Defendant|GRANT|PAYMENT|TOTAL DUE|PHONE|TEL|Fax)\b', prev, re.I):
                            continue
                        if re.search(r'^\d+\s', prev) or re.search(r'\b(St(reet)?|Street|Rd(?!\w)|Road|Highway|HWY|Ave|Avenue|Blvd|Lane|Ln|Dr(?!\w)|Drive|Way|Court|Ct|Parkway|PKWY|Memorial|HWY|HW|HWY|WY|SW|NE|N\.E\.|S\.W\.)\b', 
                            prev, re.I) or re.search(r'\d', prev):
                            street = prev
                            break
                        if not re.search(r'^\b(Grant|GORDON|GORDON COUNTY|SCOTT|LIEN|LIEN Bk|TOTAL|PAYMENT)\b', prev, re.I):
                            street = prev
                            break

                    full_address = (street + " " + city_state_zip).strip() if street else city_state_zip
                    zip_m = re.search(r'(\d{5})$', city_state_zip)
                    zipcode = zip_m.group(1) if zip_m else ""
                    addresses.append({"address": full_address, "zipcode": zipcode})

                    if len(addresses) >= max_addresses:
                        break

            while len(addresses) < max_addresses:
                addresses.append({"address": "", "zipcode": ""})
            return addresses
        except Exception as e:
            console.print(f"[red]Error in extract_addresses_from_ocr: {e}[/red]")
            return [{"address": "", "zipcode": ""} for _ in range(max_addresses)]
    
    
    def extract_total_due(self, img):          
        try:
            pil = img.convert("RGB")
            
            proc = self.preprocess_page(pil)
            line = self.find_total_due_line(proc)
            if line:
                m = re.search(r'(?<!\d)(\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})(?!\d)', line)
                line = m.group(1).replace(',', '') if m else None
            line = line if line else "Not found"
            print(f"Total Due: {line}")
            return line
        except Exception as e:
            console.print(f"[red]Error in extract_total_due: {e}[/red]")
            return "Error"


    def remove_table_lines(self, bw_inv: np.ndarray) -> np.ndarray:
        try:
            h, w = bw_inv.shape[:2]
            h_len = max(20, w // 30)
            v_len = max(20, h // 30)
            h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (h_len, 1))
            v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, v_len))
            horiz = cv2.morphologyEx(bw_inv, cv2.MORPH_OPEN, h_kernel, iterations=1)
            vert  = cv2.morphologyEx(bw_inv, cv2.MORPH_OPEN, v_kernel, iterations=1)
            grid  = cv2.bitwise_or(horiz, vert)
            return cv2.bitwise_and(bw_inv, cv2.bitwise_not(grid))
        except Exception as e:
            console.print(f"[red]Error in remove_table_lines: {e}[/red]")
            return bw_inv


    def preprocess_page(self,pil_img: Image.Image) -> np.ndarray:
        try:
            img = np.array(pil_img.convert("RGB"))
            gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
            scale = 2.0 if max(gray.shape) < 2000 else 1.5
            gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            gray = clahe.apply(gray)
            bw_inv = cv2.adaptiveThreshold(
                gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 31, 10
            )
            clean_inv = self.remove_table_lines(bw_inv)
            return cv2.bitwise_not(clean_inv)
        except Exception as e:
            console.print(f"[red]Error in preprocess_page: {e}[/red]")
            return np.array(pil_img.convert("L"))
        

    def find_total_due_line(self, proc_img: np.ndarray) -> str | None:
        """ Find line containing 'Total Due' or similar keywords. """
        try:
            cfg_page = "--oem 3 --psm 6 -l eng -c preserve_interword_spaces=1"
            page_text = pytesseract.image_to_string(proc_img, config=cfg_page)
            for raw in page_text.splitlines():
                if raw.strip() and TOTAL_LINE_REGEX.search(raw.upper()):
                    return raw
            data_dict = pytesseract.image_to_data(proc_img, config=cfg_page, output_type=pytesseract.Output.DICT)
            lines = {}
            for i, txt in enumerate(data_dict["text"]):
                if txt.strip():
                    key = (data_dict["block_num"][i], data_dict["par_num"][i], data_dict["line_num"][i])
                    lines.setdefault(key, []).append(txt)
            for parts in lines.values():
                line = " ".join(parts).strip()
                if TOTAL_LINE_REGEX.search(line.upper()):
                    return line
            return None
        except Exception as e:
            console.print(f"[red]Error in find_total_due_line: {e}[/red]")
            return None
        

    def save_to_excel(self, filename="lien_data.xlsx"):
        """ Save scraped results to Excel with clickable PDF links. """

        try:
            if not hasattr(self, "results") or not self.results:
                print("[red][WARNING] No results to save![/red]")
                return

            df = pd.DataFrame(self.results)
            columns = {
                "county": "County",
                "direct_party_debtor": "Direct Party (Debtor)",
                "reverse_party_claimant": "Reverse Party (Claimant)",
                "address": "Address",
                "ocr_address": "OCR Address",
                "zipcode": "Zipcode",
                "total_due": "Total Due",
                "ocr_total_due": "OCR Total Due",
                "instrument": "Instrument",
                "date_filed": "Date Filed",
                "book": "Book",
                "page": "Page",
                "description": "Description",
                "amount": "Amount",
                "pdf_document_url": "PDF Document URL",
                "pdf_filename": "View PDF",
            }

            df.rename(columns=columns, inplace=True)
            for data_col, xl_col in columns.items():
                if xl_col not in df.columns.tolist():
                    df[xl_col] = ""

            df = df[list(columns.values())]

            if "View PDF" in df.columns:
                df["View PDF"] = df["View PDF"].apply(
                    lambda x: f'=HYPERLINK("file:///{os.path.join(self.documents_dir, x).replace(os.sep, "/")}", "{x}")'
                    if isinstance(x, str) and x.strip() else ""
                )

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base, ext = os.path.splitext(filename)
            search_name = re.sub(r'[^a-zA-Z0-9]', '', self.form_data.get("search_name", "")).replace(" ", "_")
            final_filename = f"{base}_{search_name}_{ts}{ext}"
            final_path = os.path.join(self.county_folder_path, final_filename)

            with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)

            print("-" * 50)
            console.print(f"[bold green]Saved {len(df)} records to --> {final_path}[/bold green]")
            print("-" * 50)
            os.remove(self.csv_path)
        except Exception as e:
            console.print(f"[red]Error in save_to_excel: {e}[/red]")


    async def scrape(self, form_data: dict):
        """Run lien scraper dynamically with Django form data"""
        try:
            self.form_data = form_data
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

            print("Starting Lien Index Scraper...")
            print(f"Screen Resolution set to --> {WIDTH}x{HEIGHT}")
            context = await self.browser.new_context(
                storage_state=STATE_FILE if STATE_FILE.exists() else None,
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
                await self.page.goto(self.homepage, wait_until="domcontentloaded", timeout=60000)
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

            # Search using form data
            await self.start_search()
                
            # Extract all search result URLs
            await self.get_search_results()
            
            # Process all result URLs
            await self.process_result_urls()
            
            # Save data to excel
            self.save_to_excel()
        except Exception as e:
            console.print(f"[red]Error in scrape: {e}[/red]\n{traceback.format_exc()}")
        finally:
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()

