from __future__ import annotations

import os
import re
import ssl
import json
import random
import asyncio
import traceback
from pathlib import Path
from datetime import datetime


import cv2
import certifi
import img2pdf
import numpy as np
import pytesseract
import pandas as pd
from PIL import Image
from typing import Any, Dict
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

# Corrected Import to break circular dependency
from dashboard.utils.state import stop_scraper_flag 


# Load environment variables
load_dotenv()
console = Console()


# ---------- config -------------------------------------------------------------
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
UA_TYPE = "mac"
UA = UA_DICT.get(UA_TYPE, UA_DICT["win"])
EXTRA_HEADERS = {
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
}
TOTAL_LINE_REGEX = re.compile(r'(TOTAL\s*DUE|TOTALDUE)', re.I)


# load Tesseract path for Windows if needed
try:
    if os.name == "nt":  # Windows
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
except Exception as e:
    console.print(f"[red]Error setting up Tesseract: {e}[/red]")


# ---------- core scraping ----------------------------------------------------
class GSCCCAScraper:
    """Scrape the latest tax records from GSCCCA pages."""

    def __init__(self) -> None:
        try:
            self.page = None
            self.email = TAX_EMAIL
            self.password = TAX_PASSWORD
            self.form_data = {}
            self.homepage = "https://www.gsccca.org/"
            self.login_url = "https://apps.gsccca.org/login.asp"
            self.name_search_url = "https://search.gsccca.org/Lien/namesearch.asp"
            self.results = []
            

            script_dir = Path(__file__).parent.absolute() 
            
            self.base_output_dir = os.path.join(script_dir.parent, "output") 
            self.lien_data_dir = os.path.join(self.base_output_dir, "lien_data")
            self.downloads_dir = os.path.join(self.lien_data_dir, "documents") 
            os.makedirs(self.downloads_dir, exist_ok=True)
            self.excel_output_dir = self.lien_data_dir 
            os.makedirs(self.excel_output_dir, exist_ok=True)
            
            # console.print(f"[green]Lien Excel output directory: {self.excel_output_dir}[/green]")
            # console.print(f"[green]Lien Documents directory: {self.downloads_dir}[/green]")
            # # --------------------------------------------

            # SSL Context for aiohttp
            self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        except Exception as e:
            console.print(f"[red]Error initializing GSCCCAScraper: {e}[/red]")
            raise
        


    def time_sleep(self, a: int = 2500, b: int = 5000) -> int:
        try:
            return random.uniform(a, b)
        except Exception as e:
            console.print(f"[red]Error in time_sleep: {e}[/red]")
            return random.uniform(2000, 4000)

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

    async def login(self):
        """Perform login and save cookies."""
        try:
            print("[LOGIN] Trying to login...")
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
                console.print("[red][LOGIN] Login successful![/red]")
                await self.dump_cookies()
                return True
            else:
                print("[LOGIN] Login failed!")
                return False
        except Exception as e:
            console.print(f"[red]Error during login: {e}[/red]")
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
                print("[INFO] Announcement page detected. Redirecting to name search...")
                await self.page.select_option("#Options", "dismiss")
                await self.page.wait_for_timeout(1000)
                await self.page.click("input[name='Continue']")
        except Exception as e:
            console.print(f"[red]Error handling announcement: {e}[/red]")


    async def start_search(self):
        """Go directly to Name Search page."""
        try:
            print("[STEP 2] Going directly to Name Search page...")
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
        """On liennames.asp page, process ALL rows with Occurs values."""
        print("[STEP 4] Processing ALL rows with Occurs values...")
        try:
            await self.page.wait_for_selector("table.name_results", timeout=30_000)
            
            # Get total number of rows initially
            rows = await self.page.query_selector_all("table.name_results tr")
            total_rows = len(rows) - 1  # Exclude header
            
            print(f"[INFO] Found {total_rows} rows to process")
            
            # Store current search results URL for recovery
            search_results_url = self.page.url
            
            # Process each row sequentially
            for row_index in range(total_rows):
                print("*" * 50)
                print(f"Starting processing for row {row_index + 1}/{total_rows}")
                # Stop check
                if stop_scraper_flag['lien']:
                    console.print("[yellow]Stop signal received in get_search_results.[/yellow]")
                    break
                    
                try:
                    # Wait for table to be present and get fresh rows every time
                    await self.page.wait_for_selector("table.name_results", timeout=15000)
                    rows = await self.page.query_selector_all("table.name_results tr")
                    
                    if row_index + 1 >= len(rows):
                        print(f"[WARNING] Row index {row_index + 1} not found, skipping")
                        continue
                        
                    current_row = rows[row_index + 1]  # Skip header
                    cols = await current_row.query_selector_all("td")
                    
                    if len(cols) < 3:
                        print(f"[WARNING] Not enough columns in row {row_index + 1}, skipping")
                        continue
                        
                    # Get Occurs value and radio button
                    occurs_text = await cols[1].inner_text()
                    radio = await cols[0].query_selector("input[type='radio']")
                    
                    try:
                        occurs = int(occurs_text.strip())
                        print(f"[STEP 4] Processing row {row_index + 1}/{total_rows} with Occurs = {occurs}")
                        
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
                        await self.page.wait_for_load_state("domcontentloaded")
                        await self.page.wait_for_timeout(self.time_sleep(a=2000, b=3000))
                        
                        # Now process the RP details for this selection
                        await self.process_rp_details()
                        
                        # After processing RP details, navigate back to search results
                        print(f"[INFO] Completed processing for Occurs {occurs}. Navigating back to search results...")
                        
                        # Try multiple navigation methods
                        back_success = False
                        
                        # Method 1: Try specific back button
                        back_button = await self.page.query_selector("input[name='bBack']")
                        if back_button:
                            try:
                                await back_button.click()
                                await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
                                await self.page.wait_for_timeout(self.time_sleep(a=2000, b=3000))
                                back_success = True
                                print(f"[SUCCESS] Back to search results using bBack button")
                            except Exception as e:
                                print(f"[WARNING] bBack button failed: {e}")
                        
                        # Method 2: Try browser back if first method failed
                        if not back_success:
                            try:
                                await self.page.go_back()
                                await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
                                await self.page.wait_for_timeout(self.time_sleep(a=2000, b=3000))
                                back_success = True
                                print(f"[SUCCESS] Back to search results using browser back")
                            except Exception as e:
                                print(f"[WARNING] Browser back failed: {e}")
                        
                        # Method 3: Direct navigation to search results URL as fallback
                        if not back_success:
                            try:
                                await self.page.goto(search_results_url, wait_until="domcontentloaded", timeout=30000)
                                await self.page.wait_for_selector("table.name_results", timeout=15000)
                                back_success = True
                                print(f"[SUCCESS] Back to search results using direct URL navigation")
                            except Exception as e:
                                print(f"[WARNING] Direct navigation failed: {e}")
                        
                        # Final fallback: Go to name search page
                        if not back_success:
                            try:
                                await self.page.goto(self.name_search_url, wait_until="domcontentloaded", timeout=30000)
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
                            print(f"[SUCCESS] Successfully returned to search results after Occurs {occurs}")
                        except Exception as timeout_error:
                            print(f"[WARNING] Table reload timeout, but continuing...")
                            
                    except ValueError:
                        print(f"[WARNING] Invalid Occurs value: {occurs_text}, skipping")
                        continue
                        
                except Exception as e:
                    print(f"[ERROR] Failed to process row {row_index + 1}: {e}")
                    traceback.print_exc()
                    
                    # Enhanced recovery mechanism
                    try:
                        print("[INFO] Attempting enhanced recovery...")
                        
                        # Try to go back to search results URL
                        try:
                            await self.page.goto(search_results_url, wait_until="domcontentloaded", timeout=30000)
                            await self.page.wait_for_selector("table.name_results", timeout=15000)
                            print("[SUCCESS] Recovered to search results via URL")
                        except Exception:
                            # If that fails, go to name search and re-search
                            print("[INFO] URL recovery failed, trying fresh search...")
                            await self.page.goto(self.name_search_url, wait_until="domcontentloaded", timeout=30000)
                            await self.start_search()
                            await self.page.wait_for_selector("table.name_results", timeout=15000)
                            # Update search results URL
                            search_results_url = self.page.url
                            print("[SUCCESS] Recovered via fresh search")
                        
                        # Re-calculate total rows after recovery
                        rows = await self.page.query_selector_all("table.name_results tr")
                        total_rows = len(rows) - 1
                        print(f"[INFO] After recovery: {total_rows} rows remaining")
                        
                    except Exception as recovery_error:
                        print(f"[ERROR] Enhanced recovery failed: {recovery_error}")
                        # If recovery fails, break the loop
                        break

            print("[INFO] Completed processing all Occurs values")

        except Exception as e:
            console.print(f"[red]Error in get_search_results: {e}[/red]")
            traceback.print_exc()
        
    async def human_delay(self, min_t=0.8, max_t=2.0):
        try:
            t = random.uniform(min_t, max_t)
            await asyncio.sleep(t)
        except Exception as e:
            console.print(f"[red]Error in human_delay: {e}[/red]")
            await asyncio.sleep(1.0)

    async def human_scroll(self, min_y=200, max_y=800):
        try:
            y = random.randint(min_y, max_y)
            await self.page.mouse.wheel(0, y)
            print(f"[HUMAN] Scrolled {y}px")
            await self.human_delay(0.5, 1.2)
        except Exception as e:
            console.print(f"[red]Error in human_scroll: {e}[/red]")


    async def process_rp_details(self):
        """Step 5: Process all RP buttons, extract data and save with improved reliability"""
        current_results_count = len(self.results)
        visited_pages = set()
        current_page = 1

        print(f"[INFO] Starting RP details processing... Current results: {current_results_count}")

        try:
            count = 0
            while True:
                # Stop check
                count += 1
                print("-" * 50)
                if count == 4:
                    break
                if stop_scraper_flag['lien']:
                    console.print("[yellow]Stop signal received. Stopping lien scraper.[/yellow]")
                    break

                # Wait for page to load completely and get fresh RP links
                await self.page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(2)
                
                rp_links = await self.page.query_selector_all("a[href*='lienfinal']")
                if not rp_links:
                    print("[WARNING] No RP buttons found on this page")
                    break

                # Use page URL as unique identifier instead of first link href
                current_url = self.page.url
                if current_url in visited_pages:
                    print(f"[INFO] Duplicate page detected → already visited. Stopping loop.")
                    break
                visited_pages.add(current_url)

                total = len(rp_links)
                print(f"[INFO] Found {total} RP buttons on page {current_page}")

                # Process each RP link with fresh element references
                for i in range(3):  # Limit to 100 per page for safety
                    print("-" * 30)
                    print(f"[INFO] Processing RP {i+1}/{total} on page {current_page}")
                    if stop_scraper_flag['lien']:
                        console.print("[yellow]Stop signal received. Stopping lien scraper.[/yellow]")
                        break

                    try:
                        # Refresh RP links to avoid stale elements
                        await asyncio.sleep(1)
                        rp_links = await self.page.query_selector_all("a[href*='lienfinal']")
                        if i >= len(rp_links):
                            print(f"[WARNING] RP link index {i} not available, skipping")
                            continue

                        link = rp_links[i]
                        link_text = await link.inner_text() or f"RP_{i+1}"
                        print(f"[INFO] Processing RP {i+1}/{total}: {link_text}")

                        # Click with improved retry mechanism
                        retries = 3
                        clicked_successfully = False
                        for attempt in range(retries):
                            try:
                                await link.scroll_into_view_if_needed()
                                await asyncio.sleep(1)
                                await link.click()
                                await self.page.wait_for_load_state("domcontentloaded")
                                await asyncio.sleep(2)
                                
                                # Verify we navigated to a new page
                                new_url = self.page.url
                                if new_url != current_url and "lienfinal" in new_url:
                                    clicked_successfully = True
                                    break
                                else:
                                    print(f"[RETRY] Page didn't navigate properly, attempt {attempt + 1}")
                                    await self.page.go_back()
                                    await asyncio.sleep(2)
                                    
                            except Exception as e:
                                print(f"[ERROR] Click failed (Attempt {attempt+1}/{retries}): {e}")
                                if attempt == retries - 1:
                                    raise
                                await asyncio.sleep(2)

                        if not clicked_successfully:
                            print(f"[ERROR] Failed to navigate for RP {i+1}, skipping")
                            continue

                        if stop_scraper_flag['lien']:
                            break

                        # Parse the RP detail page
                        data = await self.parse_rp_detail()
                        if data:
                            self.results.append(data)
                            print(f"[SUCCESS] Saved RP {i+1}/{total} on page {current_page} → "
                                f"{data.get('Name Selected','N/A')} | "
                                f"Book={data.get('Book','')} Page={data.get('Page','')}")

                        # Navigate back with improved reliability
                        await asyncio.sleep(2)
                        
                        # Try multiple back methods
                        back_success = False
                        back_methods = [
                            ("Back button", "input[name='bBack']"),
                            ("Back button by value", "input[value='Back']"),
                            ("Back button by type", "input[type='button'][value*='Back']")
                        ]

                        for method_name, selector in back_methods:
                            try:
                                back_btn = await self.page.query_selector(selector)
                                if back_btn:
                                    await back_btn.click()
                                    await self.page.wait_for_load_state("domcontentloaded")
                                    await asyncio.sleep(2)
                                    back_success = True
                                    print(f"[SUCCESS] Back using {method_name}")
                                    break
                            except Exception as e:
                                print(f"[WARNING] {method_name} failed: {e}")

                        if not back_success:
                            print("[WARNING] All back methods failed, using browser back")
                            await self.page.go_back()
                            await self.page.wait_for_load_state("domcontentloaded")
                            await asyncio.sleep(2)

                        # Verify we're back on the main list page
                        try:
                            await self.page.wait_for_selector("a[href*='lienfinal']", timeout=10000)
                        except Exception as e:
                            print(f"[WARNING] Could not verify return to list page: {e}")
                            # Try to recover by going to the previous URL
                            if len(visited_pages) > 1:
                                previous_pages = list(visited_pages)
                                previous_url = previous_pages[-2] if len(previous_pages) >= 2 else previous_pages[0]
                                await self.page.goto(previous_url, wait_until="domcontentloaded")

                    except Exception as e:
                        print(f"[ERROR] Failed at RP {i+1} on page {current_page}: {e}")
                        traceback.print_exc()
                        
                        # Recovery attempt
                        try:
                            await self.page.goto(current_url, wait_until="domcontentloaded")
                            await asyncio.sleep(3)
                            print("[SUCCESS] Recovered to main list page")
                        except Exception as recovery_error:
                            print(f"[ERROR] Recovery failed: {recovery_error}")
                            break

                # Stop check after inner loop
                if stop_scraper_flag['lien']:
                    break

                # Improved pagination handling
                print(f"[INFO] Looking for next page... Current page: {current_page}")
                
                next_page_link = None
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
                        print(f"[INFO] Found next page link with selector: {selector}")
                        break

                if next_page_link and not stop_scraper_flag['lien']:
                    current_page += 1
                    print(f"[INFO] Navigating to page {current_page}...")
                    
                    try:
                        # Get the href for recovery
                        next_href = await next_page_link.get_attribute("href")
                        
                        await next_page_link.click()
                        await self.page.wait_for_load_state("domcontentloaded")
                        await asyncio.sleep(3)
                        
                        # Verify navigation by checking for RP links
                        try:
                            await self.page.wait_for_selector("a[href*='lienfinal']", timeout=15000)
                            print(f"[SUCCESS] Navigated to page {current_page}")
                        except Exception as e:
                            print(f"[WARNING] RP links not found after navigation, trying JavaScript method")
                            # Fallback to JavaScript navigation
                            if next_href and "fnSubmitThisForm" in next_href:
                                url_match = re.search(r"fnSubmitThisForm\('([^']+)'\)", next_href)
                                if url_match:
                                    next_url = url_match.group(1)
                                    if not next_url.startswith("http"):
                                        next_url = "https://search.gsccca.org/Lien/" + next_url
                                    await self.page.goto(next_url, wait_until="domcontentloaded")
                                    await self.page.wait_for_selector("a[href*='lienfinal']", timeout=15000)
                                    print(f"[SUCCESS] Navigated via JavaScript to page {current_page}")
                            
                    except Exception as e:
                        print(f"[ERROR] Pagination failed: {e}")
                        break
                else:
                    print(f"[INFO] No more pages found. Total pages processed: {current_page}")
                    break

        except Exception as e:
            console.print(f"[red]Error in process_rp_details: {e}[/red]")
            traceback.print_exc()


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


    async def parse_rp_detail(self):
        """ Helper: Parse lienfinal.asp detail page with BeautifulSoup + Viewer URL + Single Page PDF + OCR + Address1/2 + Zipcode1/2 """
        try:
            await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
            await asyncio.sleep(1.5)
            html = await self.page.content()
            soup = BeautifulSoup(html, "html.parser")
            data = {}
            
            # Stop check
            if stop_scraper_flag['lien']:
                console.print("[yellow]Stop signal received during parse_rp_detail.[/yellow]")
                return {}

            def safe_text(el):
                return el.get_text(" ", strip=True) if el else ""

            # ---------- Normal Data Extraction ----------
            header_table = soup.find("table", cellpadding="2")
            if header_table:
                rows = header_table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) == 2:
                        key = safe_text(cells[0]).rstrip(":")
                        val = safe_text(cells[1])
                        data[key] = val
                    elif len(cells) == 1:
                        data["Extra Info"] = safe_text(cells[0])

            doc_table = soup.find("table", width="800", cellpadding="0", cellspacing="0")
            if doc_table:
                rows = doc_table.find_all("tr")[1:]
                if rows:
                    cols = [safe_text(td) for td in rows[0].find_all("td")]
                    if len(cols) >= 6:
                        data.update({
                            "County": cols[0],
                            "Instrument": cols[1],
                            "Date Filed": cols[2],
                            "Time": cols[3],
                            "Book": cols[4],
                            "Page": cols[5],
                        })

            desc_table = soup.find("td", string=lambda t: t and "Description" in t)
            if desc_table:
                tbody = desc_table.find_parent("table")
                desc_val = safe_text(tbody.find_all("tr")[1].find("td"))
                data["Description"] = desc_val

            debtor_table = soup.find("td", string=lambda t: t and "Direct Party (Debtor)" in t)
            if debtor_table:
                tbody = debtor_table.find_parent("table")
                debtors = [safe_text(td) for td in tbody.find_all("td")[1:]]
                data["Direct Party (Debtor)"] = "; ".join(debtors)

            claimant_table = soup.find("td", string=lambda t: t and "Reverse Party (Claimant)" in t)
            if claimant_table:
                tbody = claimant_table.find_parent("table")
                claimants = [safe_text(td) for td in tbody.find_all("td")[1:]]
                data["Reverse Party (Claimant)"] = "; ".join(claimants)

            cross_table = soup.find("td", string=lambda t: t and "Cross-Referenced Instruments" in t)
            if cross_table:
                tbody = cross_table.find_parent("table")
                rows = tbody.find_all("tr")[1:]
                refs = []
                for row in rows:
                    cols = [safe_text(td) for td in row.find_all("td")]
                    if any(cols):
                        refs.append(" | ".join(cols))
                data["Cross-Referenced Instruments"] = "; ".join(refs)

            record_info = soup.find("i")
            if record_info:
                data["Record Added"] = safe_text(record_info)

            # ---------- helper: extract up to 2 addresses (Address) ----------
            def extract_addresses_from_ocr(text, max_addresses=2):
                """
                Return list of dicts: [{'address': ..., 'zipcode': ...}, ...] length == max_addresses (padded).
                Logic: look for lines that contain 'City, ST 12345' pattern (2-letter state + 5-digit zip),
                then take preceding non-header line(s) as street line when available.
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
                                if re.search(r'^\d+\s', prev) or re.search(r'\b(St(reet)?|Street|Rd(?!\w)|Road|Highway|HWY|Ave|Avenue|Blvd|Lane|Ln|Dr(?!\w)|Drive|Way|Court|Ct|Parkway|PKWY|Memorial|HWY|HW|HWY|WY|SW|NE|N\.E\.|S\.W\.)\b', prev, re.I) or re.search(r'\d', prev):
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
            
            # ---------- Viewer URL + Single Page PDF ----------
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
                        f"https://search.gsccca.org/Imaging/HTML5Viewer.aspx?"
                        f"id={lien_id}&key1={book}&key2={page_num}&county={county}&userid={userid}&appid={appid}"
                    )
                    data["PDF Document URL"] = viewer_url
                    print(f"[INFO] Viewer URL captured → {viewer_url}")

                    # ---------- PDF File Name ----------
                    debtor_name = data.get("Direct Party (Debtor)", "UnknownDebtor").split(";")[0][:40]
                    debtor_name = debtor_name.replace(" ", "_").replace(",", "")
                    pdf_name = f"{debtor_name}_Page{page_num}.pdf"

                    # Use the new downloads directory
                    pdf_path = os.path.join(self.downloads_dir, pdf_name)

                    try:
                        popup = await self.page.context.new_page()
                        await popup.goto(viewer_url, timeout=50000)
                        await popup.wait_for_load_state("domcontentloaded")
                        await asyncio.sleep(3)  # let canvas render
                        
                        # Stop check before screenshot/OCR
                        if stop_scraper_flag['lien']:
                            await popup.close()
                            return data

                        # NEW: Select "Fit Window" option from zoom dropdown
                        print("[INFO] Selecting 'Fit Window' option for proper image display...")
                        
                        # Wait for the zoom selector to be available
                        await popup.wait_for_selector("td.vtm_zoomSelectCell select", timeout=10000)
                        
                        # Select "Fit Window" option
                        fit_window_option = "fitwindow"
                        await popup.select_option("td.vtm_zoomSelectCell select", fit_window_option)
                        print(f"[INFO] Selected 'Fit Window' option")
                        
                        # Wait for the image to adjust to the new zoom level
                        await asyncio.sleep(3)
                        
                        # Additional wait for canvas content to render properly
                        await popup.wait_for_selector("div.vtm_imageClipper canvas", timeout=10000, state="attached")
                        await asyncio.sleep(2)

                        canvas = await popup.query_selector("div.vtm_imageClipper canvas")

                        if canvas:
                            # Use downloads_dir for temp file
                            tmp_img = os.path.join(self.downloads_dir, f"tmp_{page_num}.png")
                            
                            # Take screenshot with full page option if needed
                            try:
                                await canvas.screenshot(path=tmp_img, timeout=30000)
                                print(f"[INFO] Canvas screenshot saved to {tmp_img}")
                                
                                # Verify the screenshot was taken properly
                                if os.path.exists(tmp_img) and os.path.getsize(tmp_img) > 0:
                                    print(f"[SUCCESS] Screenshot verified - file size: {os.path.getsize(tmp_img)} bytes")
                                else:
                                    print("[WARNING] Screenshot file is empty or missing, trying full page screenshot...")
                                    await popup.screenshot(path=tmp_img, full_page=True, timeout=30000)
                                    print(f"[INFO] Fallback: Full page screenshot saved to {tmp_img}")
                                    
                            except Exception as screenshot_error:
                                print(f"[WARNING] Canvas screenshot failed: {screenshot_error}. Trying full page screenshot...")
                                # Fallback: take full page screenshot
                                await popup.screenshot(path=tmp_img, full_page=True, timeout=30000)
                                print(f"[INFO] Fallback: Full page screenshot saved to {tmp_img}")

                            # Convert single PNG to PDF
                            with open(pdf_path, "wb") as f:
                                f.write(img2pdf.convert([tmp_img]))

                            data["PDF"] = pdf_name
                            print(f" [INFO] PDF saved (single page) → {pdf_path}")

                            # ----------- OCR Extraction + Address1/2 -----------
                            try:
                                img = Image.open(tmp_img).convert("L")
                                text = pytesseract.image_to_string(img, lang="eng")
                                data["OCR_Text"] = text.strip()
                                print(f" [OCR] OCR extracted → {len(text.split())} words")

                                # extract up to 2 addresses
                                addr_list = extract_addresses_from_ocr(data["OCR_Text"], max_addresses=2)
                                data["Address"] = addr_list[1]["address"]
                                data["Zipcode"] = addr_list[1]["zipcode"]
                                
                                proc_img = self.preprocess_page(img)
                                line = self.find_total_due_line(proc_img)
                                data["Total Due"] = self.extract_total_due(img=img)

                            except Exception as e:
                                print(f"[ERROR] OCR extraction failed: {e}")
                                data["OCR_Text"] = ""
                                data["Address"] = ""
                                data["Zipcode"] = ""
                                data["Total Due"] = ""
                            # ----------------------------------------------------

                            os.remove(tmp_img)
                        else:
                            print("[WARNING] No canvas found in popup, trying full page screenshot...")
                            # Fallback: take full page screenshot
                            tmp_img = os.path.join(self.downloads_dir, f"tmp_{page_num}.png")
                            await popup.screenshot(path=tmp_img, full_page=True)
                            with open(pdf_path, "wb") as f:
                                f.write(img2pdf.convert([tmp_img]))
                            data["PDF"] = pdf_name
                            print(f" [INFO] PDF saved (full page fallback) → {pdf_path}")

                        await popup.close()

                    except Exception as e:
                        print(f"[ERROR] PDF generation failed: {e}")
                        data["PDF"] = ""
                        data["OCR_Text"] = ""
                        data["Address"] = ""
                        data["Zipcode"] = ""
                        data["Total Due"] = ""
                else:
                    data["PDF Document URL"] = ""
                    data["PDF"] = ""
                    data["OCR_Text"] = ""
                    data["Address"] = ""
                    data["Zipcode"] = ""
                    data["Total Due"] = ""
            else:
                data["PDF Document URL"] = ""
                data["PDF"] = ""
                data["OCR_Text"] = ""
                data["Address"] = ""
                data["Zipcode"] = ""
                data["Total Due"] = ""

            return data
        except Exception as e:
            console.print(f"[red]Error in parse_rp_detail: {e}[/red]")
            return {}


    def save_to_excel(self, filename="LienResults.xlsx"):
        """ Save scraped results to Excel with clickable PDF links. """

        try:
            if not hasattr(self, "results") or not self.results:
                print("[WARNING] No results to save")
                return

            df = pd.DataFrame(self.results)

            columns = [
                "Direct Party (Debtor)", "Reverse Party (Claimant)", "Address", "Zipcode", "Total Due",
                "County", "Instrument", "Date Filed", "Book", "Page","Description",
                "PDF Document URL", "PDF",
            ]

            for col in columns:
                if col not in df.columns:
                    df[col] = ""

            df = df[columns]

            if "PDF" in df.columns:
                # IMPORTANT: downloads_dir ab Project Root ke relative hai
                df["PDF"] = df["PDF"].apply(
                    lambda x: f'=HYPERLINK("file:///{os.path.join(self.downloads_dir, x).replace(os.sep, "/")}", "{x}")'
                    if isinstance(x, str) and x.strip() else ""
                )

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base, ext = os.path.splitext(filename)
            final_filename = f"{base}_{ts}{ext}"
            
            # Use self.excel_output_dir (jo Project Root ke andar hai)
            final_path = os.path.join(self.excel_output_dir, final_filename)

            with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)

            print(f"[INFO] Saved {len(df)} records to {final_path}")
        except Exception as e:
            console.print(f"[red]Error in save_to_excel: {e}[/red]")
            
            

    async def scrape(self, page_url: str) -> Dict[str, Any]:
        """Open *url* in the current page and return latest post data."""
        try:
            await self.page.goto(page_url, wait_until="domcontentloaded", timeout=60_000)
            await self.page.wait_for_timeout(3000)

            title = await self.page.title()

            rows = await self.page.query_selector_all("table tr")
            data = []
            for row in rows:
                text = await row.inner_text()
                data.append(text.strip())

            return {
                "page_title": title,
                "row_count": len(rows),
                "rows": data[:10]
            }

        except Exception as e:
            console.print(f"[red]Scrape error: {e}[/red]")
            return {}


    async def run_dynamic(self, form_data: dict):
        """Run lien scraper dynamically with Django form data"""
        try:
            self.form_data = form_data
            playwright = await pw.async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=HEADLESS,
                channel="chrome",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--start-maximized",
                    "--no-proxy-server",
                ]
            )

            print("Starting scraper...")
            context = await browser.new_context(
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

            # login if needed
            if STATE_FILE.exists():
                await context.add_cookies(json.loads(Path(STATE_FILE).read_text())["cookies"])
                await self.page.goto(self.homepage, wait_until="domcontentloaded", timeout=60000)
                await self.check_and_handle_announcement()
            else:
                await self.page.goto("https://google.com", wait_until="domcontentloaded")
                await self.page.wait_for_timeout(self.time_sleep(3, 5))
                await self.page.goto(self.login_url, wait_until="domcontentloaded")
                if not await self.check_session():
                    print("Attempting fresh login...")
                    await self.login()
                    await self.page.wait_for_timeout(self.time_sleep())
                        
            # Global stop check before starting core work
            if stop_scraper_flag['lien']:
                console.print("[yellow]Scraper started but received immediate stop signal. Exiting.[/yellow]")
                await browser.close()
                await playwright.stop()
                return

            # --- Steps using form data ---
            if not await self.check_session():
                console.print("[yellow]Session invalid → Re-logging in...[/yellow]")
                await self.login()
                await self.page.wait_for_timeout(self.time_sleep())

            await self.start_search()
            
            if stop_scraper_flag['lien']:
                console.print("[yellow]Stop signal received after search start. Exiting.[/yellow]")
                await browser.close()
                await playwright.stop()
                return
                
            await self.get_search_results()
            
            if stop_scraper_flag['lien']:
                console.print("[yellow]Stop signal received after search results. Exiting.[/yellow]")
                await browser.close()
                await playwright.stop()
                return
                
            await self.process_rp_details()
            self.save_to_excel()

            await browser.close()
            await playwright.stop()
        except Exception as e:
            console.print(f"[red]Error in run_dynamic: {e}[/red]")
            traceback.print_exc()


async def main() -> None:
    try:
        scraper = GSCCCAScraper()
        await scraper.run()
    except Exception as e:
        console.print(f"[red]Error in main: {e}[/red]")
        traceback.print_exc()


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
