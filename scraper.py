from __future__ import annotations

import os
import re
import json
import math
import random
import asyncio
import traceback
import pandas as pd
import requests
import openpyxl

from pathlib import Path
from PIL import Image
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw
from typing import Tuple, Optional, Any, Dict
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

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
UA_TYPE = "win"
UA = UA_DICT.get(UA_TYPE, UA_DICT["win"])
EXTRA_HEADERS = {
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"
}

# ---------- core scraping ----------------------------------------------------
class GSCCCAScraper:
    """Scrape the latest tax records from GSCCCA pages."""

    def __init__(self) -> None:
        self.page = None
        self.email = TAX_EMAIL
        self.password = TAX_PASSWORD
        self.homepage = "https://www.gsccca.org/"
        self.login_url = "https://apps.gsccca.org/login.asp"
        self.name_search_url = "https://search.gsccca.org/Lien/namesearch.asp"
        self.results = []

    def time_sleep(self, a: int = 2500, b: int = 5000) -> int:
        return random.uniform(a, b)

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
            await self.page.wait_for_load_state("domcontentloaded")
            all_text = await self.page.evaluate("document.body.innerText")
            if "logout" in all_text.lower():
                print("Logout link detected -> User is already logged in.")
                return True

            print("Could not detect login/logout clearly.")
            return False

        except Exception as e:
            print(f"[already_logged_in ERROR] {e}")
            return False

    async def login_(self, email: str, password: str):
        """Perform login and save cookies."""
        print("[LOGIN] Navigating to login page...")
        await self.page.goto(self.login_url, wait_until="domcontentloaded", timeout=60000)
        await self.page.wait_for_timeout(self.time_sleep())
        await self.check_and_handle_announcement()

        print(f"[LOGIN] Filling username: {email}")
        await self.page.fill("input[name='txtUserID']", email)

        print("[LOGIN] Filling password...")
        await self.page.fill("input[name='txtPassword']", password)

        try:
            print("[LOGIN] Clicking login button...")
            await self.page.click("img[name='logon']")
        except Exception as e:
            print(f"[LOGIN] Click failed: {e}, using JS submit...")
            await self.page.evaluate("document.forms['frmLogin'].submit()")

        await self.page.wait_for_load_state("networkidle", timeout=15000)

        if await self.page.query_selector("a:has-text('Logout')"):
            print("[LOGIN] Login successful!")
            await self.dump_cookies()
            return True
        else:
            print("[LOGIN] Login failed")
            return False
    
    async def step1_open_homepage(self):
        """Go to homepage & check if logged in."""
        print("[STEP 1] Opening homepage...")
        await self.page.goto(self.homepage, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(self.time_sleep())

        if await self.already_logged_in():
            print("[STEP 1] Logged in detected")
            return True
        else:
            print("[STEP 1] Not logged in")
            return False
        
    async def check_and_handle_announcement(self):
        """Check if announcement page loaded; if yes, redirect to name_search_url."""
        current_url = self.page.url
        if "CustomerCommunicationApiAnnouncement1.asp" in current_url:
            print("[INFO] Announcement page detected. Redirecting to name search...")
            await self.page.select_option("#Options", "dismiss")
            await self.page.wait_for_timeout(1500)
            await self.page.click("input[name='Continue']")

    async def step2_click_name_search(self):
        """Go directly to Name Search page."""
        print("[STEP 2] Going directly to Name Search page...")
        try:
            await self.page.goto(self.name_search_url, wait_until="domcontentloaded", timeout=60000)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.check_and_handle_announcement()
            print("[STEP 2] Landed on Name Search page")
        except Exception as e:
            print(f"[STEP 2 ERROR] {e}")

    async def step3_fill_form(self):
        """Fill Name Search form with given details."""
        print("[STEP 3] Filling Name Search form...")

        try:
            await self.page.select_option("#txtPartyType", "2")
            await self.page.wait_for_timeout(self.time_sleep())

            await self.page.select_option("select[name='txtInstrCode']", "2")
            await self.page.wait_for_timeout(self.time_sleep())

            await self.page.select_option("select[name='intCountyID']", "64")
            await self.page.wait_for_timeout(self.time_sleep())

            await self.page.check("input[name='bolInclude'][value='0']")
            await self.page.wait_for_timeout(self.time_sleep())

            search_box = await self.page.query_selector("#txtSearchName")
            await search_box.click()
            for ch in "gordon":
                await self.page.keyboard.type(ch, delay=random.randint(100, 250)) 
            await self.page.wait_for_timeout(self.time_sleep())

            await self.page.fill("input[name='txtFromDate']", "")
            for ch in "01/01/2025":
                await self.page.keyboard.type(ch, delay=random.randint(100, 220))

            await self.page.fill("input[name='txtToDate']", "")
            for ch in "09/23/2025":
                await self.page.keyboard.type(ch, delay=random.randint(100, 220))
            await self.page.wait_for_timeout(self.time_sleep())

            await self.page.select_option("select[name='MaxRows']", "100")
            await self.page.wait_for_timeout(self.time_sleep())

            await self.page.select_option("select[name='TableType']", "1")
            await self.page.wait_for_timeout(self.time_sleep())
 
            await self.page.click("form[name='SearchType'] input[value='Search']")
            print("[STEP 3] Form filled successfully")

        except Exception as e:
            print(f"[STEP 3 ERROR] {e}")

    async def step4_select_highest_occurs(self):
        """On liennames.asp page, select row with highest Occurs score."""
        print("[STEP 4] Selecting row with highest Occurs...")

        try:
            await self.page.wait_for_selector("table.name_results", timeout=30_000)
            rows = await self.page.query_selector_all("table.name_results tr")
            highest_occurs = -1
            best_radio = None
            for row in rows[1:]:  
                cols = await row.query_selector_all("td")
                if len(cols) < 3:
                    continue
                occurs_text = await cols[1].inner_text()
                name_text = await cols[2].inner_text()
                radio = await cols[0].query_selector("input[type='radio']")
                try:
                    occurs = int(occurs_text.strip())
                except:
                    continue

                if occurs > highest_occurs:
                    highest_occurs = occurs
                    best_radio = radio

            if best_radio:
                await best_radio.click()
                print(f"[STEP 4] Selected row with highest Occurs = {highest_occurs}")
            else:
                print("[STEP 4] No rows found to select")
                
            await self.page.click("input[value='Display Details']")
            await self.page.wait_for_load_state("domcontentloaded")
            await self.page.wait_for_timeout(self.time_sleep())

        except Exception as e:
            print(f"[STEP 4 ERROR] {e}")

    async def human_delay(self, min_t=0.8, max_t=2.0):
        t = random.uniform(min_t, max_t)
        await asyncio.sleep(t)

    async def human_scroll(self, min_y=200, max_y=800):
        y = random.randint(min_y, max_y)
        await self.page.mouse.wheel(0, y)
        print(f"[HUMAN] Scrolled {y}px")
        await self.human_delay(0.5, 1.2)

    async def process_all_rp(self):
        rp_index = 0
        while True:
            rp_links = await self.page.query_selector_all("a[href*='lienfinal.asp'] img[src*='sym_rp.gif']")
            if rp_index >= len(rp_links):
                print("[INFO] No more RP links found.")
                break
            print(f"[INFO] Processing RP #{rp_index+1}")

            rp_links = await self.page.query_selector_all("a[href*='lienfinal.asp'] img[src*='sym_rp.gif']")
            rp_link = await rp_links[rp_index].evaluate_handle("node => node.parentElement")

            await rp_link.click()
            await self.page.wait_for_load_state("domcontentloaded")
            await self.human_delay()

            try:
                await self.process_rp_detail()
            except Exception as e:
                print(f"[ERROR] Failed inside RP {rp_index+1}: {e}")

            back = await self.page.query_selector("input[name='bBack']")
            if back:
                await back.click()
                await self.page.wait_for_load_state("domcontentloaded")
                await self.human_delay()

            rp_index += 1
   
    async def scrape_documents(self):
        """Scrape the documents table from nameselected.asp"""
        await self.human_scroll()
        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")
        documents = []

        for table in soup.select("table.table_borders"):
            rows = table.find_all("tr")
            if not rows:
                continue
            cols = rows[0].find_all("td", class_="reg_deed_cell_borders")
            if len(cols) >= 6:
                link_el = cols[0].find("a")
                href = link_el.get("href") if link_el else None

                doc = {
                    "link": href,
                    "county": cols[1].get_text(strip=True),
                    "instrument": cols[2].get_text(strip=True),
                    "filed": cols[3].get_text(strip=True),
                    "book": cols[4].get_text(strip=True),
                    "page": cols[5].get_text(strip=True),
                    "properties": [],
                    "cross_refs": []
                }
                
                nested_table = rows[1].find("table")
                if nested_table:
                    for nrow in nested_table.find_all("tr"):
                        props = [td.get_text(strip=True) for td in nrow.find_all("td")]
                        if props:
                            doc["properties"].append(props)

                cross_tables = rows[1].find_all("table")
                if len(cross_tables) > 1:
                    cross_table = cross_tables[-1]
                    for crow in cross_table.find_all("tr")[1:]:
                        ccols = [td.get_text(strip=True) for td in crow.find_all("td")]
                        if any(ccols):
                            doc["cross_refs"].append(ccols)

                documents.append(doc)

        print(f"[INFO] Scraped {len(documents)} document(s) from nameselected.asp")
        for d in documents:
            print("   →", d)

        self.results.extend(documents)

        back = await self.page.query_selector("input[name='bBack']")
        if back:
            await back.click()
            await self.page.wait_for_load_state("domcontentloaded")
            await self.human_delay()

    async def search_real_estate(self, name):
        await self.page.goto("https://search.gsccca.org/RealEstate/namesearch.asp")
        await self.page.wait_for_selector("#txtSearchName")
        await self.human_scroll()
        await self.human_delay()

        await self.page.fill("#txtSearchName", name)
        await self.human_delay()
        await self.page.click("#btnSubmit")
        await self.page.wait_for_load_state("domcontentloaded")
        await self.human_delay()

        await self.select_highest_occurs()
        back = await self.page.query_selector("input[name='bBack']")
        if back:
            await back.click()
            await self.page.wait_for_load_state("domcontentloaded")
            await self.human_delay()

    async def select_highest_occurs(self):
        await self.human_scroll()
        rows = await self.page.query_selector_all("table.name_results tr[style]")
        best_row, max_occurs = None, -1
        for row in rows:
            cols = await row.query_selector_all("td")
            if len(cols) >= 3:
                occurs = int((await cols[1].inner_text()).strip())
                if occurs > max_occurs:
                    max_occurs = occurs
                    best_row = row

        if best_row:
            radio = await best_row.query_selector("input[type='radio']")
            await radio.click()
            await self.human_delay()
            await self.page.click("#btnDisplayDetails")
            await self.page.wait_for_load_state("domcontentloaded")
            await self.human_delay()
            await self.scrape_documents()
        back = await self.page.query_selector("input[name='bBack']")
        if back:
            await back.click()
            await self.page.wait_for_load_state("domcontentloaded")
            await self.human_delay()

    async def process_rp_detail(self):
        await self.human_scroll()
        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")
        debtor_names = []
        debtor_header = soup.find("td", string=lambda t: t and "Direct Party (Debtor)" in t)
        if debtor_header:
            tbody = debtor_header.find_parent("tbody")
            if tbody:
                rows = tbody.find_all("tr")
                for row in rows[1:]:  
                    td = row.find("td")
                    if td and td.text.strip():
                        debtor_names.append(td.text.strip())

        print(f"[INFO] Found Debtor Names: {debtor_names}")
        documents = []
        rows = soup.select("table.table_borders > tr, table.table_borders > tbody > tr")
        for row in rows:
            cols = row.find_all("td", class_="reg_deed_cell_borders")
            if len(cols) >= 6:
                link_el = cols[0].find("a")
                href = link_el.get("href") if link_el else None

                doc = {
                    "link": href,
                    "county": cols[1].get_text(strip=True),
                    "instrument": cols[2].get_text(strip=True),
                    "filed_date": cols[3].get_text(strip=True),
                    "book": cols[4].get_text(strip=True),
                    "page": cols[5].get_text(strip=True),
                }
                documents.append(doc)

        print(f"[INFO] Collected {len(documents)} document(s)")
        for d in documents:
            print("   →", d)
        for debtor in debtor_names:
            print(f"[INFO] Searching for debtor: {debtor}")
            await self.search_real_estate(debtor)

        return {
            "debtors": debtor_names,
            "documents": documents
        }

    def save_to_excel(self, filename="results.xlsx"):
        try:
            if not self.results:
                print("[WARN] No results to save.")
                return
            if isinstance(self.results[0], dict):
                df = pd.DataFrame(self.results)
            else:
                df = pd.DataFrame({"Data": self.results})

            df.to_excel(filename, index=False, engine="openpyxl")
            print(f"[INFO] Results saved to {filename}")

        except Exception as e:
            print(f"[ERROR] Failed to save results: {e}")


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

    async def run(self):
        """initialize plawright browser and run scraper"""
        playwright = await pw.async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=HEADLESS, 
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ]
        )

        # Initialise browser session
        if STATE_FILE.exists():
            print("Loaded session from storage...")
            context = await browser.new_context(
                storage_state=STATE_FILE,
                user_agent=UA,
                locale=LOCALE,
                timezone_id=TIMEZONE,
                viewport=VIEWPORT,
                device_scale_factor=1,
                extra_http_headers = EXTRA_HEADERS,
            )
            
            self.page = await context.new_page()
            
        else:
            print("Starting fresh session...")
            context = await browser.new_context(
                user_agent=UA,
                locale=LOCALE,
                timezone_id=TIMEZONE,
                viewport=VIEWPORT,
                device_scale_factor=1,
                extra_http_headers = EXTRA_HEADERS,
            )
            
            self.page = await browser.new_page()

        if STATE_FILE.exists():
            print("Loaded session from cookies.json...")
            await context.add_cookies(json.loads(Path(STATE_FILE).read_text())["cookies"])
            await self.page.goto(self.homepage, wait_until="domcontentloaded")
        else:
            print("Starting fresh login...")
            await self.page.goto(self.login_url, wait_until="domcontentloaded")
            if not await self.already_logged_in():
                if await self.login_(email=self.email, password=self.password):
                    await self.page.wait_for_timeout(self.time_sleep())
                    await self.dump_cookies()

        # -------- Login handling --------
        if not await self.step1_open_homepage():
            await self.login_(email=self.email, password=self.password)
            await self.page.wait_for_timeout(self.time_sleep())

        # -------- Steps after login --------
        
        await self.step2_click_name_search()
        await self.check_and_handle_announcement()
        await self.step3_fill_form()
        await self.check_and_handle_announcement()
        await self.step4_select_highest_occurs()
        await self.check_and_handle_announcement()
        await self.process_all_rp()
        await self.process_rp_detail()
        await self.search_real_estate(name="Gordon")
        await self.select_highest_occurs()
        await self.scrape_documents()
        self.save_to_excel()
        await self.check_and_handle_announcement()

        await browser.close()
        await playwright.stop()


async def main() -> None:
    scraper = GSCCCAScraper()
    await scraper.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        console.print("\n[bold yellow]Interrupted![/bold yellow]\n", traceback.format_exc())
    finally:
        console.print("[bold green]Exiting...[/bold green]")


