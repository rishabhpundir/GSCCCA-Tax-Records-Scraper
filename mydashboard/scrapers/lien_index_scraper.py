from __future__ import annotations

import os
import io
import re
import json
import math
import random
import asyncio
import traceback
import pandas as pd
import requests
import openpyxl
from openpyxl.styles import Font
import img2pdf
import aiohttp
import ssl
import certifi
import pytesseract
import cv2
import numpy as np
from datetime import datetime


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

if os.name == "nt":  # Windows
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


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
TOTAL_LINE_REGEX = re.compile(r'(TOTAL\s*DUE|TOTALDUE)', re.I)

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
        self.downloads_dir = "downloads"

        # SSL Context for aiohttp
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        


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
        await self.page.wait_for_timeout(2000) 

        print("[LOGIN] Checking 'Remember login details' checkbox if not already checked...")
        checkbox = await self.page.query_selector("input[type='checkbox'][name='permanent']")
        if checkbox:
            is_checked = await checkbox.is_checked()
            if not is_checked:
                await checkbox.click()
                print("[LOGIN] Checkbox clicked.")
            else:
                print("[LOGIN] Checkbox already checked.")
        else:
            print("[LOGIN] Checkbox not found on the page.")

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

    async def process_rp_details(self):
        """ Step 5: Process all RP buttons, extract data and save """
        self.results = []
        visited_pages = set()

        while True:
            # Unique marker: first RP link href
            rp_links = await self.page.query_selector_all("a[href*='lienfinal']")
            if not rp_links:
                print("[WARNING] No RP buttons found on this page")
                break

            first_link = await rp_links[0].get_attribute("href")
            if first_link in visited_pages:
                print(f"[INFO] Duplicate page detected → already visited. Stopping loop.")
                break
            visited_pages.add(first_link)

            total = len(rp_links)
            print(f"[INFO] Found {total} RP buttons on this page")

            for i in range(min(2, total)):  
                try:
                    rp_links = await self.page.query_selector_all("a[href*='lienfinal']")
                    if i >= len(rp_links):
                        continue

                    link = rp_links[i]

                    # Click with retry
                    retries = 3
                    for attempt in range(retries):
                        try:
                            await link.click()
                            await self.page.wait_for_load_state("domcontentloaded")
                            break
                        except Exception as e:
                            print(f"[ERROR] Click failed (Attempt {attempt+1}/{retries}): {e}")
                            if attempt == retries - 1:
                                raise
                            await asyncio.sleep(3)

                    data = await self.parse_rp_detail()
                    if data:
                        self.results.append(data)
                        print(f"[SUCCESS] Saved RP index {i+1} → "
                              f"{data.get('Name Selected','N/A')} | "
                              f"Book={data.get('Book','')} Page={data.get('Page','')}")
                        
                        # pd.DataFrame(self.results).to_excel("LienResults.xlsx", index=False)

                    # back
                    await asyncio.sleep(2)
                    back_btn = await self.page.query_selector("input[name='bBack']")
                    if back_btn:
                        await back_btn.click()
                        await self.page.wait_for_load_state("domcontentloaded")
                    else:
                        await self.page.go_back()
                        await self.page.wait_for_load_state("domcontentloaded")

                except Exception as e:
                    print(f"[ERROR] Failed at RP index {i}: {e}")
                    try:
                        await self.page.go_back()
                        await self.page.wait_for_load_state("domcontentloaded")
                    except:
                        pass
                    continue

            # Pagination: look for "Next" explicitly
            next_page = await self.page.query_selector("a:has-text('Next')")
            if next_page:
                print("[INFO] Going to next page...")
                try:
                    await next_page.click()
                    await self.page.wait_for_load_state("domcontentloaded")
                except Exception as e:
                    print(f"[ERROR] Pagination failed: {e}")
                    break
            else:
                print("[INFO] No more pages found.")
                break

        self.save_to_excel("LienResults.xlsx")


    

    def remove_table_lines(self, bw_inv: np.ndarray) -> np.ndarray:
        h, w = bw_inv.shape[:2]
        h_len = max(20, w // 30)
        v_len = max(20, h // 30)
        h_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (h_len, 1))
        v_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, v_len))
        horiz = cv2.morphologyEx(bw_inv, cv2.MORPH_OPEN, h_kernel, iterations=1)
        vert  = cv2.morphologyEx(bw_inv, cv2.MORPH_OPEN, v_kernel, iterations=1)
        grid  = cv2.bitwise_or(horiz, vert)
        return cv2.bitwise_and(bw_inv, cv2.bitwise_not(grid))

    def preprocess_page(self,pil_img: Image.Image) -> np.ndarray:
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

    def find_total_due_line(self, proc_img: np.ndarray) -> str | None:
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
    
    
    def extract_total_due(self, img):          
        pil = img.convert("RGB")
        
        proc = self.preprocess_page(pil)
        line = self.find_total_due_line(proc)
        if line:
            m = re.search(r'(?<!\d)(\d{1,3}(?:,\d{3})*(?:\.\d{2})|\d+\.\d{2})(?!\d)', line)
            line = m.group(1).replace(',', '') if m else None
        line = line if line else "Not found"
        print(f"Total Due: {line}")
        return line


    async def parse_rp_detail(self):
        """ Helper: Parse lienfinal.asp detail page with BeautifulSoup + Viewer URL + Single Page PDF + OCR + Address1/2 + Zipcode1/2 """
        await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
        await asyncio.sleep(1.5)
        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")
        data = {}

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

                download_dir = "downloads"
                os.makedirs(download_dir, exist_ok=True)
                pdf_path = os.path.join(download_dir, pdf_name)

                try:
                    popup = await self.page.context.new_page()
                    await popup.goto(viewer_url, timeout=50000)
                    await popup.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(3)  # let canvas render

                    # Actual single page from canvas
                    await popup.wait_for_selector("div.vtm_imageClipper canvas", timeout=10000)
                    canvas = await popup.query_selector("div.vtm_imageClipper canvas")

                    if canvas:
                        tmp_img = os.path.join(download_dir, f"tmp_{page_num}.png")
                        await canvas.screenshot(path=tmp_img)

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
                        print("[WARNING] No canvas found in popup")
                        data["PDF"] = ""
                        data["OCR_Text"] = ""
                        data["Address"] = ""
                        data["Zipcode"] = ""
                        data["Total Due"] = ""

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
                data["Address1"] = ""
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


    def save_to_excel(self, filename="LienResults.xlsx"):
        """ Save scraped results to Excel with clickable PDF links. """

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

        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, "Output")
        os.makedirs(output_dir, exist_ok=True)  

        if "PDF" in df.columns:
            df["PDF"] = df["PDF"].apply(
                lambda x: f'=HYPERLINK("file:///{os.path.join(self.downloads_dir, x).replace(os.sep, "/")}", "{x}")'
                if isinstance(x, str) and x.strip() else ""
            )

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(filename)
        final_filename = f"{base}_{ts}{ext}"
        
        output_dir = Path("Output")
        output_dir.mkdir(exist_ok=True)
        final_path = os.path.join(output_dir, final_filename)

        with pd.ExcelWriter(final_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

        print(f"[INFO] Saved {len(df)} records to {final_path}")
        
        

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
        """initialize playwright browser and run scraper"""
        playwright = await pw.async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=HEADLESS, 
            channel="chrome",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-maximized",
            ]
        )

        if STATE_FILE.exists():
            print("Loaded session from storage...")
            context = await browser.new_context(
                storage_state=STATE_FILE,
                user_agent=UA,
                locale=LOCALE,
                timezone_id=TIMEZONE,
                viewport=VIEWPORT,
                device_scale_factor=1,
                extra_http_headers=EXTRA_HEADERS,
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
                extra_http_headers=EXTRA_HEADERS,
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

        # -------- Steps after login --------
        if not await self.step1_open_homepage():
            await self.login_(email=self.email, password=self.password)
            await self.page.wait_for_timeout(self.time_sleep())

        await self.step2_click_name_search()
        await self.check_and_handle_announcement()
        await self.step3_fill_form()
        await self.check_and_handle_announcement()
        await self.step4_select_highest_occurs()
        await self.check_and_handle_announcement()
        await self.process_rp_details()  
        await self.check_and_handle_announcement()
        self.save_to_excel()
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


