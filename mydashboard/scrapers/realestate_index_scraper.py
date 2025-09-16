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
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw
from datetime import datetime

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

# ---------- Scraper Class -----------------------------------------------------

class RealestateIndexScraper:
    def __init__(self) -> None:
        self.page = None
        self.email = TAX_EMAIL
        self.password = TAX_PASSWORD
        self.realestate_url = "https://search.gsccca.org/RealEstate/namesearch.asp"
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.results = []

    def time_sleep(self, a=1.5, b=3.0) -> float:
        return random.uniform(a, b)

    async def login(self) -> bool:
        await self.page.goto("https://apps.gsccca.org/login.asp", wait_until="domcontentloaded", timeout=60000)
        await self.page.wait_for_timeout(self.time_sleep())
        await self.check_and_handle_announcement()  
        await self.page.fill("input[name='txtUserID']", self.email)
        await self.page.fill("input[name='txtPassword']", self.password)
        try:
            await self.page.click("img[name='logon']")
        except:
            await self.page.evaluate("document.forms['frmLogin'].submit()")
        await self.page.wait_for_load_state("networkidle", timeout=15000)
        if await self.page.query_selector("a:has-text('Logout')"):
            console.print("[green]Login successful[/green]")
            state = await self.page.context.storage_state()
            Path(STATE_FILE).write_text(json.dumps(state, indent=2))
            return True
        return False

    async def check_and_handle_announcement(self):
        if "CustomerCommunicationApiAnnouncement1.asp" in self.page.url:
            console.print("[yellow]Dismissing announcement[/yellow]")
            await self.page.select_option("#Options", "dismiss")
            await self.page.wait_for_timeout(1500)
            await self.page.click("input[name='Continue']")
            await self.page.wait_for_timeout(2000)

    async def step2_open_realestate_search(self):
        console.print("[cyan]Opening Real Estate Name Search page[/cyan]")
        await self.page.goto(self.realestate_url, wait_until="domcontentloaded", timeout=60000)
        await self.check_and_handle_announcement()
        await self.page.wait_for_timeout(self.time_sleep())

    async def step3_fill_form_from_excel(self):
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

    async def step4_select_names_and_display(self, search_name: str):
        try:
            radios = await self.page.query_selector_all("input[name='rdoEntityName']")
            console.print(f"[cyan]Found {len(radios)} entity names for {search_name}[/cyan]")

            for i in range(len(radios)):
                current_radios = await self.page.query_selector_all("input[name='rdoEntityName']")
                radio = current_radios[i]

                await radio.click()
                await self.page.click("#btnDisplayDetails")
                await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                await asyncio.sleep(self.time_sleep())

                console.print(f"[blue]Opened Display Details for entity {i+1}[/blue]")

                await self.step5_parse_documents(search_name, i+1)

                await self.page.go_back()
                await self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                await asyncio.sleep(self.time_sleep())

        except Exception as e:
            console.print(f"[red]Step 4 error: {e}[/red]")
            traceback.print_exc()

    async def step5_parse_documents(self, search_name: str, entity_idx: int):
        try:
            links = await self.page.query_selector_all("a[href*='final.asp']")
            console.print(f"[cyan]Found {len(links)} GE/GR document links[/cyan]")

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
            
            details_url = self.page.url

            for i in range(len(hrefs)):
                pdf_url = hrefs[i]
                console.print(f"[green]Opening doc {i+1}: {pdf_url}[/green]")
                await self.page.goto(pdf_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(self.time_sleep())

                view_button = await self.page.query_selector("input[value='View Image']")
                if view_button:
                    console.print(f"[blue]Clicking 'View Image' button...[/blue]")

                    async with self.page.context.expect_page() as popup_info:
                        await view_button.click()

                    popup = await popup_info.value
                    await popup.wait_for_load_state("domcontentloaded")
                    await asyncio.sleep(self.time_sleep())

                    pdf_viewer_url = popup.url
                    os.makedirs('screenshots', exist_ok=True)
                    await popup.wait_for_selector('canvas', timeout=15000)

                    canvas_data_url = await popup.evaluate("""
                        () => {
                            const canvas = document.querySelector('canvas');
                            return canvas ? canvas.toDataURL('image/png') : null;
                        }
                    """)

                    if canvas_data_url:
                        base64_image = canvas_data_url.split(',')[1]
                        screenshot_path = f"screenshots/pdf_{random.randint(1000,9999)}.png"
                        with open(screenshot_path, "wb") as f:
                            f.write(base64.b64decode(base64_image))
                        console.print(f"[green]Canvas screenshot saved at {screenshot_path}[/green]")
                    else:
                        console.print("[red]Canvas not found! Saving fallback data.[/red]")
                        screenshot_path = "N/A"

                    self.results.append({
                        "search_name": search_name,
                        "entity_index": entity_idx,
                        "doc_index": i+1,
                        "final_url": pdf_url,
                        "pdf_viewer": pdf_viewer_url,
                        "screenshot": screenshot_path
                    })
                    await popup.close()
                else:
                    console.print(f"[red]View Image button not found! Saving fallback data.[/red]")
                    self.results.append({
                        "search_name": search_name,
                        "entity_index": entity_idx,
                        "doc_index": i+1,
                        "final_url": pdf_url,
                        "pdf_viewer": self.page.url,
                        "screenshot": "N/A"
                    })
                
                await self.page.goto(details_url, wait_until="domcontentloaded")
                await asyncio.sleep(self.time_sleep())

        except Exception as e:
            console.print(f"[red]Step 5 error: {e}[/red]")
            traceback.print_exc()

    def save_results_to_excel(self, filename="realestate_index.xlsx"):
        if not self.results:
            console.print("[red]No results to save[/red]")
            return

        try:
            df = pd.DataFrame(self.results)
            df.drop_duplicates(subset=["pdf_viewer"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            # Create Output directory if it doesn't exist
            output_dir = Path("Output")
            output_dir.mkdir(exist_ok=True)
            
            # Add timestamp to filename
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

        console.print(f"[green]Final Results:[/green] {json.dumps(self.results, indent=2)}")
        excel_path = self.save_results_to_excel()
        if excel_path:
            console.print(f"[green]Excel file created at: {excel_path}[/green]")
        else:
            console.print("[red]Failed to create Excel file[/red]")

        await browser.close()
        await playwright.stop()


async def main():
    scraper = RealestateIndexScraper()
    try:
        await scraper.run()
    except Exception:
        console.print("\n[bold yellow]Interrupted![/bold yellow]\n", traceback.format_exc())
    finally:
        scraper.save_results_to_excel()
        console.print("[bold green]Exiting...[/bold green]")

if __name__ == "__main__":
    asyncio.run(main())