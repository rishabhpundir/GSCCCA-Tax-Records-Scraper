from __future__ import annotations

import os
import re
import json
import math
import random
import asyncio
import traceback
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
import playwright.async_api as pw
from typing import Tuple, Optional, Any, Dict, Optional
from playwright.async_api import TimeoutError as PlaywrightTimeoutError


load_dotenv()
console = Console()


# ---------- config -------------------------------------------------------------
HEADLESS = False 
STATE_FILE = Path("cookies.json")

FB_EMAIL = os.getenv("USERNAME")
FB_PASSWORD = os.getenv("PASSWORD")

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
        self.email = FB_EMAIL
        self.password = FB_PASSWORD
        self.homepage = "https://search.gsccca.org/Lien/namesearch.asp"
        self.login_url = "https://apps.gsccca.org/login.asp"
        
    
    def time_sleep(self, a: int = 2500, b: int = 5000) -> None:
        return random.uniform(a, b)
    
    
    async def already_logged_in(self) -> bool:
        """Checks for an existing Facebook session"""
        try:
            pass
        except PlaywrightTimeoutError:
            return False
                    

    async def login_(self, email: str, password: str) -> bool:
        pass

    
    async def dump_cookies(self, out_file="cookies.json"):
        # save FULL storage state (cookies + local/session storage)
        state = await self.page.context.storage_state()
        Path(out_file).write_text(json.dumps(state, indent=2))
        print(f"Saved storage state to --> {out_file}")
        

    async def scrape(self, page_url: str) -> Dict[str, Any]:
        """Open *url* in the current page and return latest post data."""
        await self.page.goto("https://google.com/", wait_until="domcontentloaded", timeout=60_000)
        await self.page.wait_for_timeout(3000)
        # *** continue ***


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

        if not STATE_FILE.exists():
            await self.page.goto(f"https://google.com", wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)
            
            await self.page.goto(f"https://fingerprint-scan.com", wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)
            breakpoint()
            
        if not await self.already_logged_in():
            await self.login_(email=self.email, password=self.password)
            await self.page.wait_for_timeout(self.time_sleep())
            await self.dump_cookies()
        
        # *** continue ***÷
            
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


