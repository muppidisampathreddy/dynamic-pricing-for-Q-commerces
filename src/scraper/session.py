import asyncio
import time
import re
import random
from playwright.async_api import async_playwright

class AsyncSessionManager:
    def __init__(self):
        self.url = "https://blinkit.com"
        self.auth_key = None
        self.cookies = {}
        self.meta = {}
        self.lock = asyncio.Lock()

    async def refresh_session(self):
        async with self.lock:
            print("\n[Self-Healing] Refreshing session via Async Playwright...")
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
                    )
                    page = await context.new_page()
                    
                    def intercept_request(request):
                        if "auth_key" in request.headers:
                            self.auth_key = request.headers["auth_key"]
                            for key in ["lat", "lon", "device_id", "app_version", "app_client"]:
                                if key in request.headers:
                                    self.meta[key] = request.headers[key]

                    page.on("request", intercept_request)
                    await page.goto("https://blinkit.com/s/?q=milk", wait_until="networkidle")
                    # Randomized jitter: 8-12 seconds
                    await asyncio.sleep(random.uniform(8, 12))
                    
                    pw_cookies = await context.cookies()
                    self.cookies = {c['name']: c['value'] for c in pw_cookies}
                    await browser.close()
            except Exception as e:
                print(f"Playwright Refresh Failed: {e}")
            
            print(f"Session Refreshed. Auth Key: {str(self.auth_key)[:10]}...")

    def get_headers(self, query=""):
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "app_client": self.meta.get("app_client", "consumer_web"),
            "app_version": self.meta.get("app_version", "1010101010"),
            "auth_key": str(self.auth_key),
            "access_token": "null",
            "content-type": "application/json",
            "device_id": self.meta.get("device_id", "534296b3c1b246b3"),
            "lat": self.meta.get("lat", "17.4657191"),
            "lon": self.meta.get("lon", "78.3511035"),
            "origin": "https://blinkit.com",
            "referer": f"https://blinkit.com/s/?q={query or 'milk'}",
        }
