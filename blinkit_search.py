import time
import json
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from curl_cffi import requests
import polars as pl
import duckdb

class BlinkitSessionManager:
    def __init__(self):
        self.url = "https://blinkit.com"
        self.auth_key = None
        self.cookies = {}
        self.meta = {}
        self.refresh_session()

    def refresh_session(self):
        print("\n[Self-Healing] Refreshing session via Playwright...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                
                def intercept_request(request):
                    if "auth_key" in request.headers:
                        self.auth_key = request.headers["auth_key"]
                        for key in ["lat", "lon", "device_id", "app_version", "app_client"]:
                            if key in request.headers:
                                self.meta[key] = request.headers[key]

                page.on("request", intercept_request)
                page.goto("https://blinkit.com/s/?q=milk", wait_until="networkidle")
                time.sleep(10)
                
                pw_cookies = context.cookies()
                self.cookies = {c['name']: c['value'] for c in pw_cookies}
                browser.close()
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

class BlinkitDataPipeline:
    def __init__(self, db_path="blinkit_data.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        # Ensure the DuckDB table exists
        conn = duckdb.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                product_id BIGINT,
                product_name VARCHAR,
                brand VARCHAR,
                price FLOAT,
                mrp FLOAT,
                inventory INTEGER,
                is_sold_out BOOLEAN,
                scraped_at TIMESTAMP,
                keyword VARCHAR
            )
        """)
        conn.close()

    def process_json(self, json_data, keyword):
        """Extract and flatten product data using Polars"""
        products = []
        scraped_at = datetime.now().isoformat()
        
        if not json_data or "response" not in json_data:
            return None

        snippets = json_data["response"].get("snippets", [])
        for snippet in snippets:
            if snippet.get("widget_type") == "product_card_snippet_type_2":
                data = snippet.get("data", {})
                atc_action = data.get("atc_action", {}).get("add_to_cart", {})
                cart_item = atc_action.get("cart_item", {})
                
                if cart_item:
                    products.append({
                        "product_id": cart_item.get("product_id"),
                        "product_name": cart_item.get("product_name"),
                        "brand": cart_item.get("brand"),
                        "price": float(cart_item.get("price", 0)),
                        "mrp": float(cart_item.get("mrp", 0)),
                        "inventory": cart_item.get("inventory"),
                        "is_sold_out": data.get("is_sold_out", False),
                        "scraped_at": scraped_at,
                        "keyword": keyword
                    })

        if not products:
            return None

        df = pl.DataFrame(products)
        # Ensure correct types
        df = df.with_columns([
            pl.col("scraped_at").str.to_datetime(),
            pl.col("product_id").cast(pl.Int64)
        ])
        return df

    def save_to_duckdb(self, df):
        """Append Polars DataFrame to DuckDB"""
        if df is None or df.is_empty():
            return
        
        conn = duckdb.connect(self.db_path)
        # DuckDB can register a Polars DataFrame and append directly
        conn.execute("INSERT INTO price_history SELECT * FROM df")
        conn.close()
        print(f"Logged {len(df)} products to DuckDB.")

    def export_to_csv(self, filename="blinkit_export.csv"):
        """Export full history to CSV from DuckDB"""
        conn = duckdb.connect(self.db_path)
        conn.execute(f"COPY price_history TO '{filename}' (HEADER, DELIMITER ',')")
        conn.close()
        print(f"Exported all data to {filename}")

def fetch_blinkit_search(session_mgr, query, retry=True):
    url = "https://blinkit.com/v1/layout/search"
    params = {"q": query, "search_type": "type_to_search"}
    
    try:
        response = requests.post(
            url,
            params=params,
            headers=session_mgr.get_headers(query),
            cookies=session_mgr.cookies,
            data='{}',
            impersonate="chrome110",
            timeout=20
        )
        
        if response.status_code in [401, 403] and retry:
            print(f"Received {response.status_code}. Refreshing...")
            session_mgr.refresh_session()
            return fetch_blinkit_search(session_mgr, query, retry=False)
            
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Fetch error for {query}: {e}")
        return None

def test_pipeline():
    print("--- Starting Test Pipeline ---")
    session_mgr = BlinkitSessionManager()
    pipeline = BlinkitDataPipeline()
    test_keywords = ['Milk', 'Onions', 'Chips']
    
    for kw in test_keywords:
        print(f"Processing: {kw}")
        json_data = fetch_blinkit_search(session_mgr, kw)
        df = pipeline.process_json(json_data, kw)
        if df is not None:
            pipeline.save_to_duckdb(df)
        time.sleep(2)
    
    pipeline.export_to_csv("test_export.csv")
    print("--- Test Pipeline Finished ---")

def main():
    # To run the full 100-keyword scrape, you could call a separate function.
    # For now, let's run the requested test_pipeline.
    test_pipeline()

if __name__ == "__main__":
    main()
