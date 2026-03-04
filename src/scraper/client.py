from curl_cffi import requests
import asyncio
import random

async def fetch_blinkit_search(session_mgr, query, retry_count=0):
    url = "https://blinkit.com/v1/layout/search"
    params = {"q": query, "search_type": "type_to_search"}
    
    if retry_count > 3:
        print(f"Max retries reached for {query}. Skipping.")
        return None

    # Use AsyncSession for concurrent requests
    async with requests.AsyncSession() as s:
        try:
            response = await s.post(
                url,
                params=params,
                headers=session_mgr.get_headers(query),
                cookies=session_mgr.cookies,
                data='{}',
                impersonate="chrome110",
                timeout=20
            )
            
            # 429 Error: Backoff and Refresh
            if response.status_code == 429:
                wait_time = random.uniform(45, 75)
                print(f"!!! [429 Throttled] Waiting {wait_time:.1f}s and refreshing session...")
                await asyncio.sleep(wait_time)
                await session_mgr.refresh_session()
                return await fetch_blinkit_search(session_mgr, query, retry_count + 1)

            # 401/403 Error: Simple Refresh
            if response.status_code in [401, 403]:
                print(f"Received {response.status_code}. Refreshing session...")
                await session_mgr.refresh_session()
                return await fetch_blinkit_search(session_mgr, query, retry_count + 1)
                
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Fetch error for {query}: {e}")
            return None
