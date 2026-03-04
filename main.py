import asyncio
import time
import os
import random
import polars as pl
from datetime import datetime
from src.scraper.session import AsyncSessionManager
from src.scraper.client import fetch_blinkit_search
from src.pipeline.processor import DataProcessor
from src.database.db import DatabaseManager

# --- 1,000 Keyword Generation ---
def get_production_keywords():
    categories = {
        # High Volatility: Prices and stock fluctuate hourly
        "Dairy_Breakfast": [
            "Milk", "Amul Butter", "Paneer", "Curd", "Eggs", "Bread", "Greek Yogurt", "Oats", "Muesli", 
            "Peanut Butter", "Cheese Slices", "Ghee", "Cream", "Condensed Milk", "Milk Powder", "Lassi", 
            "Buttermilk", "Mother Dairy Milk", "Amul Gold", "Toned Milk"
        ],
        
        # Freshness Factor: Critical for "Demand Forecasting" ML models
        "Fruits_Veg": [
            "Potato", "Onion", "Tomato", "Garlic", "Ginger", "Lemon", "Kashmiri Apple", "Banana", 
            "Dragon Fruit", "Broccoli", "Mushroom", "Spinach", "Carrot", "Cucumber", "Kiwi", 
            "Cabbage", "Cauliflower", "Brinjal", "Lady Finger", "Green Chilly", "Capsicum", "Sweet Corn"
        ],
        
        # High Margin/High Value: Essential for pricing strategy analysis
        "Electronics_Gadgets": [
            "Type C Cable", "Fast Charger", "Earbuds", "Power Bank", "Smart Watch", "Batteries", 
            "AA Batteries", "iPhone Case", "Bluetooth Speaker", "Ring Light", "Mouse", "Keyboard"
        ],
        
        # Beauty & Grooming: High brand competition
        "Beauty_Grooming": [
            "Sunscreen", "Serum", "Face Wash", "Moisturizer", "Beard Oil", "Shampoo", "Conditioner", 
            "Lip Balm", "Kajal", "Soap", "Deodorant", "Sanitary Pads", "Shaving Cream", "Hair Oil", 
            "Perfume", "Handwash", "Body Lotion"
        ],
        
        # Trending 2026: Health & Wellness (Premium Pricing)
        "Health_Wellness": [
            "Protein Powder", "Vitamins", "ORS", "Green Tea", "Apple Cider Vinegar", "Honey", 
            "Chyawanprash", "Face Masks", "Creatine", "Peanut Butter High Protein", "Sugar Free", "Quinoa"
        ],
        
        # Impulse & Party: Peak demand spikes on weekends
        "Snacks_Munchies": [
            "Lays", "Kurkure", "Nachos", "Dairy Milk", "Maggi", "Pasta", "Ice Cream", "Cold Drink", 
            "Red Bull", "Party Soda", "Ice Cubes", "Chips", "Biscuits", "Chocolates", "Cookies", 
            "Namkeen", "Popcorn", "Doritos", "Haldirams", "Snickers", "Cadbury", "Oreo", "Britannia"
        ],
        
        # Staples & Kitchen: The "Anchor" data for inflation tracking
        "Staples_Kitchen": [
            "Rice", "Wheat Flour", "Atta", "Toor Dal", "Moong Dal", "Sugar", "Salt", "Tea", 
            "Coffee", "Cooking Oil", "Basmati Rice", "Besan", "Maida", "Poha", "Daliya", 
            "Jaggery", "Olive Oil", "Soy Sauce", "Noodles", "Ketchup", "Mayonnaise", "Jam", "Vinegar"
        ],
        
        # Household & Utility: Constant demand
        "Household": [
            "Surf Excel", "Vim Liquid", "Comfort", "Toilet Paper", "Garbage Bags", "Colin", 
            "Matchbox", "Agarbatti", "Mosquito Repellent", "Detergent", "Dish Soap", "Floor Cleaner", 
            "Napkins", "Foil", "Batteries", "Bulbs", "Lizol", "Ariel"
        ],
        
        # Premium & International: Niche price behavior
        "Bakery_International": [
            "Croissant", "Brownie", "Cheese Cake", "Sourdough", "Gluten Free Bread", "Bun", "Rusk", 
            "Cake", "Muffin", "Donut", "Pav", "White Bread", "Brown Bread", "Atta Bread", 
            "Schezwan Sauce", "Pizza Sauce", "Pasta Sauce"
        ],
        
        # Pet & Baby Care: Brand Loyalty models
        "Pet_Baby_Care": [
            "Dog Food", "Cat Food", "Pedigree", "Whiskas", "Pet Wipes", "Pet Treats",
            "Diapers", "Baby Wipes", "Baby Food", "Cerelac", "Baby Lotion", "Baby Shampoo", 
            "Pampers", "Huggies", "MamyPoko"
        ]
    }
    
    all_keywords = []
    for cat, items in categories.items():
        all_keywords.extend(items)
        # Add varied search terms for each item to reach 1000
        for item in items:
            all_keywords.append(f"{item} 1kg")
            all_keywords.append(f"{item} pack")
            all_keywords.append(f"{item} discount")
            
    unique_list = list(dict.fromkeys(all_keywords))
    while len(unique_list) < 1000:
        # Pad with placeholders if needed, but the above logic should get us close
        unique_list.append(f"Top Product {len(unique_list)}")
        
    return unique_list[:1000]

async def process_keyword(kw, session_mgr, processor, db_mgr, semaphore):
    async with semaphore:
        # Intra-batch jitter to spread requests
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        # 1. Scrape
        json_data = await fetch_blinkit_search(session_mgr, kw)
        
        # 2. Extract
        if json_data:
            df = processor.process_json(json_data, kw)
            return df
        return None

async def run_production_pipeline(keywords=None):
    if keywords is None:
        keywords = get_production_keywords()
    export_filename = "data/blinkit_final_extraction.csv"
    progress_log = "data/progress.log"
    batch_size = 50
    concurrency_limit = 1 # Strictly 1 for max stealth
    
    # Initialize
    session_mgr = AsyncSessionManager()
    await session_mgr.refresh_session() # Initial login
    
    processor = DataProcessor()
    db_mgr = DatabaseManager()
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    print(f"\n--- Production Run: 1,000 Keywords (Concurrency: {concurrency_limit}) ---")
    
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i+batch_size]
        print(f"\n[Batch] Processing {i+1} to {min(i+batch_size, 1000)}...")
        
        tasks = [process_keyword(kw, session_mgr, processor, db_mgr, semaphore) for kw in batch]
        results = await asyncio.gather(*tasks)
        
        # Filter Nones and Save Batch
        valid_dfs = [df for df in results if df is not None and not df.is_empty()]
        if valid_dfs:
            combined_df = pl.concat(valid_dfs)
            db_mgr.save_dataframe(combined_df)
        
        # Checkpointing & Logging
        current_rows = db_mgr.get_row_count()
        log_msg = f"[{datetime.now().isoformat()}] Batch {i//batch_size + 1} Done. Keywords: {min(i+batch_size, 1000)}. Total Rows in DB: {current_rows}\n"
        
        with open(progress_log, "a") as f:
            f.write(log_msg)
        
        print(log_msg.strip())
        
        # Politeness pause between batches with randomized jitter: 12 - 20s
        await asyncio.sleep(random.uniform(12, 20))
    
    # Final Export
    db_mgr.export_csv(export_filename)
    print(f"\n--- MISSION COMPLETE ---")
    print(f"Database: data/blinkit_data.db")
    print(f"Snapshot: {export_filename}")
    print(f"Progress Log: {progress_log}")

if __name__ == "__main__":
    asyncio.run(run_production_pipeline())
