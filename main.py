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
        # 1. Dairy & Breakfast
        "Dairy_Breakfast": [
            "Milk", "Amul Butter", "Paneer", "Curd", "Eggs", "Bread", "Greek Yogurt", "Oats", "Muesli", 
            "Peanut Butter", "Cheese Slices", "Ghee", "Cream", "Condensed Milk", "Milk Powder", "Lassi", 
            "Buttermilk", "Mother Dairy Milk", "Amul Gold", "Toned Milk"
        ],
        
        # 2. Fruits & Vegetables
        "Fruits_Veg": [
            "Potato", "Onion", "Tomato", "Garlic", "Ginger", "Lemon", "Kashmiri Apple", "Banana", 
            "Dragon Fruit", "Broccoli", "Mushroom", "Spinach", "Carrot", "Cucumber", "Kiwi", 
            "Cabbage", "Cauliflower", "Brinjal", "Lady Finger", "Green Chilly", "Capsicum", "Sweet Corn"
        ],
        
        # 3. Electronics & Gadgets
        "Electronics_Gadgets": [
            "Type C Cable", "Fast Charger", "Earbuds", "Power Bank", "Smart Watch", "Batteries", 
            "AA Batteries", "iPhone Case", "Bluetooth Speaker", "Ring Light", "Mouse", "Keyboard"
        ],

        # 4. Beauty & Grooming
        "Beauty_Grooming": [
            "Sunscreen", "Serum", "Face Wash", "Moisturizer", "Beard Oil", "Shampoo", "Conditioner", 
            "Lip Balm", "Kajal", "Soap", "Deodorant", "Sanitary Pads", "Shaving Cream", "Hair Oil", 
            "Perfume", "Handwash", "Body Lotion"
        ],

        # 5. Health & Wellness
        "Health_Wellness": [
            "Protein Powder", "Vitamins", "ORS", "Green Tea", "Apple Cider Vinegar", "Honey", 
            "Chyawanprash", "Face Masks", "Creatine", "Peanut Butter High Protein", "Sugar Free", "Quinoa"
        ],

        # 6. Snacks & Munchies
        "Snacks_Munchies": [
            "Lays", "Kurkure", "Nachos", "Dairy Milk", "Maggi", "Pasta", "Ice Cream", "Cold Drink", 
            "Red Bull", "Party Soda", "Ice Cubes", "Chips", "Biscuits", "Chocolates", "Cookies", 
            "Namkeen", "Popcorn", "Doritos", "Haldirams", "Snickers", "Cadbury", "Oreo", "Britannia"
        ],

        # 7. Staples & Kitchen
        "Staples_Kitchen": [
            "Rice", "Wheat Flour", "Atta", "Toor Dal", "Moong Dal", "Sugar", "Salt", "Tea", 
            "Coffee", "Cooking Oil", "Basmati Rice", "Besan", "Maida", "Poha", "Daliya", 
            "Jaggery", "Olive Oil", "Soy Sauce", "Noodles", "Ketchup", "Mayonnaise", "Jam", "Vinegar"
        ],

        # 8. Household & Utility
        "Household": [
            "Surf Excel", "Vim Liquid", "Comfort", "Toilet Paper", "Garbage Bags", "Colin", 
            "Matchbox", "Agarbatti", "Mosquito Repellent", "Detergent", "Dish Soap", "Floor Cleaner", 
            "Napkins", "Foil", "Batteries", "Bulbs", "Lizol", "Ariel"
        ],

        # 9. Pharmacy & Specialized Health
        "Pharmacy_OTC": [
            "Digene Tablet", "Saridon", "Strepsils", "Eno Sachet", "Hansaplast", 
            "Pain Relief Patch", "BCAA Powder", "Mass Gainer", "Fish Oil Capsules", 
            "Multivitamin Men", "Multivitamin Women", "Pregnancy Test Kit", "Intimate Wash"
        ],

        # 10. Kitchen Tools & Dining
        "Kitchen_Dining_Tools": [
            "Chef Knife", "Non Stick Pan", "Glass Tumbler Set", "Plastic Containers", 
            "Dinner Plate Set", "Spatula set", "Chopping Board", "Lunch Box", 
            "Water Bottle 2L", "Steel Spoon Set", "Tea Strainer", "Ice Tray"
        ],

        # 11. Specialized Cleaning & Laundry
        "Specialized_Cleaning": [
            "Drain Cleaner", "Cockroach Spray", "Rat Trap", "Naphthalene Balls", 
            "Toilet Brush", "Microfiber Mop", "Vacuum Bag", "Descaling Powder", 
            "Stain Remover", "Ironing Spray", "Leather Polish", "Woolen Detergent"
        ],

        # 12. Electronics Accessories
        "Electronics_Expanded": [
            "HDMI Cable", "VGA Adapter", "Extension Cord 5m", "Universal Remote", 
            "WiFi Range Extender", "LED Strip Lights", "USB Hub", "Smartphone Tripod", 
            "Screen Guard", "OTG Adapter", "Smart Bulb", "Laptop Stand", "Webcam", 
            "Monitor Stand", "Wall Mount TV", "Smart Plug"
        ],

        # 13. Stationery, Art & DIY
        "Art_Stationery": [
            "Classmate Notebook", "A4 Printing Paper", "Gel Pen Pack", "Permanent Marker", 
            "Fevicol Stick", "Sticky Notes", "Highlighter Set", "Stapler", "Spiral Notebook", 
            "Calculator", "Whiteboard Marker", "Acrylic Paint Set", "Paint Brush Pack", 
            "Canvas Board", "Hot Glue Gun", "Scissor Set", "Glitter Glue", "Sketching Pencil Set", 
            "Modeling Clay", "Origami Paper", "Watercolor Set", "Sketchbook", "Geometry Box", 
            "Correction Tape", "Oil Pastels", "Calligraphy Pen"
        ],

        # 14. Hardware & Home Improvement
        "Hardware_Tools": [
            "Screwdriver Set", "Digital Weighing Scale", "Extension Cord", "Torch Light", 
            "Double Sided Tape", "Step Ladder", "Combination Plier", "Utility Knife", 
            "Super Glue", "M-Seal", "Measuring Tape", "Drill Machine", "Wall Hook", 
            "Curtain Brackets", "Door Handle", "Padlock", "WD-40", "Hammer", "Spirit Level"
        ],

        # 15. Home Furnishing & Decor
        "Home_Decor": [
            "Cotton Bedsheet", "Memory Foam Pillow", "Microfiber Towel", "Door Mat", 
            "Scented Candle Set", "Artificial Plants", "Cushion Cover", "Wall Clock", 
            "Flower Vase", "Curtain Rods", "Table Cloth"
        ],

        # 16. Sports & Fitness
        "Gym_Fitness": [
            "Yoga Mat", "Dumbbells 5kg", "Resistance Bands Set", "Skipping Rope", 
            "Hand Gripper", "Push Up Bar", "Badminton Shuttlecocks", "Tennis Ball", 
            "Cricket Bat", "Ab Roller", "Yoga Block", "Cycling Gloves", 
            "Protein Shaker Bottle", "Isotonic Drink", "Wrist Support"
        ],

        # 17. Gourmet Specialty
        "Gourmet_Specialty": [
            "Almond Milk Unsweetened", "Oat Milk", "Hazelnut Spread", "Chocolate Syrup", 
            "Pancake Mix", "Frozen Waffles", "Blueberries", "Avocado", "Tofu Extra Firm", 
            "Kombucha", "Matcha Powder", "Granola Bars", "Honey Nut Cornflakes"
        ],

        # 18. Puja Essentials
        "Puja_Essentials": [
            "Mangaldeep Agarbatti", "Camphor Tablets", "Cow Ghee Diya", "Cotton Batti", 
            "Puja Oil", "Dhoop Sticks", "Ganga Jal", "Rose Water", "Sandalwood Powder"
        ],

        # 19. Travel, Fashion & Auto
        "Travel_Fashion_Auto": [
            "Neck Pillow", "Umbrella Large", "Raincoat Men", "Trolley Bag Cover", 
            "TSA Padlock", "Portable Fan USB", "Eye Mask", "Leather Belt", 
            "Canvas Wallet", "Tote Bag", "Backpack for College", "Shoe Polish", 
            "Car Wash Shampoo", "Microfiber Cloth Car", "Car Air Freshener Gel", 
            "Chain Lube Bike", "Tyre Polish", "Dashboard Cleaner"
        ],

        # 20. Books & Hobbies
        "Books_Hobbies": [
            "Self Help Books", "Coloring Book for Kids", "Sudoku Book", "Uno Cards", 
            "Monopoly Game", "Building Blocks", "Puzzle 500 Pieces", "Fountain Pen Ink", 
            "Magnifying Glass", "Flash Cards"
        ],

        # 21. Festive & Party
        "Festive_Party": [
            "Birthday Candles", "Party Poppers", "Balloons", "Birthday Sash", "Paper Plates", 
            "Disposable Cups", "Festive Diya", "Holi Colors", "Rakhi", "Diwali Gift Hamper", 
            "Christmas Decor", "New Year Party Hats", "Seasonal Umbrella"
        ],

        # 22. Meat & Seafood
        "Meat_Seafood": [
            "Chicken Breast", "Mutton Curry Cut", "Prawns Frozen", "Basa Fillet", "Rohu Fish", 
            "Chicken Sausages", "Salami", "Plant-based Nuggets", "Soya Chaap", "Smoked Salmon", "Pork Pepperoni"
        ],

        # 23. Gardening & Plant Care
        "Gardening": [
            "Potting Soil", "Organic Vermicompost", "Plant Seeds", "Spray Bottle for Plants", 
            "Ceramic Pots", "Gardening Gloves", "Neem Oil Spray", "Flower Food", "Decorative Pebbles"
        ],

        # 24. Ethnic Breakfast & Instant Meals
        "Ethnic_Meals": [
            "Instant Poha Mix", "Ready-to-eat Upma", "Idli Dosa Batter", "Malabar Paratha", 
            "Medu Vada Batter", "Ready-to-eat Rajma Chawal", "Palak Paneer", "Instant Pasta Bowls"
        ],

        # 25. Toys & Parenting
        "Toys_Parenting": [
            "Hot Wheels Cars", "Barbie Dolls", "Slime Kit", "Playing Cards", "Bubble Maker", 
            "Rubik's Cube", "Action Figures", "Board Games", "Small Plush Toys"
        ],

        # 26. Pet & Baby Care
        "Pet_Baby_Care": [
            "Dog Food", "Cat Food", "Pedigree", "Whiskas", "Pet Wipes", "Pet Treats",
            "Diapers", "Baby Wipes", "Baby Food", "Cerelac", "Baby Lotion", "Baby Shampoo", 
            "Pampers", "Huggies", "MamyPoko"
        ],

        # 27. Bakery & International
        "Bakery_International": [
            "Croissant", "Brownie", "Cheese Cake", "Sourdough", "Gluten Free Bread", "Bun", "Rusk", 
            "Cake", "Muffin", "Donut", "Pav", "White Bread", "Brown Bread", "Atta Bread", 
            "Schezwan Sauce", "Pizza Sauce", "Pasta Sauce"
        ]
    }
    
    all_keywords = []
    for cat, items in categories.items():
        all_keywords.extend(items)
        # Expansion logic for deep search coverage
        for item in items:
            all_keywords.append(f"{item} pack")
            all_keywords.append(f"{item} discount")
            
    unique_list = list(dict.fromkeys(all_keywords))
    return unique_list

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
