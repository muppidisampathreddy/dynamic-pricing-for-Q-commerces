from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
CLEAN_DATA_DIR = DATA_DIR / "clean"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

for d in [CLEAN_DATA_DIR, MODELS_DIR, FIGURES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

REQUIRED_COLUMNS = [
    "scraped_at", "keyword", "product_id", "product_name",
    "price", "mrp", "unit_value", "unit_type", "inventory",
]
OPTIONAL_COLUMNS = ["merchant_id", "brand", "rating", "unavail_qty"]

HIGH_DISCOUNT_THRESHOLD_PCT = 15.0
RARE_UNIT_TYPE_MIN_COUNT = 50
DEFAULT_N_CLUSTERS = 4
RANDOM_STATE = 42

CATEGORY_MAP = {
    "Dairy_Breakfast": [
        "Milk", "Amul Butter", "Paneer", "Curd", "Eggs", "Bread", "Greek Yogurt", "Oats", "Muesli",
        "Peanut Butter", "Cheese Slices", "Ghee", "Cream", "Condensed Milk", "Milk Powder", "Lassi",
        "Buttermilk", "Mother Dairy Milk", "Amul Gold", "Toned Milk",
    ],
    "Fruits_Veg": [
        "Potato", "Onion", "Tomato", "Garlic", "Ginger", "Lemon", "Kashmiri Apple", "Banana",
        "Dragon Fruit", "Broccoli", "Mushroom", "Spinach", "Carrot", "Cucumber", "Kiwi",
        "Cabbage", "Cauliflower", "Brinjal", "Lady Finger", "Green Chilly", "Capsicum", "Sweet Corn",
    ],
    "Electronics_Gadgets": [
        "Type C Cable", "Fast Charger", "Earbuds", "Power Bank", "Smart Watch", "Batteries",
        "AA Batteries", "iPhone Case", "Bluetooth Speaker", "Ring Light", "Mouse", "Keyboard",
    ],
    "Beauty_Grooming": [
        "Sunscreen", "Serum", "Face Wash", "Moisturizer", "Beard Oil", "Shampoo", "Conditioner",
        "Lip Balm", "Kajal", "Soap", "Deodorant", "Sanitary Pads", "Shaving Cream", "Hair Oil",
        "Perfume", "Handwash", "Body Lotion",
    ],
    "Health_Wellness": [
        "Protein Powder", "Vitamins", "ORS", "Green Tea", "Apple Cider Vinegar", "Honey",
        "Chyawanprash", "Face Masks", "Creatine", "Peanut Butter High Protein", "Sugar Free", "Quinoa",
    ],
    "Snacks_Munchies": [
        "Lays", "Kurkure", "Nachos", "Dairy Milk", "Maggi", "Pasta", "Ice Cream", "Cold Drink",
        "Red Bull", "Party Soda", "Ice Cubes", "Chips", "Biscuits", "Chocolates", "Cookies",
        "Namkeen", "Popcorn", "Doritos", "Haldirams", "Snickers", "Cadbury", "Oreo", "Britannia",
    ],
    "Staples_Kitchen": [
        "Rice", "Wheat Flour", "Atta", "Toor Dal", "Moong Dal", "Sugar", "Salt", "Tea",
        "Coffee", "Cooking Oil", "Basmati Rice", "Besan", "Maida", "Poha", "Daliya",
        "Jaggery", "Olive Oil", "Soy Sauce", "Noodles", "Ketchup", "Mayonnaise", "Jam", "Vinegar",
    ],
    "Household": [
        "Surf Excel", "Vim Liquid", "Comfort", "Toilet Paper", "Garbage Bags", "Colin",
        "Matchbox", "Agarbatti", "Mosquito Repellent", "Detergent", "Dish Soap", "Floor Cleaner",
        "Napkins", "Foil", "Bulbs", "Lizol", "Ariel",
    ],
    "Pharmacy_OTC": [
        "Digene Tablet", "Saridon", "Strepsils", "Eno Sachet", "Hansaplast",
        "Pain Relief Patch", "BCAA Powder", "Mass Gainer", "Fish Oil Capsules",
        "Multivitamin Men", "Multivitamin Women", "Pregnancy Test Kit", "Intimate Wash",
    ],
    "Kitchen_Dining_Tools": [
        "Chef Knife", "Non Stick Pan", "Glass Tumbler Set", "Plastic Containers",
        "Dinner Plate Set", "Spatula set", "Chopping Board", "Lunch Box",
        "Water Bottle 2L", "Steel Spoon Set", "Tea Strainer", "Ice Tray",
    ],
    "Specialized_Cleaning": [
        "Drain Cleaner", "Cockroach Spray", "Rat Trap", "Naphthalene Balls",
        "Toilet Brush", "Microfiber Mop", "Vacuum Bag", "Descaling Powder",
        "Stain Remover", "Ironing Spray", "Leather Polish", "Woolen Detergent",
    ],
    "Electronics_Expanded": [
        "HDMI Cable", "VGA Adapter", "Extension Cord 5m", "Universal Remote",
        "WiFi Range Extender", "LED Strip Lights", "USB Hub", "Smartphone Tripod",
        "Screen Guard", "OTG Adapter", "Smart Bulb", "Laptop Stand", "Webcam",
        "Monitor Stand", "Wall Mount TV", "Smart Plug",
    ],
    "Art_Stationery": [
        "Classmate Notebook", "A4 Printing Paper", "Gel Pen Pack", "Permanent Marker",
        "Fevicol Stick", "Sticky Notes", "Highlighter Set", "Stapler", "Spiral Notebook",
        "Calculator", "Whiteboard Marker", "Acrylic Paint Set", "Paint Brush Pack",
        "Canvas Board", "Hot Glue Gun", "Scissor Set", "Glitter Glue", "Sketching Pencil Set",
        "Modeling Clay", "Origami Paper", "Watercolor Set", "Sketchbook", "Geometry Box",
        "Correction Tape", "Oil Pastels", "Calligraphy Pen",
    ],
    "Hardware_Tools": [
        "Screwdriver Set", "Digital Weighing Scale", "Extension Cord", "Torch Light",
        "Double Sided Tape", "Step Ladder", "Combination Plier", "Utility Knife",
        "Super Glue", "M-Seal", "Measuring Tape", "Drill Machine", "Wall Hook",
        "Curtain Brackets", "Door Handle", "Padlock", "WD-40", "Hammer", "Spirit Level",
    ],
    "Home_Decor": [
        "Cotton Bedsheet", "Memory Foam Pillow", "Microfiber Towel", "Door Mat",
        "Scented Candle Set", "Artificial Plants", "Cushion Cover", "Wall Clock",
        "Flower Vase", "Curtain Rods", "Table Cloth",
    ],
    "Gym_Fitness": [
        "Yoga Mat", "Dumbbells 5kg", "Resistance Bands Set", "Skipping Rope",
        "Hand Gripper", "Push Up Bar", "Badminton Shuttlecocks", "Tennis Ball",
        "Cricket Bat", "Ab Roller", "Yoga Block", "Cycling Gloves",
        "Protein Shaker Bottle", "Isotonic Drink", "Wrist Support",
    ],
    "Gourmet_Specialty": [
        "Almond Milk Unsweetened", "Oat Milk", "Hazelnut Spread", "Chocolate Syrup",
        "Pancake Mix", "Frozen Waffles", "Blueberries", "Avocado", "Tofu Extra Firm",
        "Kombucha", "Matcha Powder", "Granola Bars", "Honey Nut Cornflakes",
    ],
    "Puja_Essentials": [
        "Mangaldeep Agarbatti", "Camphor Tablets", "Cow Ghee Diya", "Cotton Batti",
        "Puja Oil", "Dhoop Sticks", "Ganga Jal", "Rose Water", "Sandalwood Powder",
    ],
    "Travel_Fashion_Auto": [
        "Neck Pillow", "Umbrella Large", "Raincoat Men", "Trolley Bag Cover",
        "TSA Padlock", "Portable Fan USB", "Eye Mask", "Leather Belt",
        "Canvas Wallet", "Tote Bag", "Backpack for College", "Shoe Polish",
        "Car Wash Shampoo", "Microfiber Cloth Car", "Car Air Freshener Gel",
        "Chain Lube Bike", "Tyre Polish", "Dashboard Cleaner",
    ],
    "Books_Hobbies": [
        "Self Help Books", "Coloring Book for Kids", "Sudoku Book", "Uno Cards",
        "Monopoly Game", "Building Blocks", "Puzzle 500 Pieces", "Fountain Pen Ink",
        "Magnifying Glass", "Flash Cards",
    ],
    "Festive_Party": [
        "Birthday Candles", "Party Poppers", "Balloons", "Birthday Sash", "Paper Plates",
        "Disposable Cups", "Festive Diya", "Holi Colors", "Rakhi", "Diwali Gift Hamper",
        "Christmas Decor", "New Year Party Hats", "Seasonal Umbrella",
    ],
    "Meat_Seafood": [
        "Chicken Breast", "Mutton Curry Cut", "Prawns Frozen", "Basa Fillet", "Rohu Fish",
        "Chicken Sausages", "Salami", "Plant-based Nuggets", "Soya Chaap", "Smoked Salmon", "Pork Pepperoni",
    ],
    "Gardening": [
        "Potting Soil", "Organic Vermicompost", "Plant Seeds", "Spray Bottle for Plants",
        "Ceramic Pots", "Gardening Gloves", "Neem Oil Spray", "Flower Food", "Decorative Pebbles",
    ],
    "Ethnic_Meals": [
        "Instant Poha Mix", "Ready-to-eat Upma", "Idli Dosa Batter", "Malabar Paratha",
        "Medu Vada Batter", "Ready-to-eat Rajma Chawal", "Palak Paneer", "Instant Pasta Bowls",
    ],
    "Toys_Parenting": [
        "Hot Wheels Cars", "Barbie Dolls", "Slime Kit", "Playing Cards", "Bubble Maker",
        "Rubik's Cube", "Action Figures", "Board Games", "Small Plush Toys",
    ],
    "Pet_Baby_Care": [
        "Dog Food", "Cat Food", "Pedigree", "Whiskas", "Pet Wipes", "Pet Treats",
        "Diapers", "Baby Wipes", "Baby Food", "Cerelac", "Baby Lotion", "Baby Shampoo",
        "Pampers", "Huggies", "MamyPoko",
    ],
    "Bakery_International": [
        "Croissant", "Brownie", "Cheese Cake", "Sourdough", "Gluten Free Bread", "Bun", "Rusk",
        "Cake", "Muffin", "Donut", "Pav", "White Bread", "Brown Bread", "Atta Bread",
        "Schezwan Sauce", "Pizza Sauce", "Pasta Sauce",
    ],
}

KEYWORD_TO_CATEGORY = {kw.lower(): cat for cat, kws in CATEGORY_MAP.items() for kw in kws}
