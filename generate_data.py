"""
generate_data.py
----------------
Generates three realistic e-commerce CSV datasets:
  - customers.csv  (1 000+ records)
  - products.csv   (500+ records)
  - orders.csv     (10 000+ records)

Intentional data-quality issues are injected into each dataset so that the
PySpark cleaning pipeline has meaningful work to do:
  * Missing / null values
  * Duplicate rows
  * Inconsistent string casing
  * Invalid numeric values (negatives, out-of-range)
  * Badly formatted dates

Run:
    python generate_data.py

Output goes to data/raw/
"""

import os
import random
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

# ─── Reproducibility ─────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("en_US")
Faker.seed(SEED)

# ─── Output directory ─────────────────────────────────────────────────────────
RAW_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# ─── Constants ────────────────────────────────────────────────────────────────
NUM_CUSTOMERS = 1050
NUM_PRODUCTS = 520
NUM_ORDERS = 10_500

US_STATES = [
    "California", "Texas", "Florida", "New York", "Pennsylvania",
    "Illinois", "Ohio", "Georgia", "North Carolina", "Michigan",
    "New Jersey", "Virginia", "Washington", "Arizona", "Tennessee",
    "Massachusetts", "Indiana", "Missouri", "Maryland", "Wisconsin",
    "Colorado", "Minnesota", "South Carolina", "Alabama", "Louisiana",
    "Kentucky", "Oregon", "Oklahoma", "Connecticut", "Utah",
    "Nevada", "Iowa", "Arkansas", "Mississippi", "Kansas",
    "New Mexico", "Nebraska", "Idaho", "West Virginia", "Hawaii",
    "New Hampshire", "Maine", "Montana", "Rhode Island", "Delaware",
    "South Dakota", "North Dakota", "Alaska", "Vermont", "Wyoming",
]

# Abbreviations used to inject inconsistent formatting
STATE_ABBREV = {
    "California": "CA", "Texas": "TX", "Florida": "FL",
    "New York": "NY", "Pennsylvania": "PA", "Illinois": "IL",
    "Ohio": "OH", "Georgia": "GA", "Michigan": "MI",
}

CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books",
    "Sports & Outdoors", "Toys & Games", "Health & Beauty",
    "Automotive", "Garden & Tools", "Office Supplies",
]

# ─── Helper functions ─────────────────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> str:
    """Return a random date string between start and end."""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def inject_nulls(series: pd.Series, null_rate: float = 0.04) -> pd.Series:
    """Randomly replace values with NaN at the specified rate."""
    mask = np.random.random(len(series)) < null_rate
    series = series.copy().astype(object)
    series[mask] = np.nan
    return series


def inject_duplicates(df: pd.DataFrame, dup_rate: float = 0.025) -> pd.DataFrame:
    """Append a random sample of rows to simulate duplicate records."""
    n_dups = max(1, int(len(df) * dup_rate))
    dupes = df.sample(n=n_dups, random_state=SEED)
    return pd.concat([df, dupes], ignore_index=True)


# ─────────────────────────────────────────────────────────────────────────────
# 1. CUSTOMERS
# ─────────────────────────────────────────────────────────────────────────────

def generate_customers() -> pd.DataFrame:
    print("  Generating customers...")

    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, NUM_CUSTOMERS + 1)]
    names = [fake.name() for _ in range(NUM_CUSTOMERS)]
    cities = [fake.city() for _ in range(NUM_CUSTOMERS)]

    # Mix full state names with abbreviations and wrong casing to create
    # inconsistent formatting that the cleaning step must standardise.
    states = []
    for _ in range(NUM_CUSTOMERS):
        state = random.choice(US_STATES)
        rand = random.random()
        if rand < 0.10 and state in STATE_ABBREV:
            states.append(STATE_ABBREV[state])          # abbreviation
        elif rand < 0.15:
            states.append(state.upper())                # ALL CAPS
        elif rand < 0.20:
            states.append(state.lower())                # all lower
        else:
            states.append(state)                        # normal
    
    reg_start = datetime(2018, 1, 1)
    reg_end   = datetime(2024, 12, 31)
    reg_dates = [random_date(reg_start, reg_end) for _ in range(NUM_CUSTOMERS)]

    df = pd.DataFrame({
        "customer_id":       customer_ids,
        "customer_name":     names,
        "city":              cities,
        "state":             states,
        "registration_date": reg_dates,
    })

    # ── Inject data-quality issues ────────────────────────────────────────────
    df["customer_name"] = inject_nulls(df["customer_name"], null_rate=0.03)
    df["city"]          = inject_nulls(df["city"],          null_rate=0.02)
    df["state"]         = inject_nulls(df["state"],         null_rate=0.02)

    # A handful of obviously invalid dates
    bad_date_idx = random.sample(range(len(df)), 15)
    for idx in bad_date_idx:
        df.at[idx, "registration_date"] = random.choice(
            ["N/A", "0000-00-00", "13/32/2022", ""]
        )

    df = inject_duplicates(df, dup_rate=0.03)

    # Shuffle so duplicates are not all at the end
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    print(f"    → {len(df):,} rows (including duplicates)")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. PRODUCTS
# ─────────────────────────────────────────────────────────────────────────────

def _product_name(category: str) -> str:
    """Generate a realistic product name for a given category."""
    templates = {
        "Electronics":       ["Wireless Earbuds", "4K Smart TV", "Gaming Laptop",
                              "Bluetooth Speaker", "USB-C Hub", "Smart Watch",
                              "Action Camera", "Portable Charger", "LED Monitor",
                              "Mechanical Keyboard"],
        "Clothing":          ["Running Shoes", "Denim Jacket", "Cotton T-Shirt",
                              "Yoga Pants", "Winter Coat", "Polo Shirt",
                              "Sneakers", "Hooded Sweatshirt", "Cargo Shorts",
                              "Athletic Socks"],
        "Home & Kitchen":    ["Air Fryer", "Coffee Maker", "Blender",
                              "Instant Pot", "Non-stick Pan", "Rice Cooker",
                              "Vacuum Cleaner", "Knife Set", "Toaster Oven",
                              "Stand Mixer"],
        "Books":             ["Data Engineering Handbook", "Python for Data Science",
                              "Clean Code", "System Design Interview",
                              "Atomic Habits", "The Pragmatic Programmer",
                              "Machine Learning Basics", "SQL Performance Tuning",
                              "Deep Work", "Cloud Architecture Patterns"],
        "Sports & Outdoors": ["Yoga Mat", "Dumbbell Set", "Resistance Bands",
                              "Hiking Backpack", "Cycling Helmet", "Jump Rope",
                              "Pull-up Bar", "Foam Roller", "Trekking Poles",
                              "Tennis Racket"],
        "Toys & Games":      ["LEGO Set", "Board Game", "Action Figure",
                              "Remote Control Car", "Puzzle Set", "Card Game",
                              "Building Blocks", "Doll House", "Model Kit",
                              "Plush Toy"],
        "Health & Beauty":   ["Face Moisturizer", "Hair Dryer", "Electric Toothbrush",
                              "Vitamin D Supplement", "Protein Powder",
                              "Sunscreen SPF50", "Essential Oil Set",
                              "Shampoo & Conditioner", "Makeup Brush Set",
                              "Foam Cleanser"],
        "Automotive":        ["Car Phone Mount", "Dash Camera", "Tire Inflator",
                              "LED Headlights", "Car Vacuum", "Seat Cushion",
                              "Jump Starter", "Floor Mats", "Car Wax Kit",
                              "OBD2 Scanner"],
        "Garden & Tools":    ["Garden Hose", "Pruning Shears", "Soil Tester",
                              "Raised Bed Kit", "Compost Bin", "Sprinkler Head",
                              "Weed Puller", "Lawn Mower Blade", "Plant Pots",
                              "Fertilizer Bag"],
        "Office Supplies":   ["Ergonomic Chair", "Standing Desk", "Desk Lamp",
                              "Notebook Set", "Stapler", "Paper Shredder",
                              "Whiteboard", "Desk Organizer", "Monitor Stand",
                              "Cable Management Kit"],
    }
    base = random.choice(templates.get(category, ["Generic Product"]))
    # Add a model number to differentiate products in the same category
    model = f"{random.choice(['Pro', 'Plus', 'Lite', 'Max', 'Elite', 'Basic'])} {random.randint(1, 9)}000"
    return f"{base} {model}"


def generate_products() -> pd.DataFrame:
    print("  Generating products...")

    product_ids = [f"PROD{str(i).zfill(5)}" for i in range(1, NUM_PRODUCTS + 1)]

    categories = []
    for _ in range(NUM_PRODUCTS):
        cat = random.choice(CATEGORIES)
        # Inject inconsistent casing
        rand = random.random()
        if rand < 0.08:
            cat = cat.upper()
        elif rand < 0.12:
            cat = cat.lower()
        categories.append(cat)

    names  = [_product_name(CATEGORIES[i % len(CATEGORIES)]) for i in range(NUM_PRODUCTS)]
    prices = np.round(np.random.uniform(5.99, 1499.99, NUM_PRODUCTS), 2)

    df = pd.DataFrame({
        "product_id":   product_ids,
        "product_name": names,
        "category":     categories,
        "price":        prices,
    })

    # ── Inject data-quality issues ────────────────────────────────────────────
    df["product_name"] = inject_nulls(df["product_name"], null_rate=0.025)
    df["price"]        = inject_nulls(df["price"],        null_rate=0.03)

    # A few negative / zero prices to validate and drop in cleaning
    bad_price_idx = random.sample(range(len(df)), 10)
    for idx in bad_price_idx:
        df.at[idx, "price"] = random.choice([-1.0, 0.0, -99.99])

    df = inject_duplicates(df, dup_rate=0.03)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    print(f"    → {len(df):,} rows (including duplicates)")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. ORDERS
# ─────────────────────────────────────────────────────────────────────────────

def generate_orders(customer_ids: list, product_ids: list) -> pd.DataFrame:
    print("  Generating orders...")

    order_ids = [f"ORD{str(i).zfill(6)}" for i in range(1, NUM_ORDERS + 1)]

    # Most orders reference valid customers; ~2% reference non-existent ones
    order_customers = []
    for _ in range(NUM_ORDERS):
        if random.random() < 0.02:
            order_customers.append(f"CUST{random.randint(99000, 99999)}")
        else:
            order_customers.append(random.choice(customer_ids))

    # Most orders reference valid products; ~2% reference non-existent ones
    order_products = []
    for _ in range(NUM_ORDERS):
        if random.random() < 0.02:
            order_products.append(f"PROD{random.randint(99000, 99999)}")
        else:
            order_products.append(random.choice(product_ids))

    quantities = np.random.randint(1, 15, NUM_ORDERS)
    discounts  = np.round(np.random.choice(
        [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30],
        size=NUM_ORDERS,
        p=[0.35, 0.20, 0.18, 0.12, 0.08, 0.05, 0.02]
    ), 2)

    order_start = datetime(2022, 1, 1)
    order_end   = datetime(2024, 12, 31)
    order_dates = [random_date(order_start, order_end) for _ in range(NUM_ORDERS)]

    df = pd.DataFrame({
        "order_id":   order_ids,
        "customer_id": order_customers,
        "product_id":  order_products,
        "quantity":    quantities,
        "discount":    discounts,
        "order_date":  order_dates,
    })

    # ── Inject data-quality issues ────────────────────────────────────────────
    df["discount"]  = inject_nulls(df["discount"],  null_rate=0.05)
    df["quantity"]  = inject_nulls(df["quantity"],  null_rate=0.02)

    # Some negative quantities (invalid — will be filtered in cleaning)
    bad_qty_idx = random.sample(range(len(df)), 30)
    for idx in bad_qty_idx:
        df.at[idx, "quantity"] = random.choice([-1, -3, -5, 0])

    # Discount values > 1 (invalid — discount should be 0-1)
    bad_disc_idx = random.sample(range(len(df)), 20)
    for idx in bad_disc_idx:
        df.at[idx, "discount"] = random.choice([1.5, 2.0, -0.1])

    df = inject_duplicates(df, dup_rate=0.025)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)
    print(f"    → {len(df):,} rows (including duplicates)")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  E-commerce Dataset Generator")
    print("="*60)

    customers_df = generate_customers()
    products_df  = generate_products()

    # Use the clean IDs (first NUM records before duplicates) for valid FK refs
    clean_customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, NUM_CUSTOMERS + 1)]
    clean_product_ids  = [f"PROD{str(i).zfill(5)}" for i in range(1, NUM_PRODUCTS + 1)]

    orders_df = generate_orders(clean_customer_ids, clean_product_ids)

    # ── Save to CSV ────────────────────────────────────────────────────────────
    customers_path = os.path.join(RAW_DIR, "customers.csv")
    products_path  = os.path.join(RAW_DIR, "products.csv")
    orders_path    = os.path.join(RAW_DIR, "orders.csv")

    customers_df.to_csv(customers_path, index=False)
    products_df.to_csv(products_path,   index=False)
    orders_df.to_csv(orders_path,       index=False)

    print("\n✅ Datasets saved to data/raw/")
    print(f"   customers.csv : {len(customers_df):>7,} rows")
    print(f"   products.csv  : {len(products_df):>7,} rows")
    print(f"   orders.csv    : {len(orders_df):>7,} rows")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
