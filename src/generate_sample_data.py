"""
Generate realistic sample CSV data for the E-commerce Transactional Dataset.
This creates small representative samples that mirror the real Kaggle dataset schema.
Run this ONLY if you don't have the real dataset downloaded yet.

Usage:
    python src/generate_sample_data.py
"""

import csv
import os
import random
import json
from datetime import datetime, timedelta

random.seed(42)

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
os.makedirs(SAMPLE_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 300
NUM_TRANSACTIONS = 2000
NUM_CLICKSTREAM = 5000

GENDERS = ["M", "F"]
DEVICE_TYPES = ["Android", "iOS", "Web"]
DEVICE_VERSIONS = [
    "Samsung Galaxy S21", "iPhone 13", "Pixel 6", "OnePlus 9",
    "iPad Pro", "Chrome 110", "Safari 16", "Firefox 109",
    "Samsung Galaxy A52", "iPhone 14 Pro"
]
COUNTRIES = ["Indonesia", "United States", "India", "United Kingdom", "Australia",
             "Singapore", "Malaysia", "Germany", "Japan", "Canada"]
REGIONS = {
    "Indonesia": ["Jakarta", "Surabaya", "Bandung", "Medan", "Bali"],
    "United States": ["California", "New York", "Texas", "Florida", "Illinois"],
    "India": ["Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "Gujarat"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Singapore": ["Central", "East", "West", "North", "South"],
    "Malaysia": ["Kuala Lumpur", "Penang", "Johor", "Sabah", "Sarawak"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"]
}
MASTER_CATEGORIES = ["Apparel", "Accessories", "Footwear", "Personal Care", "Free Items"]
SUB_CATEGORIES = {
    "Apparel": ["Topwear", "Bottomwear", "Dress", "Innerwear"],
    "Accessories": ["Watches", "Bags", "Belts", "Wallets", "Jewellery"],
    "Footwear": ["Shoes", "Sandal", "Flip Flops"],
    "Personal Care": ["Fragrance", "Lips", "Eyes", "Nails", "Skin Care"],
    "Free Items": ["Free Gift", "Sample"]
}
ARTICLE_TYPES = {
    "Topwear": ["Tshirts", "Shirts", "Casual Shirts", "Formal Shirts", "Sweatshirts"],
    "Bottomwear": ["Jeans", "Track Pants", "Shorts", "Trousers"],
    "Dress": ["Dresses", "Jumpsuit", "Rompers"],
    "Innerwear": ["Bra", "Briefs", "Boxers", "Trunk"],
    "Watches": ["Analog Watches", "Digital Watches", "Smart Watches"],
    "Bags": ["Backpacks", "Handbags", "Laptop Bag", "Clutches"],
    "Belts": ["Formal Belts", "Casual Belts"],
    "Wallets": ["Wallets", "Card Holder"],
    "Jewellery": ["Earrings", "Ring", "Necklace", "Bracelet"],
    "Shoes": ["Casual Shoes", "Sports Shoes", "Formal Shoes", "Sneakers"],
    "Sandal": ["Sports Sandals", "Floaters"],
    "Flip Flops": ["Flip Flops", "Slides"],
    "Fragrance": ["Perfume", "Deodorant"],
    "Lips": ["Lipstick", "Lip Gloss"],
    "Eyes": ["Mascara", "Eyeliner"],
    "Nails": ["Nail Polish"],
    "Skin Care": ["Moisturiser", "Sunscreen", "Face Wash"],
    "Free Gift": ["Free Gift"],
    "Sample": ["Sample Pack"]
}
BASE_COLOURS = ["Black", "White", "Blue", "Red", "Green", "Navy Blue",
                "Grey", "Brown", "Pink", "Yellow", "Purple", "Orange", "Maroon"]
SEASONS = ["Summer", "Winter", "Spring", "Fall"]
USAGES = ["Casual", "Formal", "Sports", "Smart Casual", "Party", "Travel"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Bank Transfer", "E-Wallet",
                   "COD", "PayPal", "QRIS"]
PAYMENT_STATUSES = ["Success", "Failed"]
PROMO_CODES = [None, "WELCOME10", "SALE20", "FLASH15", "MEMBER25", "HOLIDAY30",
               "NEWUSER50", "FREESHIP"]
EVENT_NAMES = ["view_product", "add_to_cart", "remove_from_cart", "checkout",
               "search", "view_category", "apply_promo", "view_homepage",
               "login", "logout", "wishlist_add", "share_product"]
TRAFFIC_SOURCES = ["mobile", "web", "tablet"]


def random_date(start_year=2022, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    rand_days = random.randint(0, delta.days)
    rand_secs = random.randint(0, 86399)
    return start + timedelta(days=rand_days, seconds=rand_secs)


def generate_customers():
    """Generate customer table CSV."""
    filepath = os.path.join(SAMPLE_DIR, "customer.csv")
    first_names_m = ["James", "Robert", "John", "Michael", "David", "William",
                     "Ahmad", "Budi", "Ravi", "Kenji", "Hans", "Pierre"]
    first_names_f = ["Mary", "Patricia", "Jennifer", "Linda", "Sarah", "Emma",
                     "Siti", "Priya", "Yuki", "Anna", "Maria", "Fatima"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                  "Wijaya", "Kusuma", "Sharma", "Tanaka", "Mueller", "Dubois",
                  "Lee", "Chen", "Kim", "Singh", "Anderson", "Taylor"]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id", "first_name", "last_name", "username", "email",
            "gender", "device_type", "device_id", "device_version",
            "home_location_lat", "home_location_long", "home_location",
            "home_country", "first_join_date"
        ])
        for i in range(1, NUM_CUSTOMERS + 1):
            gender = random.choice(GENDERS)
            fname = random.choice(first_names_m if gender == "M" else first_names_f)
            lname = random.choice(last_names)
            username = f"{fname.lower()}{lname.lower()}{random.randint(1, 999)}"
            email = f"{username}@{'gmail.com' if random.random() < 0.6 else 'yahoo.com'}"
            device_type = random.choice(DEVICE_TYPES)
            device_id = f"DEV-{random.randint(100000, 999999)}"
            device_version = random.choice(DEVICE_VERSIONS)
            country = random.choice(COUNTRIES)
            region = random.choice(REGIONS[country])
            lat = round(random.uniform(-10, 55), 6)
            lon = round(random.uniform(95, 145), 6)
            join_date = random_date(2020, 2023).strftime("%Y-%m-%d")

            writer.writerow([
                f"CUST-{i:05d}", fname, lname, username, email, gender,
                device_type, device_id, device_version, lat, lon,
                region, country, join_date
            ])
    print(f"✓ Generated {NUM_CUSTOMERS} customers → {filepath}")
    return filepath


def generate_products():
    """Generate product table CSV."""
    filepath = os.path.join(SAMPLE_DIR, "product.csv")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id", "gender", "masterCategory", "subCategory", "articleType",
            "baseColour", "season", "year", "usage", "productDisplayName"
        ])
        for i in range(1, NUM_PRODUCTS + 1):
            gender = random.choice(["Men", "Women", "Unisex"])
            master = random.choice(MASTER_CATEGORIES)
            sub = random.choice(SUB_CATEGORIES[master])
            article = random.choice(ARTICLE_TYPES[sub])
            colour = random.choice(BASE_COLOURS)
            season = random.choice(SEASONS)
            year = random.randint(2020, 2024)
            usage = random.choice(USAGES)
            display_name = f"{colour} {article} for {gender}"

            writer.writerow([
                i, gender, master, sub, article, colour,
                season, year, usage, display_name
            ])
    print(f"✓ Generated {NUM_PRODUCTS} products → {filepath}")
    return filepath


def generate_transactions():
    """Generate transaction table CSV with product_metadata as JSON dict."""
    filepath = os.path.join(SAMPLE_DIR, "transaction.csv")
    product_ids = list(range(1, NUM_PRODUCTS + 1))
    customer_ids = [f"CUST-{i:05d}" for i in range(1, NUM_CUSTOMERS + 1)]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "created_at", "customer_id", "booking_id", "session_id",
            "product_metadata", "payment_method", "payment_status",
            "promo_amount", "promo_code", "shipment_fee",
            "shipment_date_limit", "shipment_location_lat",
            "shipment_location_long", "total_amount"
        ])
        for i in range(1, NUM_TRANSACTIONS + 1):
            created_at = random_date(2023, 2024).strftime("%Y-%m-%d %H:%M:%S")
            cust_id = random.choice(customer_ids)
            booking_id = f"BK-{random.randint(100000, 999999)}"
            session_id = f"SESS-{random.randint(100000, 999999)}"

            # product_metadata is a JSON dict
            num_products = random.randint(1, 4)
            products = []
            for _ in range(num_products):
                pid = random.choice(product_ids)
                qty = random.randint(1, 3)
                price = round(random.uniform(5.0, 500.0), 2)
                products.append({"product_id": pid, "quantity": qty, "price": price})
            product_meta = json.dumps({"items": products})

            payment_method = random.choice(PAYMENT_METHODS)
            # 85% success rate
            payment_status = "Success" if random.random() < 0.85 else "Failed"
            promo_code = random.choice(PROMO_CODES)
            promo_amount = round(random.uniform(0, 50), 2) if promo_code else 0.0
            shipment_fee = round(random.uniform(0, 25), 2)
            ship_date_limit = (random_date(2023, 2024) + timedelta(days=random.randint(3, 14))).strftime("%Y-%m-%d")
            ship_lat = round(random.uniform(-10, 55), 6)
            ship_lon = round(random.uniform(95, 145), 6)
            subtotal = sum(p["price"] * p["quantity"] for p in products)
            total_amount = round(subtotal + shipment_fee - promo_amount, 2)
            total_amount = max(total_amount, 0)

            writer.writerow([
                created_at, cust_id, booking_id, session_id,
                product_meta, payment_method, payment_status,
                promo_amount, promo_code if promo_code else "",
                shipment_fee, ship_date_limit, ship_lat, ship_lon,
                total_amount
            ])
    print(f"✓ Generated {NUM_TRANSACTIONS} transactions → {filepath}")
    return filepath


def generate_clickstream():
    """Generate click stream table CSV with event_metadata as JSON dict."""
    filepath = os.path.join(SAMPLE_DIR, "click_stream.csv")
    session_ids = [f"SESS-{random.randint(100000, 999999)}" for _ in range(800)]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "session_id", "event_name", "event_time", "event_id",
            "traffic_source", "event_metadata"
        ])
        for i in range(1, NUM_CLICKSTREAM + 1):
            session_id = random.choice(session_ids)
            event_name = random.choice(EVENT_NAMES)
            event_time = random_date(2023, 2024).strftime("%Y-%m-%d %H:%M:%S")
            event_id = f"EVT-{random.randint(1000000, 9999999)}"
            traffic_source = random.choice(TRAFFIC_SOURCES)

            # event_metadata as JSON
            meta = {"page": f"/{event_name.replace('_', '/')}", "duration_sec": random.randint(1, 300)}
            if event_name in ("view_product", "add_to_cart", "wishlist_add"):
                meta["product_id"] = random.randint(1, NUM_PRODUCTS)
            if event_name == "search":
                meta["query"] = random.choice(["shoes", "dress", "watch", "bag", "shirt", "jeans"])
            if event_name == "apply_promo":
                meta["promo_code"] = random.choice(["WELCOME10", "SALE20", "FLASH15"])
            event_metadata = json.dumps(meta)

            writer.writerow([
                session_id, event_name, event_time, event_id,
                traffic_source, event_metadata
            ])
    print(f"✓ Generated {NUM_CLICKSTREAM} click stream events → {filepath}")
    return filepath


if __name__ == "__main__":
    print("=" * 60)
    print("Generating sample e-commerce dataset...")
    print("=" * 60)
    generate_customers()
    generate_products()
    generate_transactions()
    generate_clickstream()
    print("=" * 60)
    print(f"All files saved to: {os.path.abspath(SAMPLE_DIR)}")
    print("=" * 60)
