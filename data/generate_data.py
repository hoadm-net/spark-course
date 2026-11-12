"""Generate the synthetic VietMart dataset used throughout the course.

Run once from the repository root:  python data/generate_data.py
Only the standard library is used and the random seed is fixed, so everyone
gets exactly the same files.
"""
import csv
import json
import os
import random
from datetime import datetime, timedelta

random.seed(42)
OUT = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(OUT, "events"), exist_ok=True)

CITIES = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Cần Thơ",
          "Hải Phòng", "Huế", "Nha Trang", "Biên Hòa"]
SEGMENTS = ["Standard", "Silver", "Gold"]
# category -> (product names, (min price, max price) in thousand VND)
CATEGORIES = {
    "Electronics": (["Headphones", "Mouse", "Keyboard", "Speaker", "Power bank"], (150, 2500)),
    "Books":       (["Self-help book", "Novel", "Comic book", "Textbook", "Dictionary"], (40, 350)),
    "Fashion":     (["T-shirt", "Jeans", "Shoes", "Backpack", "Cap"], (90, 1200)),
    "Home":        (["Pot", "Pan", "Fan", "Desk lamp", "Water bottle"], (60, 1800)),
    "Food":        (["Coffee", "Tea", "Cookies", "Cashews", "Honey"], (30, 400)),
}

# ---- 1. products.json (JSON Lines, 100 products) ----
products, product_id = [], 1
for category, (names, (low, high)) in CATEGORIES.items():
    for name in names:
        for model in range(1, 5):
            products.append({
                "product_id": product_id,
                "product_name": f"{name} Model {model}",
                "category": category,
                "price": random.randint(low, high) * 1000,
            })
            product_id += 1
with open(os.path.join(OUT, "products.json"), "w", encoding="utf-8") as f:
    for product in products:
        f.write(json.dumps(product, ensure_ascii=False) + "\n")

# ---- 2. customers.csv (5,000 customers + 50 duplicated rows, dirty cities) ----
LAST_NAMES = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Võ", "Đặng", "Bùi"]
FIRST_NAMES = ["An", "Bình", "Chi", "Dũng", "Hà", "Khoa", "Lan", "Minh", "Ngọc", "Quân", "Thảo", "Vy"]
customers = [[1, "Walk-in customer", "Hồ Chí Minh", "2024-01-01", "Standard"]]
for customer_id in range(2, 5001):
    city = random.choice(CITIES)
    r = random.random()
    if r < 0.02:
        city = ""                              # missing city
    elif r < 0.07:
        city = "  " + city.upper() + " "       # upper case with stray whitespace
    signup = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 540))
    customers.append([customer_id, f"{random.choice(LAST_NAMES)} {random.choice(FIRST_NAMES)}", city,
                      signup.strftime("%Y-%m-%d"),
                      random.choices(SEGMENTS, weights=[70, 20, 10])[0]])
customers += random.sample(customers, 50)       # duplicated rows
with open(os.path.join(OUT, "customers.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id", "full_name", "city", "signup_date", "segment"])
    writer.writerows(customers)

# ---- 3. orders.csv (50,000 orders) and order_items.csv ----
START = datetime(2024, 7, 1)
SECONDS = 549 * 24 * 3600                       # 2024-07-01 .. 2025-12-31
PAYMENTS = ["COD", "CARD", "EWALLET", "BANK"]
orders, items = [], []
for order_id in range(1, 50001):
    # 20% of all orders belong to customer 1 -> skewed key
    customer_id = 1 if random.random() < 0.20 else random.randint(2, 4800)
    ts = START + timedelta(seconds=random.randint(0, SECONDS - 1))
    payment = random.choices(PAYMENTS, weights=[40, 25, 25, 10])[0]
    num_items = random.randint(1, 5)
    p_cancel = 0.05 + (0.15 if payment == "COD" else 0) + (0.10 if num_items >= 4 else 0)
    r = random.random()
    status = "CANCELLED" if r < p_cancel else ("SHIPPING" if r > 0.95 else "COMPLETED")
    orders.append([order_id, customer_id, ts.strftime("%Y-%m-%d %H:%M:%S"), status, payment])
    for product in random.sample(products, num_items):
        items.append([order_id, product["product_id"], random.randint(1, 3), product["price"],
                      random.choice([0, 0, 0, 0.05, 0.1])])
with open(os.path.join(OUT, "orders.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "customer_id", "order_date", "status", "payment_method"])
    writer.writerows(orders)
with open(os.path.join(OUT, "order_items.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "product_id", "quantity", "unit_price", "discount"])
    writer.writerows(items)

# ---- 4. events/*.json (10 files, 2,000 clickstream events per hour-long file) ----
T0 = datetime(2025, 12, 1, 8, 0, 0)
for i in range(10):
    with open(os.path.join(OUT, "events", f"events_{i:02d}.json"), "w", encoding="utf-8") as f:
        for _ in range(2000):
            t = T0 + timedelta(hours=i, seconds=random.randint(0, 3599))
            if random.random() < 0.03:          # 3% of events arrive up to 30 minutes late
                t -= timedelta(minutes=random.randint(5, 30))
            f.write(json.dumps({
                "event_time": t.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": random.randint(1, 4800),
                "event_type": random.choices(["view", "add_to_cart", "purchase"],
                                             weights=[75, 18, 7])[0],
                "product_id": random.randint(1, 100),
            }) + "\n")

print(f"Dataset written to {OUT}")
