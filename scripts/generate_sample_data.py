"""
generate_sample_data.py
Generate realistic Olist-like e-commerce data for the DE PoC pipeline.
Creates: customers.csv, orders.json, products.xml, order_items.csv,
         sellers.csv, payments.json
"""
import os
import json
import csv
import random
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
except ImportError:
    os.system("pip install faker -q")
    from faker import Faker

fake = Faker("pt_BR")
random.seed(42)

RAW_PATH = Path(os.getenv("DATA_RAW_PATH", "data/raw"))
RAW_PATH.mkdir(parents=True, exist_ok=True)

STATES = ["SP","RJ","MG","BA","PR","RS","PE","CE","PA","SC",
          "GO","MA","AM","ES","PB","PI","RN","AL","MT","MS","DF","SE","RO","TO","AC","AP","RR"]
CATEGORIES = [
    "beleza_saude","informatica_acessorios","automotivo","cama_mesa_banho",
    "moveis_decoracao","esporte_lazer","perfumaria","utilidades_domesticas",
    "telefonia","relogios_presentes","alimentos_bebidas","bebes","papelaria",
    "brinquedos","ferramentas_jardim","outros"
]
ORDER_STATUSES = ["delivered","shipped","canceled","invoiced","processing","created","unavailable"]
PAYMENT_TYPES  = ["credit_card","boleto","voucher","debit_card"]
STATUS_WEIGHTS = [70, 10, 8, 5, 4, 2, 1]

NUM_CUSTOMERS   = 1000
NUM_SELLERS     = 100
NUM_PRODUCTS    = 500
NUM_ORDERS      = 2000
NUM_ORDER_ITEMS = 4000
NUM_PAYMENTS    = 2500


def rand_date(start="2017-01-01", end="2024-12-31"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end,   "%Y-%m-%d")
    return s + timedelta(days=random.randint(0, (e - s).days))


def uid():
    return str(uuid.uuid4()).replace("-", "")


# ─────────────────────────────────────────
# 1. Customers (CSV)
# ─────────────────────────────────────────
def generate_customers():
    path = RAW_PATH / "customers.csv"
    customers = []
    for _ in range(NUM_CUSTOMERS):
        customers.append({
            "customer_id":        uid(),
            "customer_unique_id": uid(),
            "zip_code_prefix":    str(random.randint(10000, 99999)),
            "city":               fake.city(),
            "state":              random.choice(STATES),
        })
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=customers[0].keys())
        writer.writeheader()
        writer.writerows(customers)
    print(f"✔ customers.csv        → {len(customers):>6} records")
    return [c["customer_id"] for c in customers]


# ─────────────────────────────────────────
# 2. Sellers (CSV)
# ─────────────────────────────────────────
def generate_sellers():
    path = RAW_PATH / "sellers.csv"
    sellers = []
    for _ in range(NUM_SELLERS):
        sellers.append({
            "seller_id":      uid(),
            "zip_code_prefix": str(random.randint(10000, 99999)),
            "city":           fake.city(),
            "state":          random.choice(STATES),
        })
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sellers[0].keys())
        writer.writeheader()
        writer.writerows(sellers)
    print(f"✔ sellers.csv          → {len(sellers):>6} records")
    return [s["seller_id"] for s in sellers]


# ─────────────────────────────────────────
# 3. Products (XML)
# ─────────────────────────────────────────
def generate_products():
    path = RAW_PATH / "products.xml"
    root = ET.Element("products")
    product_ids = []
    for _ in range(NUM_PRODUCTS):
        pid = uid()
        product_ids.append(pid)
        p = ET.SubElement(root, "product")
        ET.SubElement(p, "product_id").text                  = pid
        ET.SubElement(p, "product_category_name").text       = random.choice(CATEGORIES)
        ET.SubElement(p, "product_name_length").text         = str(random.randint(20, 120))
        ET.SubElement(p, "product_description_length").text  = str(random.randint(100, 3000))
        ET.SubElement(p, "product_photos_qty").text          = str(random.randint(1, 20))
        ET.SubElement(p, "product_weight_g").text            = str(random.randint(50, 40000))
        ET.SubElement(p, "product_length_cm").text           = str(random.randint(5, 100))
        ET.SubElement(p, "product_height_cm").text           = str(random.randint(5, 100))
        ET.SubElement(p, "product_width_cm").text            = str(random.randint(5, 100))
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(str(path), encoding="utf-8", xml_declaration=True)
    print(f"✔ products.xml         → {len(product_ids):>6} records")
    return product_ids


# ─────────────────────────────────────────
# 4. Orders (JSON)
# ─────────────────────────────────────────
def generate_orders(customer_ids):
    path = RAW_PATH / "orders.json"
    orders = []
    order_ids = []
    for _ in range(NUM_ORDERS):
        oid    = uid()
        status = random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS)[0]
        purch  = rand_date()
        approved = purch + timedelta(hours=random.randint(1, 48))
        deliv  = approved + timedelta(days=random.randint(3, 45)) if status == "delivered" else None
        est_deliv = purch + timedelta(days=random.randint(10, 60))

        order_ids.append(oid)
        orders.append({
            "order_id":                 oid,
            "customer_id":              random.choice(customer_ids),
            "order_status":             status,
            "order_purchase_timestamp": purch.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at":        approved.strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_at":       deliv.strftime("%Y-%m-%d %H:%M:%S") if deliv else None,
            "order_estimated_delivery": est_deliv.strftime("%Y-%m-%d"),
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(orders, f, indent=2, ensure_ascii=False)
    print(f"✔ orders.json          → {len(orders):>6} records")
    return order_ids


# ─────────────────────────────────────────
# 5. Order Items (CSV)
# ─────────────────────────────────────────
def generate_order_items(order_ids, product_ids, seller_ids):
    path = RAW_PATH / "order_items.csv"
    items = []
    seen = {}
    for _ in range(NUM_ORDER_ITEMS):
        oid = random.choice(order_ids)
        seen[oid] = seen.get(oid, 0) + 1
        price     = round(random.uniform(5.0, 2000.0), 2)
        freight   = round(random.uniform(3.0, 100.0), 2)
        limit_dt  = (datetime.now() + timedelta(days=random.randint(2, 30))).strftime("%Y-%m-%d %H:%M:%S")
        items.append({
            "order_id":           oid,
            "order_item_id":      seen[oid],
            "product_id":         random.choice(product_ids),
            "seller_id":          random.choice(seller_ids),
            "shipping_limit_date": limit_dt,
            "price":              price,
            "freight_value":      freight,
        })
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=items[0].keys())
        writer.writeheader()
        writer.writerows(items)
    print(f"✔ order_items.csv      → {len(items):>6} records")


# ─────────────────────────────────────────
# 6. Payments (JSON)
# ─────────────────────────────────────────
def generate_payments(order_ids):
    path = RAW_PATH / "payments.json"
    payments = []
    for _ in range(NUM_PAYMENTS):
        oid  = random.choice(order_ids)
        ptype = random.choice(PAYMENT_TYPES)
        installments = random.randint(1, 12) if ptype == "credit_card" else 1
        payments.append({
            "order_id":              oid,
            "payment_sequential":    random.randint(1, 3),
            "payment_type":          ptype,
            "payment_installments":  installments,
            "payment_value":         round(random.uniform(10.0, 5000.0), 2),
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payments, f, indent=2, ensure_ascii=False)
    print(f"✔ payments.json        → {len(payments):>6} records")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "═" * 55)
    print("  DATA ENGINEERING POC – Sample Data Generator")
    print("═" * 55)
    print(f"  Output directory: {RAW_PATH.resolve()}\n")

    customer_ids = generate_customers()
    seller_ids   = generate_sellers()
    product_ids  = generate_products()
    order_ids    = generate_orders(customer_ids)
    generate_order_items(order_ids, product_ids, seller_ids)
    generate_payments(order_ids)

    print("\n" + "═" * 55)
    print("  ✔ All sample data generated successfully!")
    print("═" * 55 + "\n")
