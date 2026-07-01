import argparse
import csv
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal

SCALES = {
    "small": {"customers": 1000, "products": 200, "orders": 5000},
    "1gb": {"customers": 10000, "products": 1000, "orders": 100000},
    "10gb": {"customers": 100000, "products": 5000, "orders": 1000000},
    "50gb": {"customers": 500000, "products": 10000, "orders": 5000000},
}
DOMAINS = ["viettel.com.vn", "gmail.com", "example.com", "corp.vn"]
PROVINCES = ["Ha Noi", "Da Nang", "Ho Chi Minh", "Can Tho", "Hai Phong", "Hue"]
CATEGORIES = ["mobile", "cloud", "iot", "software", "device", "service"]
STATUS = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]
CHANNELS = ["web", "mobile", "agent", "api"]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def dt(days_back=365):
    return datetime.utcnow() - timedelta(days=random.randint(0, days_back), seconds=random.randint(0, 86400))


def generate(scale, out, violation_rate):
    random.seed(42)
    cfg = SCALES[scale]
    ensure_dir(out)
    truth_rows = []

    customers = []
    for cid in range(1, cfg["customers"] + 1):
        email = f"user{cid}@{random.choice(DOMAINS)}"
        if random.random() < violation_rate:
            email = None if random.random() < 0.5 else f"invalid-email-{cid}"
            truth_rows.append(["src_customer", str(cid), "CUST_EMAIL_VALID", "critical", "quarantine"])
        created = dt(900)
        updated = created + timedelta(days=random.randint(0, 120))
        customers.append([
            cid, f"Customer {cid}", email, f"09{random.randint(10000000,99999999)}",
            f"No {cid} Street", random.choice(PROVINCES), created.isoformat(sep=" "), updated.isoformat(sep=" "), False
        ])
    write_csv(os.path.join(out, "src_customer.csv"),
              ["customer_id", "full_name", "email", "telephone", "address", "province", "created_at", "updated_at", "is_deleted"], customers)

    products = []
    for pid in range(1, cfg["products"] + 1):
        price = round(random.uniform(10000, 5000000), 2)
        products.append([pid, f"SKU-{pid:06d}", f"Product {pid}", random.choice(CATEGORIES), price, round(price * 0.7, 2), "ACTIVE", dt(100).isoformat(sep=" ")])
    write_csv(os.path.join(out, "src_product.csv"),
              ["product_id", "sku", "product_name", "category", "unit_price", "cost_price", "status", "updated_at"], products)

    orders, items, payments = [], [], []
    item_id = 1
    for oid in range(1, cfg["orders"] + 1):
        cid = random.randint(1, cfg["customers"])
        order_time = dt(365)
        n_items = random.randint(1, 5)
        total = Decimal("0.00")
        local_items = []
        for _ in range(n_items):
            pid = random.randint(1, cfg["products"])
            qty = random.randint(1, 5)
            if random.random() < violation_rate / 2:
                qty = -1
                truth_rows.append(["src_order_item", str(item_id), "ITEM_QTY_POSITIVE", "critical", "quarantine"])
            price = Decimal(str(round(random.uniform(10000, 2000000), 2)))
            discount = Decimal(str(round(random.uniform(0, 10000), 2)))
            amount = price * Decimal(max(qty, 0)) - discount
            total += amount
            local_items.append([item_id, oid, pid, qty, float(price), float(discount), float(amount), order_time.isoformat(sep=" ")])
            item_id += 1
        if random.random() < violation_rate:
            total = Decimal("-1")
            truth_rows.append(["src_order", str(oid), "ORDER_AMOUNT_POSITIVE", "critical", "quarantine"])
        orders.append([oid, cid, order_time.isoformat(sep=" "), random.choice(STATUS), random.choice(CHANNELS), float(total), order_time.isoformat(sep=" ")])
        items.extend(local_items)
        card = "4" + "".join(str(random.randint(0, 9)) for _ in range(15))
        payments.append([oid, oid, "CARD", card, float(max(total, 0)), order_time.isoformat(sep=" "), order_time.isoformat(sep=" ")])

    write_csv(os.path.join(out, "src_order.csv"),
              ["order_id", "customer_id", "order_date", "order_status", "channel", "total_amount", "updated_at"], orders)
    write_csv(os.path.join(out, "src_order_item.csv"),
              ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount", "line_amount", "updated_at"], items)
    write_csv(os.path.join(out, "src_payment.csv"),
              ["payment_id", "order_id", "payment_method", "card_number", "amount", "paid_at", "updated_at"], payments)
    write_csv(os.path.join(out, "exp_ground_truth_violation.csv"),
              ["source_table", "source_pk", "rule_id", "severity", "expected_action"], truth_rows)
    print(f"Generated {scale} dataset under {out}. Ground-truth violations: {len(truth_rows)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=SCALES.keys(), default="small")
    ap.add_argument("--out", required=True)
    ap.add_argument("--violation-rate", type=float, default=0.01)
    args = ap.parse_args()
    generate(args.scale, args.out, args.violation_rate)
