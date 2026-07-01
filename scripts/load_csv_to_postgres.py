import argparse
import os
import psycopg2

ORDER = [
    "src_customer", "src_product", "src_order", "src_order_item", "src_payment", "exp_ground_truth_violation"
]

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--dsn", required=True)
    args = ap.parse_args()
    conn = psycopg2.connect(args.dsn)
    conn.autocommit = True
    cur = conn.cursor()
    for table in ORDER:
        path = os.path.join(args.input, f"{table}.csv")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            columns = f.readline().strip().split(",")
            f.seek(0)
            cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH CSV HEADER", f)
        print(f"Loaded {table} from {path}")
    cur.close()
    conn.close()
