import csv
import random
from datetime import datetime, timedelta

OUTPUT_FILE = "sales_big.csv"
NUM_ROWS = 1000000

start_date = datetime(2024,1,1)

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["sale_id","customer_id","product_id","sale_date","amount"])

    for i in range(1,NUM_ROWS+1):
        sale_id=i
        customer_id = random.randint(1,5000)
        product_id = random.randint(1,200)
        sale_date = start_date + timedelta(days=random.randint(0,30))
        amount = round(random.uniform(-10,500),2)

        writer.writerow([
            sale_id,
            customer_id,
            product_id,
            sale_date.strftime("%Y-%m-%d"),
            amount
        ])

print(f"Generated {NUM_ROWS} rows in {OUTPUT_FILE} ")