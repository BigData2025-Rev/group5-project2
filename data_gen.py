import random
import csv
import datetime
from faker import Faker

fake = Faker()

# Constants for the generator
NUM_RECORDS = 15000  # Adjust to generate 10-15k records
ROGUE_PERCENTAGE = 0.05
CATEGORIES = ["Food", "Toys", "Accessories", "Health", "Training", "Clothing"]
PAYMENT_TYPES = ["Card", "Banking", "UPI", "Wallet"]
COUNTRIES = ["US", "Canada"]
CITIES_US = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
CITIES_CANADA = ["Toronto", "Ottawa", "Hamilton", "Mississauga", "London"]
E_COMMERCE_WEBSITE = "www.chewy.com"

# Function to generate rogue records
def introduce_rogue_data(record):
    if random.random() < 0.2:
        record["country"] = None  # Null country
    if random.random() < 0.2:
        record["qty"] = random.choice([-1, 1000])  # Outlier quantity
    if random.random() < 0.2:
        if record["payment_txn_success"] == "N":
            record["failure_reason"] = None  # Null failure reason when payment failed
    if random.random() < 0.2:
        record["country"] = record["country"].lower() if record["country"] else None  # Inconsistent capitalization
    return record

# Function to generate a single record
def generate_record():
    order_id = fake.unique.uuid4()
    customer_id = random.randint(100000, 999999)  # Adjusted to avoid uniqueness error
    customer_name = fake.first_name()
    country = random.choice(COUNTRIES)
    city = random.choice(CITIES_US if country == "US" else CITIES_CANADA)
    product_id = random.randint(100000, 999999)  # Adjusted to avoid uniqueness error
    product_name = fake.word().capitalize()
    product_category = random.choice(CATEGORIES)
    payment_type = random.choice(PAYMENT_TYPES)
    qty = random.randint(1, 5)
    price = round(random.uniform(5.0, 100.0), 2)
    datetime_order = fake.date_time_this_year()
    payment_txn_id = fake.unique.random_int(min=100000, max=999999)
    payment_txn_success = random.choice(["Y", "N"])
    failure_reason = None if payment_txn_success == "Y" else fake.sentence(nb_words=3)

    record = {
        "order_id": order_id,
        "customer_id": customer_id,
        "customer_name": customer_name,
        "product_id": product_id,
        "product_name": product_name,
        "product_category": product_category,
        "payment_type": payment_type,
        "qty": qty,
        "price": price,
        "datetime": datetime_order.strftime("%Y-%m-%d %H:%M:%S"),
        "country": country,
        "city": city,
        "ecommerce_website_name": E_COMMERCE_WEBSITE,
        "payment_txn_id": payment_txn_id,
        "payment_txn_success": payment_txn_success,
        "failure_reason": failure_reason,
    }

    return record

# Generate data
records = []
for _ in range(NUM_RECORDS):
    record = generate_record()
    if random.random() < ROGUE_PERCENTAGE:
        record = introduce_rogue_data(record)
    records.append(record)

# Write to CSV
with open("chewy_data.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)

print(f"Generated {NUM_RECORDS} records in 'chewy_data.csv'")
