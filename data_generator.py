from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, monotonically_increasing_id, when
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime, timedelta
# generate unique order_id
import random
# generate fake names
from faker import Faker

rows = 50

csv_path = "/user/mmmmmaxine/chewy_scraper_sample.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("DataGenerator").getOrCreate()

fake = Faker()
Faker.seed(12345)
random.seed(12345)

def generate_customer_id():
    return str(random.randint(100000, 999999))

def generate_random_date():
    start_date = datetime.now() - timedelta(days=30)
    return (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S")

def generate_random_name():
    return fake.name()

def generate_random_city():
    return fake.city()


payment_type = ["Card","Online Banking", "UPI","Wallet"]
def generate_payment_type():
    return random.choice(payment_type)

def generate_random_quanity():
    return random.randint(1,10)

transaction = ["Y","N"]
def generate_transaction_success():
    return random.choice(transaction)

country = ["US","Canada"]
def generate_random_country():
    return random.choice(country)

decline = ["Mismatched area code","Insufficient funds","Incorrect payment details","Transaction timeout"]
def generate_random_decline():
    return random.choice(decline)

def generate_random_city(country):
    if country == "US":
        fake = Faker("en_US")
    elif country == "Canada":
        fake = Faker("en_CA")
    return fake.city()

# Register the UDF
order_id_udf = udf(generate_customer_id, StringType())
order_date_udf = udf(generate_random_date, StringType())
customer_name_udf = udf(generate_random_name, StringType())
payment_type_udf = udf(generate_payment_type, StringType())
random_quanity_udf = udf(generate_random_quanity, IntegerType())
transaction_success_udf = udf(generate_transaction_success, StringType())
transaction_decline_udf = udf(generate_random_decline, StringType())
country_udf = udf(generate_random_country, StringType())
city_udf = udf(generate_random_city, StringType())

# Load CSV into a chewy item storage into python list
chewy_item = spark.read.csv(csv_path, header=True, inferSchema=True)
selected_columns = ["sku", "name", "price", "breadcrumb"]
chewy_item = chewy_item.select(selected_columns)
# chewy_item_list = chewy_item.collect()

# Generate order data
# order_id
# customer_id
# customer_name
# product_id
# product_name
# product_category
# payment_type
# qty
# price
# datetime
# country
# city
# ecommerce_website_name
# payment_txn_id
# payment_txn_success
# failure_reason

# order_data = [
#     (
#         row.sku,
#         row.name,
#         row.breadcrumb,
#         row.price
#     )
#     for row in random.choices(chewy_item_list, k=rows)
# ]

order_data = chewy_item.sample(False, rows / chewy_item.count()).collect()
columns = ["product_id", "product_name", "product_category", "price"]

order_df = spark.createDataFrame(order_data, columns)

order_df = order_df.withColumn("order_id", monotonically_increasing_id() + 1)\
    .withColumn("customer_id", order_id_udf())\
    .withColumn("customer_name", customer_name_udf())\
    .withColumn("payment_type", payment_type_udf())\
    .withColumn("qty", random_quanity_udf())\
    .withColumn("datetime", order_date_udf())\
    .withColumn("country", country_udf())\
    .withColumn("city", city_udf(col("country")))\
    .withColumn("ecommerce_website_name", lit("www.chewy.com"))\
    .withColumn("payment_txn_id", order_id_udf())\
    .withColumn("payment_txn_success", transaction_success_udf())\
    .withColumn("failure_reason", 
                when(col("payment_txn_success") == "N", transaction_decline_udf()).otherwise(lit("")))

print(f"Generated {rows} rows of order data")
order_df.coalesce(1).write.csv("/user/mmmmmaxine/order_data", header=True, mode="overwrite")
order_df.show(10,truncate=False)