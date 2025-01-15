from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, monotonically_increasing_id
from pyspark.sql.types import StringType
from datetime import datetime, timedelta

rows = 50

csv_path = "/user/maxine/chewy_scraper_sample.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("DataGenerator").getOrCreate()


# generate unique order_id
import random

def generate_customer_id():
    return str(random.randint(100000, 999999))

def generate_random_date():
    start_date = datetime.now() - timedelta(days=30)
    return (start_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S")

# Register the UDF
order_id_udf = udf(generate_customer_id, StringType())
order_date_udf = udf(generate_random_date, StringType())

# Load CSV into a chewy item storage into python list
chewy_item = spark.read.csv(csv_path, header=True, inferSchema=True)
selected_columns = ["sku", "name", "price", "breadcrumb"]
chewy_item = chewy_item.select(selected_columns)
chewy_item_list = chewy_item.collect()

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

order_data = [
    (
        row.sku,
        row.name,
        row.breadcrumb,
        row.price
    )
    for row in random.choices(chewy_item_list, k=rows)
]
columns = ["product_id", "product_name", "product_category", "price"]

order_df = spark.createDataFrame(order_data, columns)

order_df = order_df.withColumn("order_id", monotonically_increasing_id() + 1)\
    .withColumn("customer_id", order_id_udf())\
    .withColumn("customer_name", lit("John Doe"))\
    .withColumn("payment_type", lit("credit_card"))\
    .withColumn("qty", lit(1))\
    .withColumn("datetime", order_date_udf())\
    .withColumn("country", lit("USA"))\
    .withColumn("city", lit("Los Angeles"))\
    .withColumn("ecommerce_website_name", lit("Chewy"))\
    .withColumn("payment_txn_id", order_id_udf())\
    .withColumn("payment_txn_success", lit(True))\
    .withColumn("failure_reason", lit(None))

print(f"Generated {rows} rows of order data")
order_df.write.csv("/user/maxine/order_data.csv", header=True, mode="overwrite")