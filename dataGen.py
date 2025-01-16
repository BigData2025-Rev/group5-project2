from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataGeneration") \
    .getOrCreate()

# Generate Customers Dataset
def generate_customers(num_customers, seed,cities_df):
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)
    customers = []

    # Flatten the cities DataFrame to get city names as a list of strings
    cities = cities_df.select("City").rdd.flatMap(lambda x: x).collect()

    for _ in range(num_customers):
        id = random.randint(100000, 999999)
        name = fake.name()
        country = "USA"
        city = random.choice(cities)

        customers.append((id, name, country, city))
    
    # Convert to Spark DataFrame
    return spark.createDataFrame(customers, schema=["Customer ID", "Customer Name", "Country", "City"])

# Generate Orders Dataset
def generate_orders(num_orders, customers_df, products_df, seed):
    random.seed(seed)
    orders = []
    payment_types = ["Card", "Banking", "UPI", "Crypto"]
    failure_reasons = ["Insufficient funds", "Card expired", "Connection Error"]

    # Collect customer and product data
    customers = customers_df.collect()
    products = products_df.collect()  

    if not customers:
        raise ValueError("No customers found. Ensure the customers dataset is populated.")
    if not products:
        raise ValueError("No products with valid prices found. Ensure the products dataset is valid.")

    for order_id in range(1, num_orders + 1):
        # Select random customer and product
        customer = random.choice(customers)
        product = random.choice(products)

        qty = random.randint(1, 10)
        price = product["Price"]
        total_price = price * qty if price is not None else 0.0

        payment_id = random.randint(100000, 999999)
        payment_success = random.choice(["Y", "N"])
        failure_reason = random.choice(failure_reasons) if payment_success == "N" else None
        payment_type = random.choice(payment_types)

        # Randomize purchase time
        order_datetime = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))

        orders.append((order_id, customer["Customer ID"], customer["Customer Name"], product["URL"], 
                       product["Name"], product["Category"], payment_type, qty, price, total_price, 
                       order_datetime.strftime("%Y-%m-%d %H:%M:%S"), customer["Country"], customer["City"], 
                       "https://www.chewy.com/", payment_id, payment_success, failure_reason))
    
    # Convert to Spark DataFrame
    return spark.createDataFrame(orders, schema=[
        "Order ID", "Customer ID", "Customer Name", "Product URL", "Product Name", 
        "Product Category", "Payment Type", "Qty", "Price", "Total Price", "Datetime", 
        "Country", "City", "Ecommerce Website Name", "Payment Txn ID", 
        "Payment Txn Success", "Failure Reason"
    ])

# Load Chewy Dataset
def load_products():
    products_df = spark.read.csv("file:///home/jarrod/data-gen/chewy_scraper_sample.csv", header=True, inferSchema=True)
    products_df = products_df.select(
        col("url").alias("URL"),
        col("sku").alias("SKU"),
        col("name").alias("Name"),
        col("breadcrumb").alias("Category"),
        col("Price")
    )
    return products_df




# Load City Dataset
def load_city():
    city_df = spark.read.csv("file:///home/jarrod/data-gen/uscities.csv", header=True, inferSchema=True)
    city_df = city_df.select(
        col("city").alias("City"),
    )
    return city_df


# Main Function
def main():
    seed = 7
    num_customers = 150

    
    # Load cities
    cities_df = load_city()

    # Load products
    products_df = load_products()

    # Generate customers
    customers_df = generate_customers(num_customers, seed,cities_df)

    # Generate orders
    num_orders = 500
    try:
        orders_df = generate_orders(num_orders, customers_df, products_df, seed)

        # Save to CSV
        customers_df.write.csv("customers_spark.csv", header=True, mode="overwrite")
        products_df.write.csv("products_spark.csv", header=True, mode="overwrite")
        orders_df.write.csv("orders_spark.csv", header=True, mode="overwrite")

        print("Datasets generated and saved")
    except ValueError as e:
        print(f"Error during order generation: {e}")


    orders_df.show(5)


# Run the script
if __name__ == "__main__":
    main()
