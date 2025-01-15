import os
import re
import random
import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("data_generator").getOrCreate()

def generate_products():
    products = spark.read.option("header", "true") \
                            .option("multiLine", "true") \
                            .csv("chewy_scraper_sample.csv")

    products = products.select("name", "price", "sku", "breadcrumb")
    products = products.withColumnRenamed("sku", "product_id").withColumnRenamed("name", "product_name").withColumnRenamed("breadcrumb", "product_category")
    
    category_mapping = {
        "feeders": ["feeder", "feeders"],
        "food": ["food", "feed"],
        "treats": ["treats"],
        "toys": ["toys", "games"],
        "health": ["health", "vitamins", "supplements","pharmacy", "pill", "dental", "healthcare", "pills", "flea"],
        "accessories": ["accessories", "lighting"],
        "beds": ["beds", "blankets"],
        "scratchers": ["scratchers"],
        "cages & enclosures": ["cages", "enclosures", "habitat", "crates"],
        "grooming": ["grooming"],
        "bowls & feeding": ["bowls", "feeding"],
        "heaters & thermometers": ["heaters", "thermometers"],
        "collars & harnesses": ["collars", "harnesses"],
        "gift": ["gift", "gifts"],
        "litter": ["litter", "potty"],
        "fish": ["fish"],
        "farm animal": ["farm animal"],
        "horse": ["horse"],
        "bird": ["bird"],
        "reptiles": ["reptile", "reptiles"],
    }

    # Define the category mapping function
    def map_category(breadcrumb):
        if not breadcrumb:
            return "other"
        
        breadcrumb = breadcrumb.lower()
        for category, keywords in category_mapping.items():
            if any(re.search(rf"\b{keyword}\b", breadcrumb) for keyword in keywords):
                return category
        return "other"
    
    # Register the UDF
    map_category_udf = fun.udf(map_category, StringType())
    
    # Apply the mapping
    products = products.withColumn(
        "product_category",
        map_category_udf(fun.col("product_category"))
    )
    
    return products

#Locations
def generate_locations():       
    # US Cities
    locations = spark.read.csv("uscities.csv", header=True, inferSchema=True)
    us_cities = locations.select(
        fun.col("city_ascii").alias("city"),
        fun.col("timezone"),
        fun.col("population").cast("integer").alias("population")
    ).orderBy("population", ascending=False).limit(1500)
    us_cities = us_cities.withColumn("country", fun.lit("USA"))
    
    # Cache US cities
    us_cities = us_cities.cache()
    us_cities.count()
    
    # Canada Cities with postal code filtering
    canada_cities = spark.read.csv("canadacities.csv", header=True, inferSchema=True)
    canada_cities = canada_cities.filter(
        (fun.substring("postal", 1, 1).isin(['L', 'M', 'K', 'N']))
    ).select(
        fun.col("city_ascii").alias("city"),
        fun.col("timezone"),
        fun.col("population").cast("integer").alias("population")
    ).orderBy("population", ascending=False).limit(999)
    canada_cities = canada_cities.withColumn("country", fun.lit("Canada"))
    
    # Cache Canada cities
    canada_cities = canada_cities.cache()
    canada_cities.count()
    
    # Repartition before union
    us_cities = us_cities.repartition(10)
    canada_cities = canada_cities.repartition(10)
    
    # Perform union and final operations
    combined = us_cities.unionByName(canada_cities)
    result = combined.orderBy("population", ascending=False)
    
    # Clean up cache
    us_cities.unpersist()
    canada_cities.unpersist()
    
    return result

#returns a dataframe with 5000 users
def names_df():
    users = spark.read.csv("names.csv", header=False, inferSchema=True)
    users = users.withColumnRenamed("_c0", "customer_name")
    return users

#Takes dataframe for names and locations. Generates a dataframe of random 3000 users with a random location
#additional data points: user_id(unique identifier)
def generate_users(random_seed, locations, names):
    random_seed += 5
    # First create base users with IDs
    sampled_names = names.sample(withReplacement=False, fraction=3000/names.count(), seed=random_seed)
    sampled_names = sampled_names.limit(3000)

    locations = locations.withColumn("location_index", fun.monotonically_increasing_id()).orderBy("city")

    possible_locations = locations.select("location_index", "city", "country")

    sampled_names = sampled_names.withColumn("location_index", fun.floor(fun.rand(random_seed) * possible_locations.count()))
    sampled_names = sampled_names.join(possible_locations, sampled_names.location_index == possible_locations.location_index, "left")
    sampled_names = sampled_names.select("customer_name", "city", "country")

    sampled_names = sampled_names.withColumn("customer_id", fun.monotonically_increasing_id())
    return sampled_names

#assigns the additional data points to the users_products dataframe
def generate_order(random_seed, users_products, date_start, date_end):
    random_seed += 3
    orders = users_products
    start = date_start.timestamp()
    end = date_end.timestamp()
    orders = orders.withColumn(
        "datetime",
        (fun.rand(random_seed) * (end - start) + start).cast("timestamp")
    )
    
    random_seed += 1
    orders = orders.withColumn("qty", fun.floor(fun.rand(random_seed) * 5) + 1)

    payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]
    orders = orders.withColumn(
        "payment_type",
        fun.element_at(
            fun.array(*[fun.lit(pt) for pt in payment_types]),
            (fun.rand(random_seed) * len(payment_types) + 1).cast("int")
        )
    )
    orders = orders.withColumn("ecommerce_website_name", fun.lit("www.chewy.com"))
    orders = orders.withColumn("payment_txn_id", fun.monotonically_increasing_id())
    orders = orders.withColumn("payment_txn_success", fun.lit("Y"))
    orders = orders.withColumn("failure_reason", fun.lit(""))
    
    return orders

#duplicates each row of users_products 2-5 times, and generates an order for each row. Each should be a couple weeks apart
#For a total of 2000-5000 orders
def generate_orders_reocurring(random_seed, users, products, date_start, date_end):
    random_seed += 2
    product_ids = [row.product_id for row in products.select("product_id").distinct().collect()]
    num_products = len(product_ids)

    users = users.withColumn(
        "product_id",
        fun.element_at(
            fun.array(*[fun.lit(pid) for pid in product_ids]),
            (fun.rand(random_seed) * num_products + 1).cast("int")
        )
    )
    
    users = users.join(
        products.select("product_id", "product_name", "product_category", "price"),
        "product_id",
        "left"
    )
        
    base_orders = generate_order(random_seed, users, date_start, date_end)
    final_orders = base_orders
    for i in range(4):
        additional_orders = base_orders.withColumn("datetime", fun.expr(f"datetime + INTERVAL {(i + 1) * 14} DAYS"))
        additional_orders = additional_orders.withColumn("payment_txn_id", fun.monotonically_increasing_id())
        final_orders = final_orders.union(additional_orders)
    
    final_orders = final_orders.orderBy("customer_id")
    return final_orders

#assigns each user a random product and generates an order for each user
#any time between date_start and date_end
#For a total of about 10000 orders
def generate_orders_one_time(random_seed, users, products, date_start, date_end):
    random_seed += 1
    users = users.sample(withReplacement=True, fraction=10000/users.count(), seed=random_seed)
    
    # Create an array of product IDs and get their count
    product_ids = [row.product_id for row in products.select("product_id").distinct().collect()]
    num_products = len(product_ids)
    
    # Use rand() to generate a random index and element_at to select the product_id
    users = users.withColumn(
        "product_id",
        fun.element_at(
            fun.array(*[fun.lit(pid) for pid in product_ids]),
            (fun.rand(random_seed) * num_products + 1).cast("int")
        )
    )

    users = users.join(
        products.select("product_id", "product_name", "product_category", "price"),
        "product_id",
        "left"
    )
    
    orders = generate_order(random_seed, users, date_start, date_end)
    return orders

#Takes dataframes for users, locations, products. Generates a dataframe of random orders
#additional data points: order_id (unique identifier), order_date (random date between date_start and date_end), qty (random integer between 1 and 5), 
#payment_type (randomly selected from "Card", "Internet Banking", "UPI", "Wallet"), website_name (default www.chewy.com), 
#payment_transaction_id (unique identifier), payment_success (Y/N default Y), failure_reason (default null)
#returns a dataframe of 10k-20k orders
def generate_final_data(random_seed, date_start : datetime.datetime, date_end : datetime.datetime):
    products = generate_products()
    print("Products generated")
    locations = generate_locations()
    print("Locations generated")
    names = names_df()
    print("Names generated")
    users = generate_users(random_seed, locations, names)
    print("Users generated")

    reocurring_users = users.sample(withReplacement=False, fraction=0.2, seed=random_seed)
    one_time_users = users.subtract(reocurring_users)

    reocurring_orders = generate_orders_reocurring(random_seed, reocurring_users, products, date_start, date_end)

    one_time_orders = generate_orders_one_time(random_seed, one_time_users, products, date_start, date_end)

    print("Orders generated")
    orders = reocurring_orders.union(one_time_orders)
    orders = orders.orderBy("datetime").withColumn("order_id", fun.monotonically_increasing_id())


    all_columns = ["order_id"] + [col for col in orders.columns if col != "order_id"]
    orders = orders.select(*all_columns)
    return orders
    
random_seed = 1
date_start = datetime.datetime(2022, 1, 1)
date_end = datetime.datetime(2024, 12, 31)
orders = generate_final_data(random_seed, date_start, date_end)
orders.show()

#output to csv in one file
orders.coalesce(1).write.csv("orders.csv", header=True, mode="overwrite")
print("Outputed to orders.csv")

