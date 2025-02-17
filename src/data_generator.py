import os
import re
import random
import datetime
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("data_generator").getOrCreate()

# Takes the breadcrumb column and maps it to a category and generates the products dataframe
def generate_products():
    products = spark.read.option("header", "true") \
                            .option("multiLine", "true") \
                            .csv("chewy_scraper_sample.csv")

    products = products.select("name", "price", "sku", "breadcrumb")
    products = products.withColumnRenamed("sku", "product_id").withColumnRenamed("name", "product_name").withColumnRenamed("breadcrumb", "product_category")
    
    category_mapping = {
        "food": ["food", "feed"],
        "treats": ["treats"],
        "toys": ["toys", "games"],
        "health": ["health", "vitamins", "supplements","pharmacy", "pill", "dental", "healthcare", "pills", "flea"],
        "accessories": ["accessories", "lighting"],
        "beds": ["beds", "blankets"],
        "scratchers": ["scratchers"],
        "cages & enclosures": ["cages", "enclosures", "habitat", "crates"],
        "grooming": ["grooming"],
        "bowls & feeding": ["bowls", "feeding", "feeder", "feeders"],
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

    # Function to map category
    def map_category(breadcrumb):
        if not breadcrumb:
            return "other"
        
        breadcrumb = breadcrumb.lower()
        for category, keywords in category_mapping.items():
            if any(re.search(rf"\b{keyword}\b", breadcrumb) for keyword in keywords):
                return category
        return "other"
    
    map_category_udf = fun.udf(map_category, StringType())
    
    products = products.withColumn(
        "product_category",
        map_category_udf(fun.col("product_category"))
    )
    return products

#Locations, 1500 US cities, 100 Canadian cities from cities with postal codes L, M, K, N (Southern Ontario)
#Caches US cities, Canada cities to increase performance
def generate_locations():       
    # US Cities
    locations = spark.read.csv("uscities.csv", header=True, inferSchema=True)
    us_cities = locations.select(
        fun.col("city_ascii").alias("city"),
        fun.col("population").cast("integer").alias("population")
    ).orderBy("population", ascending=False).limit(1400)
    us_cities = us_cities.withColumn("country", fun.lit("USA"))
    
    us_cities = us_cities.cache()
    us_cities.count()
    
    # Canada Cities with postal code filtering
    canada_cities = spark.read.csv("canadacities.csv", header=True, inferSchema=True)
    canada_cities = canada_cities.filter(
        (fun.substring("postal", 1, 1).isin(['L', 'M', 'K', 'N']))
    ).select(
        fun.col("city_ascii").alias("city"),
        fun.col("population").cast("integer").alias("population")
    ).orderBy("population", ascending=False).limit(100)

    canada_cities = canada_cities.cache()
    canada_cities.count()

    #Other countries, only take cities with country as "australia", "united kingdom", and "mexico"
    other_countries = spark.read.csv("worldcities.csv", header=True, inferSchema=True)
    other_countries = other_countries.filter(
        (fun.col("country").isin(["Australia", "United Kingdom", "Mexico"]))
    ).select(
        fun.col("city_ascii").alias("city"),
        fun.col("population").cast("integer").alias("population"),
        fun.col("country")
    ).orderBy("population", ascending=False).limit(200)

    canada_cities = canada_cities.withColumn("country", fun.lit("Canada"))
    
    other_countries = other_countries.cache()
    other_countries.count()
    
    us_cities = us_cities.repartition(10)
    canada_cities = canada_cities.repartition(10)
    other_countries = other_countries.repartition(10)
    
    combined = us_cities.unionByName(canada_cities)
    combined = combined.unionByName(other_countries)
    result = combined.orderBy("population", ascending=False)
    us_cities.unpersist()
    canada_cities.unpersist()
    other_countries.unpersist()
    
    return result

#returns a dataframe with 5000 users
def names_df():
    users = spark.read.csv("names.csv", header=False, inferSchema=True)
    users = users.withColumnRenamed("_c0", "customer_name")
    return users

#Takes dataframe for names and locations. Generates a dataframe of random 4000 users with a random location
#additional data points: user_id(unique identifier)
def generate_users(random_seed, locations, names):
    random_seed += 5
    sampled_names = names.sample(withReplacement=False, fraction=4000/names.count(), seed=random_seed)
    sampled_names = sampled_names.limit(4000)

    locations = locations.withColumn("location_index", fun.monotonically_increasing_id()).orderBy("city")

    possible_locations = locations.select("location_index", "city", "country")

    sampled_names = sampled_names.withColumn("location_index", fun.floor(fun.rand(random_seed+1) * possible_locations.count()))
    sampled_names = sampled_names.join(possible_locations, sampled_names.location_index == possible_locations.location_index, "left")
    sampled_names = sampled_names.select("customer_name", "city", "country")

    sampled_names = sampled_names.withColumn("customer_id", fun.monotonically_increasing_id())
    return sampled_names

# Adjusts the time of a given timestamp to simulate sales patterns
@fun.udf(TimestampType())
def assign_time_with_trends(timestamp):
    """
    Adjusts the time of a given timestamp to simulate sales patterns:
    - Weekdays (Monday-Friday): Higher probability of sales during evening hours (6 PM - 10 PM)
    - Weekends (Saturday-Sunday): Sales evenly distributed throughout the day
    """
    import random
    day_of_week = timestamp.weekday()
    if day_of_week < 5:  # Weekdays (Monday-Friday)
        # Higher probability for evening hours (6 PM - 10 PM)
        hour = random.choices(
            population=[i for i in range(24)],
            weights=[1]*18 + [5]*4 + [1]*2,  # Increased weight for 6 PM to 10 PM
            k=1
        )[0]
    else:  # Weekends (Saturday-Sunday)
        # Uniform distribution throughout the day
        hour = random.choices(
            population=[i for i in range(24)],
            weights=[1]*24,
            k=1
        )[0]

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return timestamp.replace(hour=hour, minute=minute, second=second)

#assigns the order related data points to the users_products dataframe
def generate_order(random_seed, users_products, date_start, date_end):
    random_seed += 30
    orders = users_products
    start = date_start.timestamp()
    end = date_end.timestamp()
    orders = orders.withColumn(
        "datetime",
        (fun.rand(random_seed+1) * (end - start) + start).cast("timestamp")
    )
    
    orders = orders.withColumn("datetime", assign_time_with_trends(fun.col("datetime")))
    orders = orders.withColumn("qty", fun.floor(fun.rand(random_seed+2) * 5) + 1)

    payment_types = ["Card", "Internet Banking", "UPI", "Wallet"]
    
    # weighted the payment type
    weights = [0.4,0.1,0.4,0.1]

    @fun.udf(StringType())
    def weighted_payment():
        return random.choices(payment_types, weights)[0]

    orders = orders.withColumn("payment_type", weighted_payment())



    orders = orders.withColumn("ecommerce_website_name", fun.lit("www.chewy.com"))
    orders = orders.withColumn("payment_txn_id", fun.monotonically_increasing_id())

    failure_reasons = ["Insufficient Funds", "Invalid Payment Details","Transaction Timedout"]
    orders = orders.withColumn("payment_txn_success", fun.when(fun.rand(random_seed+4)>0.2, fun.lit("Y")).otherwise(fun.lit("N")))
    orders = orders.withColumn("failure_reason", fun.when(fun.col("payment_txn_success")=="N",
                                                          fun.element_at(fun.array(*[fun.lit(reason) for reason in failure_reasons]),
                                                                         (fun.rand(random_seed+5)*len(failure_reasons)+1).cast("int"))
                                                                         ).otherwise(fun.lit("")))
    
    return orders

def assign_product(random_seed, users, products):
    random_seed += 10
    product_ids = [row.product_id for row in products.select("product_id").distinct().collect()]
    num_products = len(product_ids)

    category_favorites = ["food", "treats", "health"]
    product_favorites = [row.product_id for row in products.filter(fun.col("product_category").isin(category_favorites)).select("product_id").distinct().collect()]
    num_product_favorites = len(product_favorites)

    canada_favorites = ["gift", "toys", "accessories"]
    canada_product_favorites = [row.product_id for row in products.filter(fun.col("product_category").isin(canada_favorites)).select("product_id").distinct().collect()]
    num_canada_product_favorites = len(canada_product_favorites)

    mexico_favorites = ["grooming", "fish", "treats"]
    mexico_product_favorites = [row.product_id for row in products.filter(fun.col("product_category").isin(mexico_favorites)).select("product_id").distinct().collect()]
    num_mexico_product_favorites = len(mexico_product_favorites)

    #if the user is in canada, there is a 60% chance of being a canada favorite
    #if the user is in mexico, there is a 60% chance of being a mexico favorite
    #otherwise, there is a 25% chance of being a product favorite
    users = users.withColumn(
         "product_id",
        fun.when(
            (fun.col("country") == "Canada") & (fun.rand(random_seed + 1) < 0.6),
            fun.element_at(
                fun.array(*[fun.lit(pid) for pid in canada_product_favorites]),
                (fun.rand(random_seed + 2) * num_canada_product_favorites + 1).cast("int"),
            ),
        )
        .when(
            (fun.col("country") == "Mexico") & (fun.rand(random_seed + 1) < 0.6),
            fun.element_at(
                fun.array(*[fun.lit(pid) for pid in mexico_product_favorites]),
                (fun.rand(random_seed + 2) * num_mexico_product_favorites + 1).cast("int"),
            ),
        )
        .when(
            fun.rand(random_seed + 1) < 0.25,
            fun.element_at(
                fun.array(*[fun.lit(pid) for pid in product_favorites]),
                (fun.rand(random_seed + 2) * num_product_favorites + 1).cast("int"),
            ),
        )
        .otherwise(
            fun.element_at(
                fun.array(*[fun.lit(pid) for pid in product_ids]),
                (fun.rand(random_seed + 3) * num_products + 1).cast("int"),
            ),
        )
    )

    users = users.join(
        products.select("product_id", "product_name", "product_category", "price"),
        "product_id",
        "left"
    )
    return users

#duplicates each row of users_products 4 times, and generates an order for each row. Each should be a couple weeks apart
#For a total of 2000-5000 orders
def generate_orders_reocurring(random_seed, users, products, date_start, date_end):
    random_seed += 20
    users = assign_product(random_seed, users, products)

    base_orders = generate_order(random_seed, users, date_start, date_end)
    final_orders = base_orders
    for i in range(4):
        additional_orders = base_orders.withColumn("datetime", fun.expr(f"datetime + INTERVAL {(i + 1) * 14} DAYS"))
        additional_orders = additional_orders.withColumn("payment_txn_id", fun.monotonically_increasing_id())
        final_orders = final_orders.union(additional_orders)
    
    final_orders = final_orders.orderBy("customer_id")
    final_orders = final_orders.filter(fun.col("datetime") <= date_end)
    return final_orders

#assigns each user a random product and generates an order for each user
#any time between date_start and date_end
#For a total of about 10000 orders
def generate_orders_one_time(random_seed, users, products, date_start, date_end):
    random_seed += 1
    users = users.sample(withReplacement=True, fraction=10000/users.count(), seed=random_seed+1)
    users = assign_product(random_seed, users, products)
    
    orders = generate_order(random_seed, users, date_start, date_end)
    return orders

#Takes dataframes for users, locations, products. Generates a dataframe of random orders
#additional data points: order_id (unique identifier), order_date (random date between date_start and date_end), qty (random integer between 1 and 5), 
#payment_type (randomly selected from "Card", "Internet Banking", "UPI", "Wallet"), website_name (default www.chewy.com), 
#payment_transaction_id (unique identifier), payment_success (Y/N default Y), failure_reason (default null)
#returns a dataframe of 10k-15k orders
def generate_final_data(random_seed, date_start : datetime.datetime, date_end : datetime.datetime):
    products = generate_products()
    print("Products generated")
    locations = generate_locations()
    print("Locations generated")
    names = names_df()
    print("Names generated")
    users = generate_users(random_seed, locations, names)
    print("Users generated")

    reocurring_users = users.sample(withReplacement=False, fraction=0.2, seed=random_seed+1)
    one_time_users = users

    reocurring_orders = generate_orders_reocurring(random_seed, reocurring_users, products, date_start, date_end)

    one_time_orders = generate_orders_one_time(random_seed, one_time_users, products, date_start, date_end)

    print("Orders generated")
    orders = reocurring_orders.union(one_time_orders)
    orders = orders.orderBy("datetime").withColumn("order_id", fun.monotonically_increasing_id())


    all_columns = ["order_id"] + [col for col in orders.columns if col != "order_id"]
    orders = orders.select(*all_columns)

    orders = orders.withColumn("price", fun.regexp_replace(orders["price"], r"[^0-9.]", ""))
    orders = orders.withColumn("price", orders["price"].cast(DoubleType()))
    return orders

#Selects up to 5% of the orders to convert to rogue data
#Either replaces the country with all lowercase letters, removes the category, removes payment_type, or replaces qty with a higher than normal number
def add_rogue_data(random_seed, orders):
    random_seed += 10
    rogue_orders = orders.sample(withReplacement=False, fraction=0.05, seed=random_seed)
    other_orders = orders.subtract(rogue_orders)

    rogue_orders = rogue_orders.withColumn("country", fun.when(fun.rand(random_seed+1) < 0.33, fun.lower(fun.col("country"))).otherwise(fun.col("country")))
    rogue_orders = rogue_orders.withColumn("qty", fun.when(fun.rand(random_seed+3) < 0.33, (fun.rand(random_seed+5) * 100).cast("int")).otherwise(fun.col("qty")))
    rogue_orders = rogue_orders.withColumn("product_category", fun.when(fun.rand(random_seed+6) < 0.33, fun.lit("")).otherwise(fun.col("product_category")))
    rogue_orders = rogue_orders.withColumn("payment_type", fun.when(fun.rand(random_seed+7) < 0.33, fun.lit("")).otherwise(fun.col("payment_type")))
    return rogue_orders.union(other_orders)



# Creates temporary month table to adjust the price for even months
def add_seasonal_trend(orders):
    
    orders = orders.withColumn("month", fun.month("datetime"))
    orders = orders.withColumn(
        "price",
        fun.when(fun.col("month").isin(10,11,12),fun.col("price") * 0.75)
        .otherwise(fun.col("price"))
    )

    orders = orders.drop("month")


    return orders


random_seed = 1
date_start = datetime.datetime(2022, 1, 1)
date_end = datetime.datetime(2024, 12, 31)
orders = generate_final_data(random_seed, date_start, date_end)
orders = add_rogue_data(random_seed, orders)
orders = orders.orderBy("order_id")
orders = add_seasonal_trend(orders)
orders.show()
print(orders.count())

#output to csv
orders.coalesce(1).write.csv("orders.csv", header=True, mode="overwrite")
print("Outputed to orders.csv")

