from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
spark = SparkSession.builder.appName("data_analysis").getOrCreate()


def load_data():
    orders = spark.read.option("header", "true") \
                            .option("multiLine", "true") \
                            .csv("project2dataset.csv")
    return orders

# sets values to empty strings if they are ROGUEROGUE, all x's (XXXXXX), or non-alphanumeric characters (ex: ---///---)
# columns to check inlude customer_name, product_category, failure_reason, country
def clean_data(data):
    data = data.withColumn("customer_name", fun.when(
        (fun.col("customer_name") == "ROGUEROGUE") | (fun.regexp_like(fun.col("customer_name"), fun.lit("^X{3,}"))) | (fun.regexp_like(fun.col("customer_name"), fun.lit("^[^a-zA-Z0-9]+$"))),
        fun.lit(None)
    )
    .otherwise(fun.col("customer_name")))

    data = data.withColumn("country", fun.when(
        (fun.col("country") == "ROGUEROGUE") | (fun.regexp_like(fun.col("country"), fun.lit("^X{3,}"))) | (fun.regexp_like(fun.col("country"), fun.lit("^[^a-zA-Z0-9]+$"))),
        fun.lit(None)
    )
    .otherwise(fun.col("country")))

    data = data.withColumn("product_category", fun.when(
        (fun.col("product_category") == "ROGUEROGUE") | (fun.regexp_like(fun.col("product_category"), fun.lit("^X{3,}"))) | (fun.regexp_like(fun.col("product_category"), fun.lit("^[^a-zA-Z0-9]+$"))),
        fun.lit(None)
    )
    .otherwise(fun.col("product_category")))

    data = data.withColumn("failure_reason", fun.when(
        (fun.col("failure_reason") == "ROGUEROGUE") | (fun.col("failure_reason") == "") | (fun.col("failure_reason").isNull()) | (fun.regexp_like(fun.col("failure_reason"), fun.lit("^X{3,}"))) | (fun.regexp_like(fun.col("failure_reason"), fun.lit("^[^a-zA-Z0-9]+$"))),
        fun.when(
            fun.col("payment_txn_success") == 1, fun.lit("payment successful")
        )
        .otherwise(fun.lit("unknown"))
    )
    .otherwise(fun.col("failure_reason")))

    #drop all rows with a null value in any column
    data = data.dropna()

    return data


data = load_data()
data = clean_data(data)

print(data.show())
print(data.count())
data.write.csv("cleaned_data.csv", header=True, mode="overwrite")