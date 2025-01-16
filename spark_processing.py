from pyspark.sql import SparkSession

import shutil
shutil.rmtree("output/chewy_data_cleaned", ignore_errors=True)


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Chewy Data Processing") \
    .getOrCreate()


# Read the generated CSV file into a Spark DataFrame
df = spark.read.csv("chewy_data.csv", header=True, inferSchema=True)

# Example: Show the schema of the data
df.printSchema()

# Example: Count records by country
country_counts = df.groupBy("country").count()
country_counts.show()

# Example: Filter out rogue records
cleaned_df = df.filter((df["country"].isNotNull()) & (df["qty"] > 0))

# Save the cleaned data back to a new CSV
cleaned_df.write.mode("overwrite").csv("output\\chewy_data_cleaned", header=True)

print("Data cleaning complete and saved as 'chewy_data_cleaned.csv'")
