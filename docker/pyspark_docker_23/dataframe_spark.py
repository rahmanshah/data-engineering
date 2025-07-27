from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, min, max,mean, count

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame with sample data
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000),
    ("Python", "FastAPI", 13000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Perform aggregations by single column
df_grouped = df.groupBy("language").agg(
    avg("users").alias("avg_users"),
    sum("users").alias("total_users"),
    min("users").alias("min_users"),
    max("users").alias("max_users"),
    count("users").alias("count_users")
)
print("Aggregations by language:")
df_grouped.show()

# Perform aggregations by multiple columns
df_grouped_multi = df.groupBy("language", "framework").agg(
    avg("users").alias("avg_users"),
    sum("users").alias("total_users"),
    min("users").alias("min_users"),
    max("users").alias("max_users"),
    count("users").alias("count_users")
)
print("Aggregations by language and framework:")
df_grouped_multi.show()


# Stop SparkSession
spark.stop()