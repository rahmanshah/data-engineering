from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("", "FastAPI", 9000),
    ("Java", "", 7000),
    ("", "", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Replace empty strings with null values
df = df.replace("", None)
print("DataFrame after replacing empty strings with null values:")
df.show()

# Stop SparkSession
spark.stop()