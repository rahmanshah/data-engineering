from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Get statistical properties of the DataFrame
print("Statistical Properties of the DataFrame:")
df.describe().show()

print("Statistical Properties of the dataframe using summary:")
df.summary().show()

# Stop SparkSession
spark.stop()