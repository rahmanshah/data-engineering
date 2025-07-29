from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users", "timestamp"]
data = [
    ("Python", "Django", 20000, "2025/01/01 10:30:00"),
    ("Python", "FastAPI", 9000, "2023/02/04 06:00:50"),
    ("Java", "Spring", 7000, "2024/03/26 09:45:00"),
    ("JavaScript", "ReactJS", 5000, "2025/04/01 11:10:12")
]
df = spark.createDataFrame(data, column_names)
df.show()

print("Original DataFrame schema:")
df.printSchema()

# convert 'timestamp' column from string to timestamp format
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy/MM/dd HH:mm:ss"))
df.show()
print("Updated DataFrame schema:")
df.printSchema()

# Stop SparkSession
spark.stop()