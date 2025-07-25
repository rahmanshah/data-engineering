from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Sample data
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("Sample DataFrame:")
df.show()

# Stop SparkSession
spark.stop()