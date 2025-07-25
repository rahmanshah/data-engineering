from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("SimplePySparkApp") \
    .getOrCreate()

print("Hello from PySpark!")

# Stop SparkSession
spark.stop()