from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Read a excel file into a DataFrame ussing the `spark-excel` library
df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Sheet1'!A1") \
    .load("data/sales.xlsx")

# Show the DataFrame content
print("DataFrame Content:")
df.show(5)

print("DataFrame Schema:")
df.printSchema()


# Stop SparkSession
spark.stop()