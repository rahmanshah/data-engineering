from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Read a CSV file into a DataFrame using csv()
df = spark.read.option("header",True) \
    .option("delimiter",";") \
    .option("inferSchema",True) \
    .csv("data/frameworks.csv")
print("DataFrame using csv():")
df.show()

# Read a CSV file into a DataFrame using format("csv").load()
df = spark.read.option("header",True) \
    .option("delimiter",";") \
    .option("inferSchema",True) \
    .format("csv") \
    .load("data/frameworks.csv")
print("DataFrame using format('csv').load():")
df.show()


# Stop SparkSession
spark.stop()