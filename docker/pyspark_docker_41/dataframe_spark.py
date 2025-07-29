from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["id", "information"]
data = [
    (0, '{"language":"Python","framework":"Django","users":20000}'),
    (1, '{"language":"Python","framework":"FastAPI","users":9000}'),
    (2, '{"language":"Java","framework":"Spring","users":7000}'),
    (3, '{"language":"JavaScript","framework":"ReactJS","users":5000}')
]
df = spark.createDataFrame(data, column_names)
df.show()

# Define the schema for the JSON data
schema = StructType([
    StructField("language", StringType(), True),
    StructField("framework", StringType(), True),
    StructField("users", IntegerType(), True)
])

# Parse the JSON string in the 'information' column
df_parsed = df.withColumn("information", from_json(col("information"), schema))
print("Parsed DataFrame:")
df_parsed.show()

# Extract fields from the parsed JSON
df_extracted = df_parsed.select(
    col("id"),
    col("information.language").alias("language"),
    col("information.framework").alias("framework"),
    col("information.users").alias("users")
)
print("Extracted DataFrame:")
df_extracted.show()

# Stop SparkSession
spark.stop()