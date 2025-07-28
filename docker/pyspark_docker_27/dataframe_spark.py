from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, BooleanType, DateType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users", "backend", "date"]
data = [
    ("Python", "Django", "20000", "true", "2022-03-15"),
    ("Python", "FastAPI", "9000", "true", "2022-06-21"),
    ("Java", "Spring", "7000", "true", "2023-12-04"),
    ("JavaScript", "ReactJS", "5000", "false", "2023-01-11")
]
df = spark.createDataFrame(data, column_names)
df.show()

# Print the schema
df.printSchema()

# Change the data type of 'users' to IntegerType
df = df.withColumn("users", col("users").cast(IntegerType()))

# Print the schema after type change
print("Schema after changing 'users' to IntegerType:")
df.printSchema()

# Change Data Type of Multiple Columns
df = df.withColumn("backend", col("backend").cast(BooleanType())) \
       .withColumn("date", col("date").cast(DateType()))

# Print the schema after type change
print("Schema after changing 'backend' to BooleanType and 'date' to DateType:")
df.printSchema()

# Stop SparkSession
spark.stop()