from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Sample data
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000)
]

# Define schema
schema = StructType([
    StructField("Language", StringType(), True),
    StructField("Framework", StringType(), True),
    StructField("Users", IntegerType(), True)
])

# Create DataFrame
pyspark_df = spark.createDataFrame(data, schema)

# Show the DataFrame
print("Sample DataFrame with Schema:")
pyspark_df.show()

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = pyspark_df.toPandas()
print("\nConverted Pandas DataFrame:")
print(pandas_df)

# Stop SparkSession
spark.stop()