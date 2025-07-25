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

# Rename a single column
pyspark_df_renamed = pyspark_df.withColumnRenamed("Users", "User Count")
# Show the renamed DataFrame
print("DataFrame after renaming 'Users' to 'User Count':")
pyspark_df_renamed.show()

# Rename multiple columns
pyspark_df_renamed_multiple = pyspark_df.withColumnRenamed("Language", "Programming Language") \
    .withColumnRenamed("Framework", "Web Framework") \
    .withColumnRenamed("Users", "User Count")
# Show the DataFrame after multiple renames
print("DataFrame after renaming multiple columns:")
pyspark_df_renamed_multiple.show()

# Stop SparkSession
spark.stop()