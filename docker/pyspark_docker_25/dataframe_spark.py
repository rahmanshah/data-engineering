from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a sample DataFrame
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Generate a unique ID for each row
df_with_id = df.withColumn("id", monotonically_increasing_id()) 

print("DataFrame with Unique ID:")
# Show the DataFrame with the unique ID
df_with_id.show()

# Show the schema of the DataFrame
print("Schema of the DataFrame:")
df_with_id.printSchema()


# Stop SparkSession
spark.stop()