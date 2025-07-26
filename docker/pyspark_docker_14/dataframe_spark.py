from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "FastAPI", None),
    (None, None, 7000),
    ("Python", "Django", 20000),
    ("Java", None, None),
]
df = spark.createDataFrame(data, column_names)
df.show()

# Replace Missing Values with Constant Values
df_cleaned = df.fillna(value="unknown", subset=["language", "framework"])
df_cleaned = df_cleaned.fillna(value=0, subset=["users"])
print("DataFrame after replacing NULL values:")
df_cleaned.show()

# Replace Missing Values with Aggregated Values
mean_value = df.select(mean(df['users'])).collect()[0][0]
df_cleaned = df.fillna(mean_value, subset=['users'])
print("DataFrame after replacing NULL values with mean:")
df_cleaned.show()


# Stop SparkSession
spark.stop()