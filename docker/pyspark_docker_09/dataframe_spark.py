from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000), 
    ("Python", "FastAPI", 9000), 
    ("Java", "Spring", 7000), 
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Drop the 'users' column
df_dropped = df.drop("users")
print("DataFrame after dropping 'users' column:")
df_dropped.show()

# Drop multiple columns
df_dropped_multiple = df.drop("language", "framework")
print("DataFrame after dropping 'language' and 'framework' columns:")
df_dropped_multiple.show()

# Stop SparkSession
spark.stop()