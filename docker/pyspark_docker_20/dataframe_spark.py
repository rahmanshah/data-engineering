from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame

column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000), 
    ("Python", "FastAPI", 9000), 
    ("Java", "Spring", 7000), 
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Add current date
df_with_date = df.withColumn("current_date", current_date())
print("DataFrame with Current Date:")
df_with_date.show()

# Add current timestamp
df_with_timestamp = df.withColumn("current_timestamp", current_timestamp())
print("DataFrame with Current Timestamp:")
df_with_timestamp.show()

# Stop SparkSession
spark.stop()