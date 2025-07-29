from pyspark.sql import SparkSession
from pyspark.sql.functions import substring

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

# Extract a substring from the 'language' column
df_with_substring = df.withColumn("language_substring", substring(df.language, 1, 3))
print("DataFrame with Substring:")
df_with_substring.show()

# Stop SparkSession
spark.stop()