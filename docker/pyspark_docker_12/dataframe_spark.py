from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "FastAPI", 9000), 
    ("Python", "FastAPI", 9000), 
    ("Java", "Spring", 7000), 
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Remove duplicates from the DataFrame
df_cleaned = df.dropDuplicates()
print("DataFrame after removing duplicates:")
df_cleaned.show()

# Remove duplicates based on a specific column
df_cleaned_specific = df.dropDuplicates(["language"])
print("DataFrame after removing duplicates based on 'language':")
df_cleaned_specific.show()

# Stop SparkSession
spark.stop()