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

# Sorting by a single column
df_sorted = df.orderBy("users", ascending=True)
print("Sorted by users (ascending):")
df_sorted.show()

df_sorted = df.orderBy("users", ascending=False)
print("Sorted by users (descending):")
df_sorted.show()

# Sorting by multiple columns
df_sorted_multi = df.orderBy(df.language, df.framework, ascending=[True, False])
print("Sorted by language (ascending) and framework (descending):")
df_sorted_multi.show()

# Stop SparkSession
spark.stop()