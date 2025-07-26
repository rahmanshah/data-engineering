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

# Filter rows with column condition
df_filtered = df.filter((df.language == "Python") & (df.users >= 10000))
print("Filtered DataFrame (column condition):")
df_filtered.show()

# Filter rows using SQL expression
df_filtered_sql = df.filter("language = 'Python' AND users >= 10000")
print("Filtered DataFrame (SQL expression):")
df_filtered_sql.show()

# Stop SparkSession
spark.stop()