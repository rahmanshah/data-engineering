from pyspark.sql import SparkSession

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

# Count the number of rows in the DataFrame
row_count = df.count()
print(f"Number of rows in the DataFrame: {row_count}")

# Count the number of columns in the DataFrame
column_count = len(df.columns)
print(f"Number of columns in the DataFrame: {column_count}")

# Stop SparkSession
spark.stop()