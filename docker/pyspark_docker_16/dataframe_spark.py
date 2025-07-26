from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# First dataframe
column_names = ["language", "framework", "users"]
data = [
    ("Python", "FastAPI", 9000),
    ("JavaScript", "ReactJS", 7000),
]
df1 = spark.createDataFrame(data, column_names)
print("First DataFrame:")
df1.show()

# Second dataframe
column_names = ["language", "framework", "users"]
data = [
    ("Python", "FastAPI", 9000),
    ("Python", "Django", 20000),
    ("Java", "Spring", 12000),
]
df2 = spark.createDataFrame(data, column_names)
print("Second DataFrame:")
df2.show()

# Concatenate the two dataframes
df_concat = df1.union(df2)
print("Concatenated DataFrame:")
df_concat.show()

# Concatenate the two dataframes with distinct values
df_concat_distinct = df1.union(df2).distinct()
print("Concatenated DataFrame with Distinct Values:")
df_concat_distinct.show()

# Stop SparkSession
spark.stop()