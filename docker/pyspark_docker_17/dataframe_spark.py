from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000), 
    ("Python", "FastAPI", 9000), 
    (None, "Spring", 9000),
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# First and Last value of column 'framework'
print("First and Last value of column 'framework':")

first_value = df.select(first("framework")).collect()[0][0]
last_value = df.select(last("framework")).collect()[0][0]

print(f"First value: {first_value}")
print(f"Last value: {last_value}")

# Count and Count Distinct values of column 'language'
print("\nCount and Count Distinct values of column 'language':")
count_language = df.select(count("language")).collect()[0][0]
count_distinct_language = df.select(countDistinct("language")).collect()[0][0]
print(f"Count of 'language': {count_language}")
print(f"Count Distinct of 'language': {count_distinct_language}")

# Maximum and Minimum values of column 'users'
print("\nMaximum and Minimum values of column 'users':")
max_users = df.select(max("users")).collect()[0][0]
min_users = df.select(min("users")).collect()[0][0]
print(f"Maximum 'users': {max_users}")
print(f"Minimum 'users': {min_users}")

# Average, Mean and Standard Deviation of column 'users'
print("\nAverage, Mean and Standard Deviation of column 'users':")
avg_users = df.select(avg("users")).collect()[0][0]
mean_users = df.select(mean("users")).collect()[0][0]
stddev_users = df.select(stddev("users")).collect()[0][0]
print(f"Average 'users': {avg_users}")
print(f"Mean 'users': {mean_users}")
print(f"Standard Deviation 'users': {stddev_users}")

# Sum and Sum Distinct values of column 'users'
print("\nSum and Sum Distinct values of column 'users':")
sum_users = df.select(sum("users")).collect()[0][0]
sum_distinct_users = df.select(sum_distinct("users")).collect()[0][0]
print(f"Sum of 'users': {sum_users}")
print(f"Sum Distinct of 'users': {sum_distinct_users}")

# Stop SparkSession
spark.stop()