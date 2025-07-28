from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame from a list 
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("JavaScript", "AngularJS", 7000),
    ("JavaScript", "ReactJS", 5000),
    ("Python", "FastAPI", 13000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Group and Concatenate Strings
grouped_df = df.groupby('language') \
    .agg(F.concat_ws(', ', F.collect_list(df.framework)) \
    .alias('frameworks'))

print("Grouped DataFrame with Concatenated Frameworks:")
grouped_df.show()

# Group and Concatenate Strings without Duplicates
grouped_df = df.groupby('language') \
    .agg(F.concat_ws(', ', F.array_distinct(F.collect_list(df.framework))).alias('frameworks')) # Using array_distinct to remove duplicates along with collect_list and concat_ws

print("Grouped DataFrame with Unique Frameworks:")
grouped_df.show()



# Stop SparkSession
spark.stop()