from pyspark.sql import SparkSession
from pyspark.sql.functions import col,trim,ltrim,rtrim

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame from a list 
column_names = ["language", "framework", "users"]
data = [
    ("Python", "    Django    ", 20000),
    ("Python", "    FastAPI", 9000),
    ("JavaScript", "  AngularJS", 7000),
    ("JavaScript", "  ReactJS     ", 5000),
    ("Python", "  FastAPI      ", 13000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Remove leading and trailing Whitespaces
df_trimmed = df.withColumn("framework", trim(col("framework")))
print("DataFrame after trimming whitespaces:")
df_trimmed.show()

# Remove leading Whitespaces
df_ltrimmed = df.withColumn("framework", ltrim(col("framework")))
print("DataFrame after removing leading whitespaces:")
df_ltrimmed.show()

# Remove trailing Whitespaces
df_rtrimmed = df.withColumn("framework", rtrim(col("framework")))
print("DataFrame after removing trailing whitespaces:")
df_rtrimmed.show()


# Stop SparkSession
spark.stop()