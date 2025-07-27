from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame using a list of tuples
column_names = ["language", "frameworks"]
data = [
    ("Python", ["FastAPI", "Django", "Flask"]),
    ("JavaScript", ["ReactJS", "AngularJS"]),
    ("Java", None),
]
df = spark.createDataFrame(data, column_names)
df.show()

# Explode Arrays into Rows without Null Values
df_new = df.select(
    col("language"), 
    explode(col("frameworks")).alias("framework")
)

print("DataFrame after exploding frameworks:")
df_new.show()

# Explode Arrays into Rows with Null Values
df_new_outer = df.select(
    col("language"),
    explode_outer(col("frameworks")).alias("framework")
)

print("DataFrame after exploding frameworks (with nulls):")
df_new_outer.show()

# Stop SparkSession
spark.stop()