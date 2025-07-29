from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language_framework", "users"]
data = [
    ("Python - Django", 20000),
    ("Python - FastAPI", 9000),
    ("Java - Spring", 7000),
    ("JavaScript - ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Split the 'language_framework' column into two separate columns
df_split = df.withColumn("language", split(col("language_framework"), " - ").getItem(0)) \
             .withColumn("framework", split(col("language_framework"), " - ").getItem(1)) \
             .drop("language_framework")

print("Split DataFrame column language_framework into language and framework columns:")
df_split.show()

# Stop SparkSession
spark.stop()