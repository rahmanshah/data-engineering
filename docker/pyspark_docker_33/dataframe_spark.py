from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat,concat_ws

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

# Concatenate string columns
df_concat = df.withColumn("language_framework", concat(col("language"), col("framework")))
print("Concatenated DataFrame:")
df_concat.show()

# Concatenate string columns with a separator
df_concat_ws = df.withColumn("language_framework", concat_ws("-", col("language"), col("framework")))
print("Concatenated DataFrame with separator:")
df_concat_ws.show()


# Stop SparkSession
spark.stop()