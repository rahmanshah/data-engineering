from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame with sample data
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 20000),
    ("JavaScript", "AngularJS", 5000),
    ("JavaScript", "ReactJS", 7000),
    ("Python", "Flask", 9000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Define a window specification to partition by language and order by users
window_spec = Window.partitionBy("language").orderBy(F.desc("users"))

# Applying Window Function row_number to add an ID column
# This will assign a unique row number to each row within the partition
# based on the order specified in the window specification
df_new = df.withColumn("row_number", F.row_number().over(window_spec))
# Show the new DataFrame with the row_number column
print("DataFrame with Row Number column:")
df_new.show()

# Applying Window Function rank() to add a Rank column
# This will assign a rank to each row within the partition
df_new = df.withColumn("rank", F.rank().over(window_spec))
# Show the new DataFrame with the rank column 
print("DataFrame with Rank column:")    
df_new.show()

# Applying Window Function dense_rank() to add a Dense Rank column
# This will assign a dense rank to each row within the partition
df_new = df.withColumn("dense_rank", F.dense_rank().over(window_spec))
# Show the new DataFrame with the dense rank column
print("DataFrame with Dense Rank column:")
df_new.show()


# Stop SparkSession
spark.stop()