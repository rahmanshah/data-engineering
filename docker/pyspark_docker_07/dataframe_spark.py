import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Sample data
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000)
]

# Define schema
schema = StructType([
    StructField("Language", StringType(), True),
    StructField("Framework", StringType(), True),
    StructField("Users", IntegerType(), True)
])

# Create DataFrame
pyspark_df = spark.createDataFrame(data, schema)

# Show the DataFrame
print("Sample DataFrame with Schema:")
pyspark_df.show()

# Select specific columns -option 1
selected_df = pyspark_df.select("Language", "Users")

# Show the selected DataFrame
print("DataFrame after selecting specific columns:")
selected_df.show()

# Select specific columns -option 2
selected_df2 = pyspark_df.select(col("Language"), col("Users"))

# Show the selected DataFrame
print("DataFrame after selecting specific columns using col():")
selected_df2.show()

# Select specific columns -option 3
selected_df3 = pyspark_df.select(pyspark_df.Language, pyspark_df.Users)
# Show the selected DataFrame
print("DataFrame after selecting specific columns using DataFrame object:")
selected_df3.show()

# Select specific columns -option 4
selected_df4 = pyspark_df.select(pyspark_df["Language"], pyspark_df["Users"])
# Show the selected DataFrame
print("DataFrame after selecting specific columns using DataFrame object with []:")
selected_df4.show()

# Select specific columns -option 5
selected_df5 = pyspark_df.select(pyspark_df.columns[1:])
# Show the selected DataFrame
print("DataFrame after selecting specific columns using DataFrame object with [1:]:")
selected_df5.show()

# Stop SparkSession
spark.stop()