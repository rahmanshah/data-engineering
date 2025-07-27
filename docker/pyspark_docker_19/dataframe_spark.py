from pyspark.sql import SparkSession
from pyspark.sql.functions import col,regexp_extract, regexp_replace

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users"]
data = [
    ("Python", "FastAPI 0.92.0", 9000),
    ("JavaScript", "ReactJS 18.0", 7000),
    ("Python", "Django 4.1", 20000),
    ("Java", "Spring Boot 3.1", 12000),
]
df = spark.createDataFrame(data, column_names)
df.show()

# Filter Data

pattern = r"^Py"
df_new = df.filter(col("language").rlike(pattern))
print("Filtered DataFrame:")
df_new.show()

# Replace Data
pattern = r"\s*(\d+(\.\d+){0,2})" 
df_new = df.withColumn(
    "framework", 
    regexp_replace(col("framework"), pattern, "") # Matches version numbers and replaces them with an empty string
)
print("DataFrame after replacing version numbers:")
df_new.show()

# Extract Data
pattern = r"(\d+(\.\d+){0,2})"
df_new = df.withColumn(
    "version", 
    regexp_extract(col("framework"), pattern, 0) # Extracts version numbers and creates a new column "version"
)
print("DataFrame after extracting version numbers:")
df_new.show()


# Stop SparkSession
spark.stop()