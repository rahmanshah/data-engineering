from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users", "date"]
data = [
    ("Python", "Django", 20000, "2025/01/01"),
    ("Python", "FastAPI", 9000, "2023/02/04"),
    ("Java", "Spring", 7000, "2024/03/26"),
    ("JavaScript", "ReactJS", 5000, "2025/04/01")
]
df = spark.createDataFrame(data, column_names)
df.show()

print("Original DataFrame schema:")
df.printSchema()

# convert 'date' column from string to date format
df = df.withColumn("date", to_date(col("date"), "yyyy/MM/dd"))
df.show()
print("Updated DataFrame schema:")
df.printSchema()

# Stop SparkSession
spark.stop()