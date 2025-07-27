from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame with sample data
# This DataFrame contains information about programming languages, frameworks, and user counts
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000),
    ("Python", "FastAPI", 9000),
    ("Java", "Spring", 7000),
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Write PySpark DataFrame to CSV File using csv() method
df.write.option("header",True) \
    .option("delimiter",";") \
    .mode("overwrite") \
    .csv("data/frameworks.csv")

# Write PySpark DataFrame to CSV File using format("csv").save()
df.write.option("header",True) \
    .option("delimiter",";") \
    .format("csv") \
    .mode("overwrite") \
    .save("data/frameworks1.csv")



# Stop SparkSession
spark.stop()