from pyspark.sql import SparkSession
from pyspark.sql.functions import count_distinct

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


# Count distinct values in the 'language' column using distinct().count()
distinct_count = df.select("language").distinct().count()
print(f"Distinct count of 'language' column using distinct().count(): {distinct_count}")

# Count distinct values in the 'language' column using countDistinct()
distinct_count_func = df.select(count_distinct("language")).first()[0]
# Note: countDistinct returns a Row object, so we access the first element
# to get the actual count value.    
print(f"Distinct count of 'language' column using countDistinct(): {distinct_count_func}")  

# Stop SparkSession
spark.stop()