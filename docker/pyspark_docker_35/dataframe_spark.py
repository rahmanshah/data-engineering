from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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


# Creating a broadcast variable
# Access the SparkContext via the SparkSession
sc = spark.sparkContext

# Create a list
languages = ["Python", "Java"]

# Broadcast the list
broadcasted_languages = sc.broadcast(languages) # creates a broadcast variable based on the list, which can be used across all nodes

# Use broadcasted variable
# Filter DataFrame using the broadcasted variable
filtered_df = df.filter(col("language").isin(broadcasted_languages.value))

# Show the filtered DataFrame
filtered_df.show()

# Stop SparkSession
spark.stop()