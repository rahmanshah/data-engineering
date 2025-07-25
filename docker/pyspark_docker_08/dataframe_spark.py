from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, sum, when
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000), 
    ("Python", "FastAPI", 9000), 
    ("Java", "Spring", 7000), 
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Add a new column with a constant value
df = df.withColumn(
    "country", 
    lit("Germany")
)
df.show()

# Add a column baseed on another column

df = df.withColumn(
    "users_percent",
    col("users")/sum('users').over(Window.partitionBy('language'))
)
df.show()

# Add a column based on a condition
df = df.withColumn(
    "type",
    when(df.language.isin(["Python", "Java"]), lit("Backend"))
        .when(df.language=="JavaScript", lit("Frontend"))
        .otherwise(lit(None))
)
df.show()

# Stop SparkSession
spark.stop()