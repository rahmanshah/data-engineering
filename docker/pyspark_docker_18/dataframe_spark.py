from pyspark.sql import SparkSession
from pyspark.sql.functions import col,udf
from pyspark.sql.types import StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000), 
    ("Python", "FastAPI", 9000), 
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# Create UDF with udf() function

# Define Python function
def lower_case(text: str) -> str:
    return text.lower()

# Convert Python function into UDF
lower_case_udf = udf(
    lambda x: lower_case(x), 
    StringType()
)

# Apply UDF to DataFrame
new_df = df.withColumn(
    "framework_lower", 
    lower_case_udf(col("framework"))
)
print("DataFrame after applying UDF:")
new_df.show()

# Create a udf with @udf decorator
# Define UDF
@udf(returnType=StringType())
def upper_case(text: str) -> str:
    return text.upper()

# Apply UDF to DataFrame
new_df = df.withColumn(
    "framework_upper",
    upper_case(col("framework"))
)
print("DataFrame after applying UDF with decorator:")
new_df.show()


# Stop SparkSession
spark.stop()