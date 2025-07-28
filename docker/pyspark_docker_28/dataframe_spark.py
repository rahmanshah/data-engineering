from pyspark.sql import SparkSession
from pyspark.sql.functions import col,pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create a DataFrame
column_names = ["language", "framework", "users"]
data = [
    ("Python", "Django", 20000), 
    ("Python", "FastAPI", 9000), 
    ("JavaScript", "ReactJS", 5000)
]
df = spark.createDataFrame(data, column_names)
df.show()

# convert the values of the PySpark DataFrame column "framework" to lowercase using pandas_udf() function
# Define Python function
def lower_case(column_values: pd.Series) -> pd.Series:
    return column_values.str.lower()

# Convert Python function into Pandas UDF
lower_case_udf = pandas_udf(
    lower_case,
    StringType()
)

# Apply Pandas UDF to DataFrame
new_df = df.withColumn(
    "framework_lower", 
    lower_case_udf(col("framework"))
)
print("DataFrame with Lowercase Framework:")
new_df.show()

# convert the values of the PySpark DataFrame column "framework" to uppercase using @pandas_udf decorator
# Define Pandas UDF
@pandas_udf(returnType=StringType())
def upper_case(column_values: pd.Series) -> pd.Series:
    return column_values.str.upper()

# Apply Pandas UDF to DataFrame
new_df = df.withColumn(
    "framework_upper",
    upper_case(col("framework"))
)
print("DataFrame with Uppercase Framework:")
new_df.show()



# Stop SparkSession
spark.stop()