from pyspark.sql import SparkSession
from pyspark.sql.functions import col,pandas_udf,udf,lower
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

# Native Spark function to convert 'framework' column to lowercase
df = df.withColumn("framework", lower(col("framework")))
print("DataFrame with Lowercase Framework:")
df.show()

# Using udf to convert 'framework' column to lowercase
# Define UDF
@udf(StringType())
def to_lower_case_udf(s: str) -> str:
    return s.lower()

# Apply UDF to DataFrame column
df_udf = df.withColumn(
"framework", to_lower_case_udf(col("framework")))
print("DataFrame with UDF Lowercase Framework:")
df_udf.show()

# Using Pandas UDF to convert 'framework' column to lowercase
# Define Pandas UDF
@pandas_udf(StringType())
def to_lower_case_pandas(s: pd.Series) -> pd.Series:
    return s.str.lower()

# Apply Pandas UDF to DataFrame column
df_pandas_udf = df.withColumn("framework", to_lower_case_pandas(col("framework")))
print("DataFrame with Pandas UDF Lowercase Framework:")
df_pandas_udf.show()



# Stop SparkSession
spark.stop()