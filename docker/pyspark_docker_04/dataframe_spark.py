from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# Create pandas dataframe
my_dict = {
    "Language": ["Python", "Python", "Java", "JavaScript"],
    "Framework": ["Django", "FastAPI", "Spring", "ReactJS"],
    "Users": [20000, 9000, 7000, 5000]
}
pandas_df = pd.DataFrame(my_dict)

# Define schema
schema = StructType([
    StructField("Language", StringType(), True),
    StructField("Framework", StringType(), True),
    StructField("Users", IntegerType(), True)
])

# Convert pandas DataFrame to Spark DataFrame
pyspark_df = spark.createDataFrame(pandas_df, schema)

# Show the DataFrame
print("Sample DataFrame with Schema:")
pyspark_df.show()

# Stop SparkSession
spark.stop()