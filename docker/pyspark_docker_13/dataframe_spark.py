from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

column_names = ["language", "framework", "users"]
data = [
    ("Python", "FastAPI", None),
    ("JavaScript", None, 7000),
    ("Python", "Django", 20000),
    ("Java", None, None),
    (None, None, None),
]
df = spark.createDataFrame(data, column_names)
df.show()

# Removing Rows with Missing Values in any Column
df_cleaned = df.dropna(how="any")
print("DataFrame after removing rows with any missing values:")
df_cleaned.show()

# Removing Rows with Missing Values in All Columns
df_cleaned_all = df.dropna(how="all")
print("DataFrame after removing rows with all missing values:")
df_cleaned_all.show()

# Removing Rows with Missing Values in Specific Column
df_cleaned_specific = df.dropna(subset=["language"])
print("DataFrame after removing rows with missing values in 'language':")
df_cleaned_specific.show()

# Removing Rows with Missing Values in Specific Columns
df_cleaned_specific_cols = df.dropna(subset=["language", "framework"])
print("DataFrame after removing rows with missing values in 'language' and 'framework':")
df_cleaned_specific_cols.show()

# Stop SparkSession
spark.stop()