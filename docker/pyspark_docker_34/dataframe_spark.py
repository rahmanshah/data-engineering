from pyspark.sql import SparkSession

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

# Sample 50% of the rows with replacement
df_sampled = df.sample(withReplacement=True, fraction=0.5)
print("Sampled DataFrame:")
df_sampled.show()

# Sample 50% of the rows without replacement
df_sampled_no_replacement = df.sample(withReplacement=False, fraction=0.5)
print("Sampled DataFrame without replacement:")
df_sampled_no_replacement.show()

# Sample 50% of the rows with a seed for reproducibility
df_sampled_with_seed = df.sample(withReplacement=False, fraction=0.5, seed=42)
print("Sampled DataFrame with seed:")
df_sampled_with_seed.show()

# Stop SparkSession
spark.stop()