from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrameSparkApp") \
    .getOrCreate()

# First DataFrame
column_names = ["id", "language"]
data = [
    (1, "Python"),
    (2, "JavaScript"),
    (3, "C++"),
    (4, "Visual Basic"),
]
df_languages = spark.createDataFrame(data, column_names)
df_languages.show()

# Second DataFrame
column_names = ["framework_id", "framework", "language_id"]
data = [
    (1, "Spring", 5),
    (2, "FastAPI", 1),
    (3, "ReactJS", 2),
    (4, "Django", 1),
    (5, "Flask", 1),
    (6, "AngularJS", 2),
]
df_frameworks = spark.createDataFrame(data, column_names)
df_frameworks.show()

# Join DataFrames
print("Inner Join:")
df_joined = df_frameworks.join(df_languages, df_frameworks.language_id == df_languages.id, "inner")
df_joined.show()

print("Full Outer Join:")
df_joined_full = df_frameworks.join(df_languages, df_frameworks.language_id == df_languages.id, "outer")
df_joined_full.show()

print("Left Join:")
df_joined_left = df_frameworks.join(df_languages, df_frameworks.language_id == df_languages.id, "left")
df_joined_left.show()

print("Right Join:")
df_joined_right = df_frameworks.join(df_languages, df_frameworks.language_id == df_languages.id, "right")
df_joined_right.show()

# Stop SparkSession
spark.stop()