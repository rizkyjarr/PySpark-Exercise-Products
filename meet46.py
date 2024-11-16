from pyspark.sql.functions import col, min, max
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

# Sample DataFrame
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Calculate Min and Max for Age
age_min = df.select(min(col("Age"))).collect()[0][0]
age_max = df.select(max(col("Age"))).collect()[0][0]

# Perform Normalization
df_normalized = df.withColumn("Age_Normalized", (col("Age") - age_min) / (age_max - age_min))

df_normalized.show()