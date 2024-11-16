from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from functools import reduce
from pyspark.sql.functions import col, regexp_replace, when, avg
from pyspark.sql.types import StringType,BooleanType,DateType,FloatType, IntegerType
import os

# Create SparkSession
spark = SparkSession.builder.appName("data_product").getOrCreate()

#1. Read datafrom local machine
appliances_df   = spark.read.csv(path=r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\meet46\data_original\All Appliances.csv", header=True, inferSchema=True)
vehicle_df      = spark.read.csv(path=r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\meet46\data_original\All Car and Motorbike Products.csv",header=True,inferSchema=True)
electronics_df  = spark.read.csv(path=r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\meet46\data_original\All Electronics.csv",header=True,inferSchema=True)
fitness_df      = spark.read.csv(path=r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\meet46\data_original\All Exercise and Fitness.csv",header=True,inferSchema=True)
groceries_df    = spark.read.csv(path=r"C:\Users\user\OneDrive\RFA _Personal Files\02. COURSE\Purwadhika_Data Engineering\Purwadhika_VS\meet46\data_original\All Grocery and Gourmet Foods.csv",header=True,inferSchema=True)

#2. Union data into one df
all_data_dfs_list = [appliances_df, vehicle_df, electronics_df, fitness_df, groceries_df]
all_data_dfs = reduce(DataFrame.unionAll, all_data_dfs_list)

#3. Backing up dataframe
df_product = all_data_dfs.alias('df_product')
df_product.show()

#4. handling discount_price and actual_price
df_product = df_product.withColumn("discount_price",regexp_replace(col("discount_price"), r'[^\d.]', "").cast("float"))
df_product = df_product.withColumn("actual_price",regexp_replace(col("actual_price"), r'[^\d.]', "").cast("float"))
df_product = df_product.withColumn(
    'no_of_ratings',
    when(col('no_of_ratings').rlike('^[0-9]+$'), col('no_of_ratings').cast('int')).otherwise(None))
df_product = df_product.withColumn("ratings", regexp_replace(col("ratings"), r"[^\d.]", "").cast("float"))

#5.Create new column (discrepancy between actual_price and discount_price)
df_product = df_product.withColumn("discount_amount", (col("actual_price")-col("discount_price")).cast("float"))

#6. drop duplicated data
df_product = df_product.drop_duplicates()

#7. sorting data
df_product = df_product.sort(df_product["no_of_ratings"].desc())

#8. filter ratings
df_product = df_product.filter(df_product["ratings"] < 5.0)

#9. Finding highest rated product based on main_category and sub_category
average_ratings = df_product.groupBy('main_category', 'sub_category') \
    .agg(avg('ratings').alias('average_ratings'))

#10. Find the category with the highest average ratings
highest_average = average_ratings.orderBy(col('average_ratings').desc())
highest_average.show()

#11. Finding product that has the highest discount_amount
highest_discount = df_product.orderBy(col('discount_amount').desc()).limit(5)
highest_discount.show()