# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, FloatType

# Initialize Spark Session
spark = SparkSession.builder.appName("UDFExample").getOrCreate()

# Sample Data
data = [(1, "John", 30), (2, "Jane", 35), (3, "Robert", 40)]
columns = ["id", "name", "age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

df.show()


# COMMAND ----------

# Define a Python function
def age_category(age):
    if age < 35:
        return "Young"
    else:
        return "Adult"

# Convert it into a UDF
age_category_udf = udf(age_category, StringType())


# Apply UDF to DataFrame
df_with_category = df.withColumn("age_category", age_category_udf(df["age"]))

df_with_category.show()



# COMMAND ----------

# Define a UDF that combines name and age
def name_with_age(name, age):
    return f"{name} is {age} years old."

# Register the UDF
name_with_age_udf = udf(name_with_age, StringType())

# Use the UDF
df_combined = df.withColumn("name_with_age", name_with_age_udf(df["name"], df["age"]))

df_combined.show()

