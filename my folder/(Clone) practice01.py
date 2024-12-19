# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE employee (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     age INT,
# MAGIC     department STRING,
# MAGIC     salary FLOAT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a single record
# MAGIC INSERT INTO employee (id, name, age, department, salary)
# MAGIC VALUES (1, 'John Doe', 30, 'HR', 50000.0);
# MAGIC
# MAGIC -- Insert multiple records
# MAGIC INSERT INTO employee (id, name, age, department, salary)
# MAGIC VALUES 
# MAGIC     (2, 'Jane Smith', 28, 'IT', 75000.0),
# MAGIC     (3, 'Robert Brown', 45, 'Finance', 85000.0),
# MAGIC     (4, 'Emily Davis', 32, 'Marketing', 65000.0),
# MAGIC     (5, 'Michael Johnson', 40, 'Sales', 70000.0);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employee;

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("CreateTableExample").getOrCreate()

# Create a DataFrame
data = [(1, "John Doe", 28, "HR", 50000.0),
        (2, "Jane Smith", 35, "IT", 75000.0)]

columns = ["id", "name", "age", "department", "salary"]

df = spark.createDataFrame(data, columns)

# Save DataFrame as Delta Table
df.write.format("delta").saveAsTable("employee_delta")


# COMMAND ----------


