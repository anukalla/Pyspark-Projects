# Databricks notebook source
#dbfs:/FileStore/shared_uploads/anusha.kalla1996@gmail.com/employees___employees.csv

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
spark = SparkSession.builder.appName("Read").getOrCreate()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Define the schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

my_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("join_date", StringType(), True),
])
 
# Read the CSV file using the defined schema
df = spark.read.format("csv") \
    .schema(my_schema) \
    .option("header", True) \
    .load('/FileStore/shared_uploads/anusha.kalla1996@gmail.com/employees___employees.csv')
 
df.display()
df.printSchema()
 

# COMMAND ----------

# read the csv file

df = spark.read.format("csv").option("interschema",True).option("header",True).load('/FileStore/shared_uploads/anusha.kalla1996@gmail.com/employees___employees.csv')
df.display()

# COMMAND ----------

#1. Display records of employees aged above 30.
df_emp = df.filter(col("age")>30)
df_emp.display()

# COMMAND ----------

#2Find the average salary of employees in each department.
df.groupBy("department").agg(avg("salary")).display()

# COMMAND ----------

#3. Add a column experience indicating the number of years an employee has been working.
df_column = df.withColumn("experience",(months_between(current_date(), col("join_date")) / 12).cast("int"))
df_column.display()

# COMMAND ----------

#4. Find the top 3 highest-paid employees.

df.orderBy(col("salary").desc()).limit(3).display()
#ascending
df.orderBy(col("salary").asc()).limit(3).display()


# COMMAND ----------

#5. Identify the department with the highest total salary.

df_total = df.groupBy("department").agg(sum("salary").alias("total_salary"))
max_salary_department = df_total.orderBy(col("total_salary").desc()).limit(1)
max_salary_department.display()


# COMMAND ----------

#6. Create a new DataFrame with employees earning more than the average salary.

df_average = df.select(avg(col("salary")).alias("average_salary")).collect()[0]["average_salary"]
df_above_avg_salary = df.filter(col("salary") > df_average)
df_above_avg_salary.display()

# COMMAND ----------

#7. Rename the column name to employee_name.

df.withColumnRenamed("name","employee_name").display()

# COMMAND ----------

#8. Find the number of employees in each department.

df.groupBy("department").agg(count("name")).display()

# COMMAND ----------

#9. Select and display only the id and name columns.

df.select("id","name").display()

# COMMAND ----------

#10. Check for any null values in the dataset.

df.filter(col("id").isNull()|col("name").isNull()|col("age").isNull()|col("department").isNull()|col("salary").isNull()|col("join_date").isNull()).display()
