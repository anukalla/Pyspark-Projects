# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Session").getOrCreate()

# COMMAND ----------

project_employee_data = [
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 1),
    (2, 4)]

schema1 = "project_id int, employee_id  string"
employee_data = [
    (1, "Khaled", 3),
    (2, "Ali", 2),
    (3, "John", 1),
    (4, "Doe", 2)]
schema2 = "employee_id  int, name string, experience_years int"


# COMMAND ----------

df_data1 = spark.createDataFrame(project_employee_data, schema1)
df_data2 = spark.createDataFrame(employee_data, schema2)

# COMMAND ----------

df_data1.display()
df_data2.display()

# COMMAND ----------

df_join = df_data1.join(df_data2, df_data1.employee_id==df_data2.employee_id, "inner").select(df_data1.employee_id,df_data1.project_id, df_data2.name,df_data2.experience_years)
df_join.display()

# COMMAND ----------

avg_exp = df_join.groupBy("project_id").agg(round(avg("experience_years"), 2).alias("average_experience_years"))
avg_exp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.
# MAGIC
# MAGIC Return the result table in any order.
# MAGIC
# MAGIC The result format is in the following example.
# MAGIC
# MAGIC  
# MAGIC
# MAGIC Example 1:
# MAGIC
# MAGIC Input: 
# MAGIC Employee table:
# MAGIC +-------+--------+------------+--------+
# MAGIC | empId | name   | supervisor | salary |
# MAGIC +-------+--------+------------+--------+
# MAGIC | 3     | Brad   | null       | 4000   |
# MAGIC | 1     | John   | 3          | 1000   |
# MAGIC | 2     | Dan    | 3          | 2000   |
# MAGIC | 4     | Thomas | 3          | 4000   |
# MAGIC +-------+--------+------------+--------+
# MAGIC Bonus table:
# MAGIC +-------+-------+
# MAGIC | empId | bonus |
# MAGIC +-------+-------+
# MAGIC | 2     | 500   |
# MAGIC | 4     | 2000  |
# MAGIC +-------+-------+

# COMMAND ----------

employee_table = [
    (3, 'Brad', 'null', 4000),
    (1, 'John', '3', 1000),
    (2, 'Dan', '3', 2000),
    (4, 'Thomas', '3', 4000)]

schema1 = "empId int, name string, supervisor string, salary int"
bonus_data = [
    (2, 500),
    (4, 2000)]
schema2 = "empId int, bonus int"


# COMMAND ----------

df1 = spark.createDataFrame(employee_table, schema1)
df2 = spark.createDataFrame(bonus_data, schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

df_join_table = df1.join(df2, df1.empId==df2.empId, "left_outer").select(df1.empId,df1.name, df1.supervisor,df1.salary,df2.bonus)
df_join_table.display()

# COMMAND ----------

from pyspark.sql.functions import col

df_result = df_join_table.filter((col("bonus")<1000)|(col("bonus").isNull()))\
    .select(col("name"), col("bonus"))
df_result.display()

# COMMAND ----------

df_result.display()
