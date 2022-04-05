# Databricks notebook source
# MAGIC %md # Pandas Function APIs
# MAGIC **U**ser **D**efined **F**unctions are a convenient way to reuse logic that needs to be be executed on datasets.
# MAGIC UDFs are also used to wrap complex logic, such as ML (or even DL) models, and make it accessible for downstream consumers in SQL.
# MAGIC <p>
# MAGIC In this notebook, we'll only focus on **pandas_udf** (but there are other types of UDFs).
# MAGIC <p>pandas_udfs use [Arrow](https://arrow.apache.org/) to cut down on serialization between the JVM and python. Some operations can also benefit from Pandas' vectorized operations to gain an additional performance boost.
# MAGIC 
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" width="400" height="200" display="block" margin-left="auto" margin-right="auto">

# COMMAND ----------

# MAGIC %md ### Pandas Function APIs - [Grouped Map](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#grouped-map)
# MAGIC * Split the data into groups by using `DataFrame.groupBy`.
# MAGIC * Apply a function on each group. The input and output of the function are both `pandas.DataFrame`. The input data contains all the rows and columns for each group.
# MAGIC * Combine the results into a new `DataFrame`

# COMMAND ----------

# DBTITLE 1,Compute the difference between each individual salary and the group's mean salary 
# Read Delta Lake files (see https://delta.io/)
pysparkDF = spark.read.format("delta").load("/databricks-datasets/learning-spark-v2/people/people-10m.delta").select('id', 'gender', 'ssn', 'salary')

# Compute the mean salary of the current group and the difference between each person's salary and the group's mean salary
def calcSalaryDiffFromMean(pandasDF):
  salary = pandasDF.salary
  return pandasDF.assign(salaryMeanForGender = salary.mean(), salaryDiffFromMean = salary - salary.mean())

# Define the output schema
outputSchemaStr = "id integer, gender string, ssn string, salary integer, salaryMeanForGender double, salaryDiffFromMean double"

# Split the people DataFrame into gender groups and compute the difference between each person's salary and their group's mean salary
outputDF = pysparkDF.groupby('gender').applyInPandas(calcSalaryDiffFromMean, schema=outputSchemaStr)
outputDF.display()

# COMMAND ----------

# MAGIC %md ### Pandas Function APIs - [Map](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#map)
# MAGIC Map operations with Pandas instances are supported by `DataFrame.mapInPandas()` which maps an iterator of `pandas.DataFrame`s to another iterator of `pandas.DataFrame`s that represents the current PySpark `DataFrame` and returns the result as a PySpark `DataFrame`. 
# MAGIC 
# MAGIC The function takes and outputs an iterator of `pandas.DataFrame`. It can return the output of arbitrary length in contrast to some Pandas UDFs although internally it works similarly with Series to Series Pandas UDF.

# COMMAND ----------

# DBTITLE 1,Filter rows based on salary
# Read Delta Lake files (see https://delta.io/)
pysparkDF = spark.read.format("delta").load("/databricks-datasets/learning-spark-v2/people/people-10m.delta").select('id', 'gender', 'ssn', 'salary')

print('There are ', pysparkDF.count(), ' rows in the input PySpark DataFrame')

# Filter the rows on each pandas.DataFrame based on the salary
def filterBySalary(iterator):
    for df in iterator:
      yield df[df.salary > 100000]

# Define the output schema
outputSchemaStr = "id integer, gender string, ssn string, salary integer"

# Perform a map operation on the input PySpark DataFrame using Pandas
filteredDF = pysparkDF.mapInPandas(filterBySalary, schema=outputSchemaStr)
print('There are ', filteredDF.count(), ' rows in the output PySpark DataFrame')

# COMMAND ----------

# MAGIC %md ### Pandas Function APIs - [Cogrouped map](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#co-grouped-map)
# MAGIC Co-grouped map operations with Pandas instances are supported by `DataFrame.groupby().cogroup().applyInPandas()` which allows two PySpark `DataFrame`s to be cogrouped by a common key and then a Python function applied to each cogroup

# COMMAND ----------

# DBTITLE 1,Inner-join each cogroup 
# Read Delta Lake files (see https://delta.io/)
firstDF = spark.read.format("delta").load("/databricks-datasets/learning-spark-v2/people/people-10m.delta").select('id', 'lastName', 'ssn')
secondDF = spark.read.format("delta").load("/databricks-datasets/learning-spark-v2/people/people-10m.delta").select('id', 'lastName', 'birthDate')

# Inner-join two DataFrames
def joinDataFrames(leftDF, rightDF):
  return leftDF.merge(rightDF)

# Define the output schema
outputSchemaStr = "id integer, lastName string, ssn string, birthDate timestamp"

# Cogroup both DataFrames by last name, and then apply a function to each cogroup, which, in this case, simply joins them
outputDF = firstDF.groupby('lastName').cogroup(secondDF.groupby('lastName')).applyInPandas(joinDataFrames, schema=outputSchemaStr)
outputDF.display()

# COMMAND ----------

# MAGIC %md ### Read More
# MAGIC * [Databricks blog post](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html)
# MAGIC * [PySpark documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.pandas_udf.html)
# MAGIC * [Benchmark notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1281142885375883/2174302049319883/7729323681064935/latest.html)
# MAGIC * ["Old UDFs" vs pandas_udfs](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
